using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace Relaybox.RabbitMq;

/// <summary>
/// Publishes with publisher confirms: <see cref="PublishAsync"/> completes only when the broker has
/// accepted the message and throws if it nacks or returns it. Channels are pooled, one per concurrent
/// publication, and discarded after any failure.
/// </summary>
internal sealed class RabbitMqMessagePublisher(RabbitMqConnection connection, IOptions<RabbitMqOptions> options)
    : IMessagePublisher, IAsyncDisposable
{
    private readonly ConcurrentBag<IChannel> _channels = [];
    private volatile bool _exchangeDeclared;

    public async Task PublishAsync(OutboxMessage message, CancellationToken ct)
    {
        var settings = options.Value;
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(settings.PublishTimeout);
        ct = timeout.Token;

        var channel = await RentAsync(settings, ct);
        var reusable = false;

        try
        {
            var properties = new BasicProperties
            {
                MessageId = message.Id.ToString(),
                Type = message.Type,
                ContentType = "application/json",
                DeliveryMode = DeliveryModes.Persistent,
                Timestamp = new AmqpTimestamp(new DateTimeOffset(message.OccurredAt, TimeSpan.Zero).ToUnixTimeSeconds()),
                Headers = new Dictionary<string, object?> { [RelayboxHeaders.TenantId] = message.TenantId },
            };

            if ((Activity.Current?.Id ?? message.TraceParent) is { } traceParent)
            {
                properties.Headers[RelayboxHeaders.TraceParent] = traceParent;
            }

            await channel.BasicPublishAsync(
                settings.Exchange,
                settings.RoutingKey(message),
                settings.Mandatory,
                properties,
                Encoding.UTF8.GetBytes(message.Payload),
                ct);

            reusable = true;
        }
        finally
        {
            if (reusable && channel.IsOpen)
            {
                _channels.Add(channel);
            }
            else
            {
                await channel.DisposeAsync();
            }
        }
    }

    private async Task<IChannel> RentAsync(RabbitMqOptions settings, CancellationToken ct)
    {
        while (_channels.TryTake(out var pooled))
        {
            if (pooled.IsOpen)
            {
                return pooled;
            }

            await pooled.DisposeAsync();
        }

        var conn = await connection.GetAsync(ct);
        var channel = await conn.CreateChannelAsync(
            new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true),
            ct);

        if (settings.DeclareExchange && !_exchangeDeclared)
        {
            await channel.ExchangeDeclareAsync(settings.Exchange, settings.ExchangeType, durable: true, cancellationToken: ct);
            _exchangeDeclared = true;
        }

        return channel;
    }

    public async ValueTask DisposeAsync()
    {
        while (_channels.TryTake(out var channel))
        {
            await channel.DisposeAsync();
        }
    }
}
