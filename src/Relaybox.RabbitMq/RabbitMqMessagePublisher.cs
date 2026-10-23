using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace Relaybox.RabbitMq;

/// <summary>
/// Publishes with publisher confirms: <see cref="PublishAsync"/> completes only when the broker has
/// accepted the message. Refusals of the message itself (nack, return, missing exchange, too large) surface
/// as <see cref="MessageRejectedException"/>; connection problems as they are. Channels are pooled, one per concurrent
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
            var headers = new Dictionary<string, object?>();
            foreach (var (name, value) in message.Headers ?? new Dictionary<string, string>())
            {
                headers[name] = value;
            }

            headers[RelayboxHeaders.TenantId] = message.TenantId;
            if ((Activity.Current?.Id ?? message.TraceParent) is { } traceParent)
            {
                headers[RelayboxHeaders.TraceParent] = traceParent;
            }

            var properties = new BasicProperties
            {
                MessageId = message.Id.ToString(),
                Type = message.Type,
                ContentType = "application/json",
                DeliveryMode = DeliveryModes.Persistent,
                Timestamp = new AmqpTimestamp(new DateTimeOffset(message.OccurredAt, TimeSpan.Zero).ToUnixTimeSeconds()),
                Headers = headers,
            };

            await channel.BasicPublishAsync(
                message.Destination ?? settings.Exchange,
                message.RoutingKey ?? settings.RoutingKey(message),
                settings.Mandatory,
                properties,
                Encoding.UTF8.GetBytes(message.Payload),
                ct);

            reusable = true;
        }
        catch (PublishException ex)
        {
            throw new MessageRejectedException(
                ex.IsReturn ? "The broker returned the message: no queue is bound for its routing key." : "The broker nacked the message.",
                isPermanent: false,
                ex);
        }
        catch (OperationInterruptedException ex) when (IsPermanentRejection(ex.ShutdownReason))
        {
            throw new MessageRejectedException(
                $"The broker closed the channel: {ex.ShutdownReason!.ReplyCode} {ex.ShutdownReason.ReplyText}", isPermanent: true, ex);
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

    /// <summary>
    /// The broker closed the channel because of this message: the exchange does not exist (404) or the message
    /// breaks a limit such as the maximum message size (406). Republishing the same message cannot succeed.
    /// </summary>
    private static bool IsPermanentRejection(ShutdownEventArgs? reason) =>
        reason is { Initiator: ShutdownInitiator.Peer, ReplyCode: Constants.NotFound or Constants.PreconditionFailed };

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
