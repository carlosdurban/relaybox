using System.Text;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Relaybox.Inbound;
using Relaybox.Serialization;

namespace Relaybox.RabbitMq;

/// <summary>
/// Consumes one queue and hands every delivery to the <see cref="InboundMessageDispatcher"/>.
/// Messages that cannot be processed are rejected without requeue, so the queue's dead letter
/// exchange moves them to the dead letter queue.
/// </summary>
internal sealed class RabbitMqConsumerService(
    string queue,
    RabbitMqConsumerOptions consumerOptions,
    RabbitMqConnection connection,
    IOptions<RabbitMqOptions> options,
    MessageTypeMap messageTypes,
    MessageHandlerRegistry handlers,
    InboundMessageDispatcher dispatcher,
    ILogger<RabbitMqConsumerService> logger) : BackgroundService
{
    private IChannel? _channel;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                _channel = await StartConsumingAsync(stoppingToken);
                logger.LogInformation("Consuming queue {Queue}", queue);
                return;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Could not start consuming queue {Queue}; retrying in {Delay}", queue,
                    consumerOptions.RestartDelay);
            }

            try
            {
                await Task.Delay(consumerOptions.RestartDelay, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);
        if (_channel is { } channel)
        {
            await channel.CloseAsync(cancellationToken);
            await channel.DisposeAsync();
        }
    }

    private async Task<IChannel> StartConsumingAsync(CancellationToken stoppingToken)
    {
        var conn = await connection.GetAsync(stoppingToken);
        var channel = await conn.CreateChannelAsync(
            new CreateChannelOptions(
                publisherConfirmationsEnabled: false,
                publisherConfirmationTrackingEnabled: false,
                consumerDispatchConcurrency: consumerOptions.ConcurrentMessages),
            stoppingToken);

        try
        {
            if (consumerOptions.DeclareTopology)
            {
                await DeclareTopologyAsync(channel, stoppingToken);
            }

            await channel.BasicQosAsync(0, consumerOptions.PrefetchCount, global: false, stoppingToken);

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += (_, delivery) => HandleDeliveryAsync(channel, delivery, stoppingToken);
            await channel.BasicConsumeAsync(queue, autoAck: false, consumer, stoppingToken);
            return channel;
        }
        catch
        {
            await channel.DisposeAsync();
            throw;
        }
    }

    private async Task DeclareTopologyAsync(IChannel channel, CancellationToken ct)
    {
        var settings = options.Value;
        var deadLetterExchange = $"{queue}.dlx";
        var deadLetterQueue = $"{queue}.dlq";

        await channel.ExchangeDeclareAsync(settings.Exchange, settings.ExchangeType, durable: true, cancellationToken: ct);
        await channel.ExchangeDeclareAsync(deadLetterExchange, ExchangeType.Fanout, durable: true, cancellationToken: ct);
        await channel.QueueDeclareAsync(deadLetterQueue, durable: true, exclusive: false, autoDelete: false, cancellationToken: ct);
        await channel.QueueBindAsync(deadLetterQueue, deadLetterExchange, routingKey: "", cancellationToken: ct);

        await channel.QueueDeclareAsync(queue, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?> { ["x-dead-letter-exchange"] = deadLetterExchange },
            cancellationToken: ct);

        var routingKeys = consumerOptions.RoutingKeys.Count > 0
            ? consumerOptions.RoutingKeys
            : handlers.MessageTypes.Select(messageTypes.GetName).ToList();
        foreach (var routingKey in routingKeys)
        {
            await channel.QueueBindAsync(queue, settings.Exchange, routingKey, cancellationToken: ct);
        }
    }

    private async Task HandleDeliveryAsync(IChannel channel, BasicDeliverEventArgs delivery, CancellationToken stoppingToken)
    {
        var properties = delivery.BasicProperties;
        var message = new InboundMessage(
            properties.MessageId,
            ReadHeader(properties, RelayboxHeaders.TenantId),
            properties.Type,
            delivery.Body,
            ReadHeader(properties, RelayboxHeaders.TraceParent));

        DispatchOutcome outcome;
        try
        {
            outcome = await dispatcher.DispatchAsync(message, stoppingToken);
        }
        catch (Exception ex)
        {
            logger.LogCritical(ex, "Unexpected error dispatching message {MessageId} from queue {Queue}; dead-lettering it",
                message.MessageId, queue);
            outcome = DispatchOutcome.DeadLetter;
        }

        switch (outcome)
        {
            case DispatchOutcome.Ack:
                await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false, CancellationToken.None);
                break;
            case DispatchOutcome.DeadLetter:
                await channel.BasicNackAsync(delivery.DeliveryTag, multiple: false, requeue: false, CancellationToken.None);
                break;
            case DispatchOutcome.Requeue:
                await channel.BasicNackAsync(delivery.DeliveryTag, multiple: false, requeue: true, CancellationToken.None);
                break;
        }
    }

    private static string? ReadHeader(IReadOnlyBasicProperties properties, string name) =>
        properties.Headers?.TryGetValue(name, out var value) == true
            ? value switch
            {
                byte[] bytes => Encoding.UTF8.GetString(bytes),
                string text => text,
                null => null,
                _ => value.ToString(),
            }
            : null;
}
