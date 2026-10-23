using System.Collections.Concurrent;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Relaybox.IntegrationTests.Infrastructure;

/// <summary>Observes the broker directly, without Relaybox: what was published, and raw publications.</summary>
public sealed class BrokerProbe : IAsyncDisposable
{
    private readonly IConnection _connection;
    private readonly IChannel _channel;

    private BrokerProbe(IConnection connection, IChannel channel)
    {
        _connection = connection;
        _channel = channel;
    }

    public ConcurrentQueue<ReceivedMessage> Received { get; } = new();

    public static async Task<BrokerProbe> StartAsync(InfrastructureFixture infrastructure, TestScenario scenario, bool listen = true)
    {
        var factory = new ConnectionFactory { Uri = new Uri(infrastructure.RabbitConnectionString) };
        var connection = await factory.CreateConnectionAsync();
        var channel = await connection.CreateChannelAsync();
        var probe = new BrokerProbe(connection, channel);

        await channel.ExchangeDeclareAsync(scenario.Exchange, ExchangeType.Topic, durable: true);
        if (listen)
        {
            var queue = await channel.QueueDeclareAsync(exclusive: true);
            await channel.QueueBindAsync(queue.QueueName, scenario.Exchange, "#");

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += (_, delivery) =>
            {
                var headers = delivery.BasicProperties.Headers;
                var tenant = headers?.TryGetValue(RelayboxHeaders.TenantId, out var value) == true
                    ? Encoding.UTF8.GetString((byte[])value!)
                    : null;
                probe.Received.Enqueue(new ReceivedMessage(
                    delivery.BasicProperties.MessageId, tenant, delivery.BasicProperties.Type, delivery.RoutingKey,
                    Encoding.UTF8.GetString(delivery.Body.Span)));
                return Task.CompletedTask;
            };
            await channel.BasicConsumeAsync(queue.QueueName, autoAck: true, consumer);
        }

        return probe;
    }

    public async Task PublishRawAsync(
        TestScenario scenario, string messageId, string? tenantId, string type, string body)
    {
        var properties = new BasicProperties { MessageId = messageId, Type = type, Headers = new Dictionary<string, object?>() };
        if (tenantId is not null)
        {
            properties.Headers[RelayboxHeaders.TenantId] = tenantId;
        }

        await _channel.BasicPublishAsync(scenario.Exchange, type, mandatory: false, properties, Encoding.UTF8.GetBytes(body));
    }

    public async Task WaitForConsumerAsync(string queue)
    {
        await Eventually.TrueAsync(async () =>
        {
            // A passive declare of a missing queue closes the channel, so each check uses its own.
            await using var channel = await _connection.CreateChannelAsync();
            try
            {
                var declared = await channel.QueueDeclarePassiveAsync(queue);
                return declared.ConsumerCount > 0;
            }
            catch (RabbitMQ.Client.Exceptions.OperationInterruptedException)
            {
                return false;
            }
        });
    }

    public async Task<uint> CountAsync(string queue) => (await _channel.QueueDeclarePassiveAsync(queue)).MessageCount;

    public async ValueTask DisposeAsync()
    {
        await _channel.DisposeAsync();
        await _connection.DisposeAsync();
    }
}

public sealed record ReceivedMessage(string? MessageId, string? TenantId, string? Type, string RoutingKey, string Body);

public static class Eventually
{
    public static Task TrueAsync(Func<bool> condition, TimeSpan? timeout = null) =>
        TrueAsync(() => Task.FromResult(condition()), timeout);

    public static async Task TrueAsync(Func<Task<bool>> condition, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(20));
        while (!await condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException("The condition was not met in time.");
            }

            await Task.Delay(100);
        }
    }
}
