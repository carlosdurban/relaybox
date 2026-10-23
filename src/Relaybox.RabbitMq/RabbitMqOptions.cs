namespace Relaybox.RabbitMq;

/// <summary>Connection and publication options of the RabbitMQ integration.</summary>
public sealed class RabbitMqOptions
{
    /// <summary>AMQP URI. Default: <c>amqp://guest:guest@localhost:5672/</c>.</summary>
    public string ConnectionString { get; set; } = "amqp://guest:guest@localhost:5672/";

    /// <summary>Connection name shown in the RabbitMQ management UI.</summary>
    public string? ClientProvidedName { get; set; }

    /// <summary>Exchange messages are published to and consumer queues are bound to. Default: <c>relaybox</c>.</summary>
    public string Exchange { get; set; } = "relaybox";

    /// <summary>Type of <see cref="Exchange"/> when it is declared. Default: <c>topic</c>.</summary>
    public string ExchangeType { get; set; } = "topic";

    /// <summary>Declare <see cref="Exchange"/> (durable) before using it. Default: <c>true</c>.</summary>
    public bool DeclareExchange { get; set; } = true;

    /// <summary>Routing key of each published message. Default: the message type name.</summary>
    public Func<OutboxMessage, string> RoutingKey { get; set; } = static message => message.Type;

    /// <summary>
    /// Fail the publication when no queue is bound for the routing key, instead of letting the broker drop it.
    /// Default: <c>false</c>.
    /// </summary>
    public bool Mandatory { get; set; }

    /// <summary>
    /// Maximum time to wait for the broker confirmation. An unresponsive broker would otherwise block
    /// the publication indefinitely. Default: 30 seconds.
    /// </summary>
    public TimeSpan PublishTimeout { get; set; } = TimeSpan.FromSeconds(30);
}

/// <summary>Options of one consumer queue.</summary>
public sealed class RabbitMqConsumerOptions
{
    /// <summary>Unacknowledged messages the broker sends to this consumer. Default: 16.</summary>
    public ushort PrefetchCount { get; set; } = 16;

    /// <summary>Messages processed concurrently. Default: 4.</summary>
    public ushort ConcurrentMessages { get; set; } = 4;

    /// <summary>
    /// Declare the queue, bind it to the exchange and give it a dead letter queue (<c>{queue}.dlq</c>).
    /// Default: <c>true</c>.
    /// </summary>
    public bool DeclareTopology { get; set; } = true;

    /// <summary>
    /// Routing keys bound to the queue when <see cref="DeclareTopology"/> is on.
    /// Default (empty): the names of the message types with a registered handler.
    /// </summary>
    public IList<string> RoutingKeys { get; } = [];

    /// <summary>Delay before retrying when the consumer cannot start. Default: 5 seconds.</summary>
    public TimeSpan RestartDelay { get; set; } = TimeSpan.FromSeconds(5);
}
