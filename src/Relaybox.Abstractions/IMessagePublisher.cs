namespace Relaybox;

/// <summary>
/// Broker-specific publication of outbox messages.
/// </summary>
public interface IMessagePublisher
{
    /// <summary>
    /// Publishes the message and completes only once the broker has confirmed it.
    /// Must throw if the broker does not confirm it.
    /// </summary>
    Task PublishAsync(OutboxMessage message, CancellationToken ct);
}
