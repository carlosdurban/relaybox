namespace Relaybox;

/// <summary>
/// Broker-specific publication of outbox messages.
/// </summary>
public interface IMessagePublisher
{
    /// <summary>
    /// Publishes the message and completes only once the broker has confirmed it. Must throw if the broker
    /// does not confirm it: <see cref="MessageRejectedException"/> when the broker refuses this message,
    /// any other exception when the broker cannot be reached.
    /// </summary>
    Task PublishAsync(OutboxMessage message, CancellationToken ct);
}

/// <summary>
/// The broker refused the message, as opposed to being unreachable.
/// </summary>
/// <remarks>
/// Unreachable brokers are retried for as long as it takes. Rejected messages are retried up to the relay's
/// maximum attempts, and permanent rejections (e.g. the destination does not exist) are failed at once,
/// so a message the broker will never accept cannot block the outbox.
/// </remarks>
public sealed class MessageRejectedException(string message, bool isPermanent, Exception? innerException = null)
    : Exception(message, innerException)
{
    /// <summary>Retrying cannot succeed without someone changing the broker setup or the message.</summary>
    public bool IsPermanent { get; } = isPermanent;
}
