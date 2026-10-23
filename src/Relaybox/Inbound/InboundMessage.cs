namespace Relaybox.Inbound;

/// <summary>A message received from a broker, before validation.</summary>
public sealed record InboundMessage(
    string? MessageId,
    string? TenantId,
    string? MessageType,
    ReadOnlyMemory<byte> Body,
    string? TraceParent);

/// <summary>What the broker integration must do with the delivery.</summary>
public enum DispatchOutcome
{
    /// <summary>Processed, or already processed before (duplicate).</summary>
    Ack,

    /// <summary>Cannot be processed: send it to the dead letter queue. Never redelivered.</summary>
    DeadLetter,

    /// <summary>Not processed because the consumer is stopping: return it to the queue.</summary>
    Requeue,
}
