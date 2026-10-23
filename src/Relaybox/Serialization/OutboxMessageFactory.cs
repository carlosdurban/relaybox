using System.Diagnostics;

namespace Relaybox.Serialization;

/// <summary>
/// Turns events raised by the application into outbox messages. Used by the database-specific
/// <c>IOutbox</c> implementations.
/// </summary>
public sealed class OutboxMessageFactory(MessageTypeMap types, IMessageSerializer serializer, TimeProvider time)
{
    public OutboxMessage Create(string tenantId, object @event)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);
        ArgumentNullException.ThrowIfNull(@event);

        return new OutboxMessage(
            Id: Guid.NewGuid(),
            TenantId: tenantId,
            Type: types.GetName(@event.GetType()),
            Payload: serializer.Serialize(@event),
            OccurredAt: time.GetUtcNow().UtcDateTime,
            Attempts: 0,
            TraceParent: Activity.Current?.Id);
    }
}
