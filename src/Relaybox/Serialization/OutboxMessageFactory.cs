using System.Diagnostics;

namespace Relaybox.Serialization;

/// <summary>
/// Turns events raised by the application into outbox messages. Used by the database-specific
/// <c>IOutbox</c> implementations.
/// </summary>
public sealed class OutboxMessageFactory(MessageTypeMap types, IMessageSerializer serializer, TimeProvider time)
{
    private static readonly HashSet<string> ReservedHeaders =
        new([RelayboxHeaders.TenantId, RelayboxHeaders.TraceParent], StringComparer.OrdinalIgnoreCase);

    public OutboxMessage Create(string tenantId, object @event, OutboxMessageOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);
        ArgumentNullException.ThrowIfNull(@event);

        if (options?.Headers?.Keys.FirstOrDefault(ReservedHeaders.Contains) is { } reserved)
        {
            throw new ArgumentException($"Header '{reserved}' is set by Relaybox and cannot be overridden.", nameof(options));
        }

        return new OutboxMessage(
            Id: Guid.NewGuid(),
            TenantId: tenantId,
            Type: types.GetName(@event.GetType()),
            Payload: serializer.Serialize(@event),
            OccurredAt: time.GetUtcNow().UtcDateTime,
            Attempts: 0,
            TraceParent: Activity.Current?.Id,
            Destination: options?.Destination,
            RoutingKey: options?.RoutingKey,
            Headers: options?.Headers is { Count: > 0 } headers ? new Dictionary<string, string>(headers) : null);
    }
}
