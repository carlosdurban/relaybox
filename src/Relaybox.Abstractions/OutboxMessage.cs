namespace Relaybox;

/// <summary>
/// A message claimed from a tenant's outbox, ready to be published.
/// </summary>
/// <param name="Id">Unique id. Published as the broker message id, so consumers can deduplicate.</param>
/// <param name="TenantId">Tenant that owns the message. Always sent as the <c>tenant-id</c> header.</param>
/// <param name="Type">Stable contract name of the event (not the CLR type name of a domain class).</param>
/// <param name="Payload">Serialized event contract (JSON).</param>
/// <param name="OccurredAt">When the event was added to the outbox (UTC).</param>
/// <param name="Attempts">Publication attempts so far, including the current one.</param>
/// <param name="TraceParent">W3C trace context captured when the event was added, if any.</param>
public sealed record OutboxMessage(
    Guid Id,
    string TenantId,
    string Type,
    string Payload,
    DateTime OccurredAt,
    int Attempts,
    string? TraceParent);
