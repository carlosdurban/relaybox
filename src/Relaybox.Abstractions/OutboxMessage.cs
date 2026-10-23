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
/// <param name="Destination">Where to publish it (a RabbitMQ exchange); <c>null</c> for the publisher's default.</param>
/// <param name="RoutingKey">Routing key; <c>null</c> for the publisher's default.</param>
/// <param name="Headers">Additional headers sent with the message.</param>
public sealed record OutboxMessage(
    Guid Id,
    string TenantId,
    string Type,
    string Payload,
    DateTime OccurredAt,
    int Attempts,
    string? TraceParent,
    string? Destination = null,
    string? RoutingKey = null,
    IReadOnlyDictionary<string, string>? Headers = null);

/// <summary>Per-message publication settings, given when the event is added to the outbox.</summary>
public sealed class OutboxMessageOptions
{
    /// <summary>Where to publish the message (a RabbitMQ exchange). Default: the publisher's default.</summary>
    public string? Destination { get; init; }

    /// <summary>Routing key. Default: the publisher's default (for RabbitMQ, the message type name).</summary>
    public string? RoutingKey { get; init; }

    /// <summary>Additional headers. <c>tenant-id</c> and <c>traceparent</c> are reserved.</summary>
    public IReadOnlyDictionary<string, string>? Headers { get; init; }
}
