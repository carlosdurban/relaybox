namespace Relaybox;

/// <summary>
/// Tells the relay which tenants may have pending outbox messages.
/// </summary>
/// <remarks>
/// A signal is only a hint: the relay always claims messages from the store before publishing,
/// so duplicated, late or lost signals never cause duplicated or lost messages.
/// Implementations: polling (reconciliation), MongoDB change streams, and in the future
/// other change-data-capture mechanisms (Postgres <c>LISTEN/NOTIFY</c>, logical replication...).
/// </remarks>
public interface IOutboxSignalSource
{
    IAsyncEnumerable<string> WatchTenantsAsync(CancellationToken ct);
}
