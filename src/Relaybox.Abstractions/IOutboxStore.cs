namespace Relaybox;

/// <summary>
/// Database-specific access to the outbox of each tenant.
/// </summary>
public interface IOutboxStore
{
    /// <summary>
    /// Atomically claims the oldest pending message of the tenant, locking it for <paramref name="lockDuration"/>.
    /// Returns <c>null</c> when there is nothing to publish. Concurrent callers never get the same message
    /// while the lock is held.
    /// </summary>
    Task<OutboxMessage?> ClaimNextAsync(string tenantId, string lockOwner, TimeSpan lockDuration, CancellationToken ct);

    /// <summary>Marks the message as published.</summary>
    Task MarkProcessedAsync(string tenantId, Guid messageId, CancellationToken ct);

    /// <summary>
    /// Records a failed attempt. The message stays locked until <paramref name="retryAt"/>, which acts as backoff.
    /// </summary>
    Task MarkFailedAsync(string tenantId, Guid messageId, string error, DateTime retryAt, CancellationToken ct);
}
