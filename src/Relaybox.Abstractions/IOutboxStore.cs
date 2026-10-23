namespace Relaybox;

/// <summary>
/// Database-specific access to the outbox of each tenant. A message is pending, processed (published)
/// or failed (given up on; it stays in the outbox until someone requeues or deletes it).
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
    /// Records a failed attempt. The message stays pending but locked until <paramref name="retryAt"/>,
    /// which acts as backoff.
    /// </summary>
    Task ScheduleRetryAsync(string tenantId, Guid messageId, string error, DateTime retryAt, CancellationToken ct);

    /// <summary>Gives up on the message: it is no longer claimed, and keeps the error for diagnosis.</summary>
    Task MarkFailedAsync(string tenantId, Guid messageId, string error, CancellationToken ct);

    /// <summary>Pending and failed messages of the tenant, for metrics and health checks.</summary>
    Task<OutboxStatistics> GetStatisticsAsync(string tenantId, CancellationToken ct);
}

/// <param name="Pending">Messages waiting to be published.</param>
/// <param name="Failed">Messages given up on.</param>
/// <param name="OldestPendingAt">When the oldest pending message was added, if any.</param>
public sealed record OutboxStatistics(long Pending, long Failed, DateTime? OldestPendingAt);
