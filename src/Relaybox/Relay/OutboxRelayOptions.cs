namespace Relaybox.Relay;

/// <summary>Options of the outbox relay (the background publisher).</summary>
public sealed class OutboxRelayOptions
{
    /// <summary>Tenants processed concurrently by this instance. Default: 4.</summary>
    public int MaxDegreeOfParallelism { get; set; } = 4;

    /// <summary>
    /// Messages published for a tenant before yielding to the next one, so a busy tenant cannot starve the rest.
    /// The tenant is queued again right away if it may have more. Default: 100.
    /// </summary>
    public int MaxMessagesPerTenantPerRound { get; set; } = 100;

    /// <summary>
    /// How long a claimed message stays locked. Must be much longer than a publication; if an instance dies,
    /// its claimed messages are picked up by another one when the lock expires. Default: 2 minutes.
    /// </summary>
    public TimeSpan LockDuration { get; set; } = TimeSpan.FromMinutes(2);

    /// <summary>
    /// Interval of the reconciliation that checks every tenant regardless of signals. Covers start-up,
    /// lost change stream events and expired locks. Default: 1 minute.
    /// </summary>
    public TimeSpan ReconciliationInterval { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>Delay after the first failed publication of a message. Doubles on every attempt. Default: 5 seconds.</summary>
    public TimeSpan RetryBaseDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Upper bound of the retry delay. Default: 5 minutes.</summary>
    public TimeSpan RetryMaxDelay { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>Delay before restarting a signal source that failed or ended. Default: 5 seconds.</summary>
    public TimeSpan SignalSourceRestartDelay { get; set; } = TimeSpan.FromSeconds(5);

    internal TimeSpan RetryDelay(int attempts)
    {
        var exponent = Math.Clamp(attempts - 1, 0, 30);
        var delay = RetryBaseDelay.TotalMilliseconds * Math.Pow(2, exponent);
        return TimeSpan.FromMilliseconds(Math.Min(delay, RetryMaxDelay.TotalMilliseconds));
    }
}
