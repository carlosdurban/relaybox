namespace Relaybox.Inbound;

/// <summary>Options for processing inbound messages.</summary>
public sealed class InboundOptions
{
    /// <summary>
    /// Attempts per delivery for transient failures before dead-lettering the message. Default: 5.
    /// </summary>
    public int MaxAttempts { get; set; } = 5;

    /// <summary>Delay after the first transient failure. Doubles on every attempt. Default: 200 ms.</summary>
    public TimeSpan RetryBaseDelay { get; set; } = TimeSpan.FromMilliseconds(200);

    /// <summary>Upper bound of the retry delay. Default: 10 seconds.</summary>
    public TimeSpan RetryMaxDelay { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Predicates that identify transient failures (infrastructure, concurrency conflicts), which are retried.
    /// Any other exception, including domain rule violations, sends the message to the dead letter queue.
    /// Database integrations add their own (e.g. MongoDB connection errors and write conflicts).
    /// </summary>
    public IList<Func<Exception, bool>> TransientExceptions { get; } =
    [
        static ex => ex is TimeoutException or IOException or HttpRequestException,
    ];

    internal bool IsTransient(Exception exception) =>
        TransientExceptions.Any(predicate => predicate(exception))
        || (exception.InnerException is { } inner && IsTransient(inner));

    internal TimeSpan RetryDelay(int attempt)
    {
        var delay = RetryBaseDelay.TotalMilliseconds * Math.Pow(2, Math.Clamp(attempt - 1, 0, 30));
        return TimeSpan.FromMilliseconds(Math.Min(delay, RetryMaxDelay.TotalMilliseconds));
    }
}
