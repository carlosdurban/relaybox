namespace Relaybox;

/// <summary>
/// Database-specific idempotent execution of inbound messages.
/// Resolved from the tenant scope of the message.
/// </summary>
public interface IInbox
{
    /// <summary>
    /// Runs <paramref name="handler"/> inside a transaction that also records <see cref="MessageContext.MessageId"/>
    /// as processed. If the id was already recorded, the handler is not run and
    /// <see cref="InboxResult.Duplicate"/> is returned. If the handler throws, nothing is recorded.
    /// </summary>
    Task<InboxResult> ExecuteOnceAsync(MessageContext context, Func<CancellationToken, Task> handler, CancellationToken ct);
}

public enum InboxResult
{
    Processed,
    Duplicate,
}
