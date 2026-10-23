using MongoDB.Driver;

namespace Relaybox.MongoDb;

/// <summary>
/// Gives message handlers (and the repositories they use) the session of the inbox transaction.
/// Scoped: only available while a message is being handled.
/// </summary>
public interface IMongoSessionAccessor
{
    /// <summary>The session of the current inbox transaction. Throws outside a message handler.</summary>
    IClientSessionHandle Session { get; }

    bool HasSession { get; }
}

internal sealed class MongoSessionAccessor : IMongoSessionAccessor
{
    private IClientSessionHandle? _session;

    public IClientSessionHandle Session =>
        _session ?? throw new InvalidOperationException("There is no inbox transaction in the current scope.");

    public bool HasSession => _session is not null;

    internal void Set(IClientSessionHandle? session) => _session = session;
}

/// <summary>
/// Inbox with <c>_id = MessageId</c>: the unique index makes deduplication atomic. The insert and the
/// handlers share one transaction, so a failed handler leaves no trace and the message can be retried.
/// </summary>
internal sealed class MongoInbox(
    IMongoClient client,
    MongoTenantCollections collections,
    MongoSessionAccessor sessionAccessor,
    TimeProvider time) : IInbox
{
    public async Task<InboxResult> ExecuteOnceAsync(
        MessageContext context, Func<CancellationToken, Task> handler, CancellationToken ct)
    {
        var inbox = await collections.GetInboxAsync(context.TenantId, ct);
        using var session = await client.StartSessionAsync(cancellationToken: ct);
        sessionAccessor.Set(session);

        try
        {
            return await session.WithTransactionAsync(
                async (s, token) =>
                {
                    var record = new InboxDocument
                    {
                        Id = context.MessageId,
                        TenantId = context.TenantId,
                        Type = context.MessageType,
                        ProcessedAt = time.GetUtcNow().UtcDateTime,
                    };

                    try
                    {
                        await inbox.InsertOneAsync(s, record, cancellationToken: token);
                    }
                    catch (MongoWriteException ex) when (ex.WriteError?.Category == ServerErrorCategory.DuplicateKey)
                    {
                        throw new AlreadyProcessedException();
                    }

                    await handler(token);
                    return InboxResult.Processed;
                },
                cancellationToken: ct);
        }
        catch (AlreadyProcessedException)
        {
            return InboxResult.Duplicate;
        }
        finally
        {
            sessionAccessor.Set(null);
        }
    }

    /// <summary>Aborts the transaction when the message id is already in the inbox.</summary>
    private sealed class AlreadyProcessedException : Exception;
}
