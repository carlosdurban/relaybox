using MongoDB.Driver;
using Relaybox.Serialization;

namespace Relaybox.MongoDb;

/// <summary>
/// Adds events to the outbox of the current tenant as part of the caller's MongoDB transaction,
/// so they are stored if and only if the business change is.
/// </summary>
public interface IOutbox
{
    Task AddAsync(IClientSessionHandle session, object @event, CancellationToken ct = default);

    Task AddRangeAsync(IClientSessionHandle session, IEnumerable<object> events, CancellationToken ct = default);
}

internal sealed class MongoOutbox(
    ITenantContext tenantContext,
    MongoTenantCollections collections,
    OutboxMessageFactory messageFactory) : IOutbox
{
    public Task AddAsync(IClientSessionHandle session, object @event, CancellationToken ct = default) =>
        AddRangeAsync(session, [@event], ct);

    public async Task AddRangeAsync(IClientSessionHandle session, IEnumerable<object> events, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(events);

        if (!session.IsInTransaction)
        {
            throw new InvalidOperationException(
                "Events must be added to the outbox inside a transaction; otherwise they can be stored without the change that raised them, or lost.");
        }

        var tenantId = tenantContext.TenantId;
        var documents = events.Select(e => OutboxDocument.From(messageFactory.Create(tenantId, e))).ToList();
        if (documents.Count == 0)
        {
            return;
        }

        var outbox = await collections.GetOutboxAsync(tenantId, ct);
        await outbox.InsertManyAsync(session, documents, cancellationToken: ct);
    }
}
