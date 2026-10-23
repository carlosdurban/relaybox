using MongoDB.Driver;

namespace Relaybox.MongoDb;

internal sealed class MongoOutboxStore(MongoTenantCollections collections, TimeProvider time) : IOutboxStore
{
    private static readonly FilterDefinitionBuilder<OutboxDocument> Filter = Builders<OutboxDocument>.Filter;
    private static readonly UpdateDefinitionBuilder<OutboxDocument> Update = Builders<OutboxDocument>.Update;

    public async Task<OutboxMessage?> ClaimNextAsync(
        string tenantId, string lockOwner, TimeSpan lockDuration, CancellationToken ct)
    {
        var outbox = await collections.GetOutboxAsync(tenantId, ct);
        var now = time.GetUtcNow().UtcDateTime;

        var pending = Filter.Eq(d => d.ProcessedAt, null)
            & (Filter.Eq(d => d.LockedUntil, null) | Filter.Lt(d => d.LockedUntil, now));
        var claim = Update
            .Set(d => d.LockedUntil, now + lockDuration)
            .Set(d => d.LockedBy, lockOwner)
            .Inc(d => d.Attempts, 1);
        var options = new FindOneAndUpdateOptions<OutboxDocument>
        {
            Sort = Builders<OutboxDocument>.Sort.Ascending(d => d.OccurredAt),
            ReturnDocument = ReturnDocument.After,
        };

        var document = await outbox.FindOneAndUpdateAsync(pending, claim, options, ct);
        return document?.ToMessage();
    }

    public async Task MarkProcessedAsync(string tenantId, Guid messageId, CancellationToken ct)
    {
        var outbox = await collections.GetOutboxAsync(tenantId, ct);
        await outbox.UpdateOneAsync(
            Filter.Eq(d => d.Id, messageId),
            Update
                .Set(d => d.ProcessedAt, time.GetUtcNow().UtcDateTime)
                .Set(d => d.LockedUntil, null)
                .Set(d => d.LockedBy, null)
                .Set(d => d.Error, null),
            cancellationToken: ct);
    }

    public async Task MarkFailedAsync(string tenantId, Guid messageId, string error, DateTime retryAt, CancellationToken ct)
    {
        var outbox = await collections.GetOutboxAsync(tenantId, ct);
        await outbox.UpdateOneAsync(
            Filter.Eq(d => d.Id, messageId) & Filter.Eq(d => d.ProcessedAt, null),
            Update
                .Set(d => d.Error, error)
                .Set(d => d.LockedUntil, retryAt)
                .Set(d => d.LockedBy, null),
            cancellationToken: ct);
    }
}
