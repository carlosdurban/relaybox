using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Relaybox.MongoDb;

/// <summary>
/// Outbox and inbox collections of each tenant. Their indexes are created the first time a tenant
/// database is used by this process; TTL changes are applied to existing indexes.
/// </summary>
internal sealed class MongoTenantCollections(
    IMongoTenantDatabaseProvider databases,
    IOptions<MongoRelayboxOptions> options,
    ILogger<MongoTenantCollections> logger)
{
    private const int IndexOptionsConflict = 85;
    private const string OutboxClaimIndex = "relaybox_claim";
    private const string OutboxTtlIndex = "relaybox_ttl";
    private const string InboxTtlIndex = "relaybox_ttl";

    private readonly ConcurrentDictionary<string, Lazy<Task>> _initialized = new(StringComparer.Ordinal);

    public async ValueTask<IMongoCollection<OutboxDocument>> GetOutboxAsync(string tenantId, CancellationToken ct)
    {
        var database = await GetInitializedDatabaseAsync(tenantId, ct);
        return database.GetCollection<OutboxDocument>(options.Value.OutboxCollectionName);
    }

    public async ValueTask<IMongoCollection<InboxDocument>> GetInboxAsync(string tenantId, CancellationToken ct)
    {
        var database = await GetInitializedDatabaseAsync(tenantId, ct);
        return database.GetCollection<InboxDocument>(options.Value.InboxCollectionName);
    }

    private async ValueTask<IMongoDatabase> GetInitializedDatabaseAsync(string tenantId, CancellationToken ct)
    {
        var database = await databases.GetDatabaseAsync(tenantId, ct);
        var key = database.DatabaseNamespace.DatabaseName;
        var initialization = _initialized.GetOrAdd(key, _ => new Lazy<Task>(() => CreateIndexesAsync(database)));

        try
        {
            await initialization.Value.WaitAsync(ct);
        }
        catch (Exception) when (initialization.Value.IsFaulted)
        {
            _initialized.TryRemove(new KeyValuePair<string, Lazy<Task>>(key, initialization));
            throw;
        }

        return database;
    }

    private async Task CreateIndexesAsync(IMongoDatabase database)
    {
        var settings = options.Value;

        var outbox = database.GetCollection<OutboxDocument>(settings.OutboxCollectionName);
        var outboxKeys = Builders<OutboxDocument>.IndexKeys;
        await outbox.Indexes.CreateOneAsync(new CreateIndexModel<OutboxDocument>(
            outboxKeys.Ascending(d => d.Status).Ascending(d => d.OccurredAt),
            new CreateIndexOptions { Name = OutboxClaimIndex }));
        await CreateTtlIndexAsync(database, outbox, outboxKeys.Ascending(d => d.ProcessedAt), OutboxTtlIndex,
            settings.OutboxRetention);

        var inbox = database.GetCollection<InboxDocument>(settings.InboxCollectionName);
        await CreateTtlIndexAsync(database, inbox, Builders<InboxDocument>.IndexKeys.Ascending(d => d.ProcessedAt),
            InboxTtlIndex, settings.InboxRetention);

        logger.LogDebug("Relaybox indexes ensured in database {Database}", database.DatabaseNamespace.DatabaseName);
    }

    private static async Task CreateTtlIndexAsync<T>(
        IMongoDatabase database, IMongoCollection<T> collection, IndexKeysDefinition<T> keys, string name, TimeSpan retention)
    {
        try
        {
            await collection.Indexes.CreateOneAsync(new CreateIndexModel<T>(
                keys, new CreateIndexOptions { Name = name, ExpireAfter = retention }));
        }
        catch (MongoCommandException ex) when (ex.Code == IndexOptionsConflict)
        {
            // The index exists with a different retention: update it in place.
            await database.RunCommandAsync<BsonDocument>(new BsonDocument
            {
                { "collMod", collection.CollectionNamespace.CollectionName },
                { "index", new BsonDocument { { "name", name }, { "expireAfterSeconds", (long)retention.TotalSeconds } } },
            });
        }
    }
}
