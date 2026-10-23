using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Relaybox.MongoDb;

/// <summary>
/// Signals a tenant as soon as an outbox message is inserted in its database, using a single
/// cluster-wide change stream. It never publishes anything itself: every replica sees the same insert,
/// and the atomic claim in the store decides which one publishes it.
/// </summary>
/// <remarks>
/// The resume token is kept in memory, so the stream resumes without gaps when it is reopened inside
/// this process. It is deliberately not persisted: on start-up the reconciliation signals every tenant
/// anyway. If the stream cannot resume (history lost, invalid token), it starts from now and signals
/// every tenant to cover the gap.
/// </remarks>
public sealed class MongoChangeStreamSignalSource(
    IMongoClient client,
    ITenantRegistry registry,
    IOptions<MongoRelayboxOptions> options,
    ILogger<MongoChangeStreamSignalSource> logger) : IOutboxSignalSource
{
    private static readonly HashSet<int> ResumeFailureCodes =
    [
        260, // InvalidResumeToken
        280, // ChangeStreamFatalError
        286, // ChangeStreamHistoryLost
    ];

    private BsonDocument? _resumeToken;
    private bool _mayHaveMissedEvents;

    public async IAsyncEnumerable<string> WatchTenantsAsync([EnumeratorCancellation] CancellationToken ct)
    {
        using var cursor = await OpenAsync(ct);

        if (_mayHaveMissedEvents)
        {
            _mayHaveMissedEvents = false;
            foreach (var tenant in await registry.GetActiveTenantsAsync(ct))
            {
                yield return tenant.TenantId;
            }
        }

        while (true)
        {
            bool hasBatch;
            try
            {
                hasBatch = await cursor.MoveNextAsync(ct);
            }
            catch (MongoCommandException ex) when (ResumeFailureCodes.Contains(ex.Code))
            {
                logger.LogWarning(ex, "Outbox change stream can no longer resume; it will restart from now");
                ForgetResumeToken();
                yield break;
            }
            catch (Exception) when (!ct.IsCancellationRequested)
            {
                // Reopened by the relay; events after the last token are replayed, earlier gaps are covered here.
                _mayHaveMissedEvents = _resumeToken is null;
                throw;
            }

            if (!hasBatch)
            {
                yield break;
            }

            foreach (var change in cursor.Current)
            {
                var databaseName = change["ns"]["db"].AsString;
                var tenant = await registry.FindByDatabaseAsync(databaseName, ct);
                if (tenant is null)
                {
                    logger.LogDebug("Ignoring outbox insert in database {Database}, which is not an active tenant", databaseName);
                    continue;
                }

                yield return tenant.TenantId;
            }

            _resumeToken = cursor.GetResumeToken() ?? _resumeToken;
        }
    }

    private async Task<IChangeStreamCursor<BsonDocument>> OpenAsync(CancellationToken ct)
    {
        try
        {
            return await WatchAsync(ct);
        }
        catch (MongoCommandException ex) when (_resumeToken is not null && ResumeFailureCodes.Contains(ex.Code))
        {
            logger.LogWarning(ex, "Outbox change stream cannot resume from its last position; starting from now");
            ForgetResumeToken();
            return await WatchAsync(ct);
        }
    }

    private Task<IChangeStreamCursor<BsonDocument>> WatchAsync(CancellationToken ct)
    {
        var settings = options.Value;
        PipelineDefinition<ChangeStreamDocument<BsonDocument>, BsonDocument> pipeline = new[]
        {
            new BsonDocument("$match", new BsonDocument
            {
                { "operationType", "insert" },
                { "ns.coll", settings.OutboxCollectionName },
            }),
            new BsonDocument("$project", new BsonDocument { { "ns", 1 } }),
        };

        var watchOptions = new ChangeStreamOptions
        {
            StartAfter = _resumeToken,
            MaxAwaitTime = settings.ChangeStreamMaxAwaitTime,
        };

        logger.LogInformation("Opening outbox change stream {Position}", _resumeToken is null ? "from now" : "from the last position");
        return client.WatchAsync(pipeline, watchOptions, ct);
    }

    private void ForgetResumeToken()
    {
        _resumeToken = null;
        _mayHaveMissedEvents = true;
    }
}
