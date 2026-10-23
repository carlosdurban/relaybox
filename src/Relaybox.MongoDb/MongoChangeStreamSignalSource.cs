using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using Relaybox.Diagnostics;

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
public sealed class MongoChangeStreamSignalSource : IOutboxSignalSource, IReportsSignalHealth
{
    private static readonly HashSet<int> ResumeFailureCodes =
    [
        260, // InvalidResumeToken
        280, // ChangeStreamFatalError
        286, // ChangeStreamHistoryLost
    ];

    private static readonly Meter Meter = new(RelayboxDiagnostics.Name);
    private static readonly Counter<long> Restarts =
        Meter.CreateCounter<long>("relaybox.changestream.restarts", "{restart}", "Times the outbox change stream was reopened.");

    private readonly IMongoClient _client;
    private readonly ITenantRegistry _registry;
    private readonly IOptions<MongoRelayboxOptions> _options;
    private readonly ILogger<MongoChangeStreamSignalSource> _logger;
    private BsonDocument? _resumeToken;
    private bool _mayHaveMissedEvents;
    private int _openings;
    private volatile bool _connected;

    public MongoChangeStreamSignalSource(
        IMongoClient client,
        ITenantRegistry registry,
        IOptions<MongoRelayboxOptions> options,
        ILogger<MongoChangeStreamSignalSource> logger)
    {
        _client = client;
        _registry = registry;
        _options = options;
        _logger = logger;
        Meter.CreateObservableGauge("relaybox.changestream.connected", () => _connected ? 1 : 0, "{connected}",
            "1 while the outbox change stream is open.");
    }

    public string Name => "MongoDB change stream";

    public bool IsConnected => _connected;

    public async IAsyncEnumerable<string> WatchTenantsAsync([EnumeratorCancellation] CancellationToken ct)
    {
        if (Interlocked.Increment(ref _openings) > 1)
        {
            Restarts.Add(1);
        }

        using var cursor = await OpenAsync(ct);
        _connected = true;
        try
        {
            await foreach (var tenantId in ReadAsync(cursor, ct))
            {
                yield return tenantId;
            }
        }
        finally
        {
            _connected = false;
        }
    }

    private async IAsyncEnumerable<string> ReadAsync(
        IChangeStreamCursor<BsonDocument> cursor, [EnumeratorCancellation] CancellationToken ct)
    {
        if (_mayHaveMissedEvents)
        {
            _mayHaveMissedEvents = false;
            foreach (var tenant in await _registry.GetActiveTenantsAsync(ct))
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
                _logger.LogWarning(ex, "Outbox change stream can no longer resume; it will restart from now");
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
                var tenant = await _registry.FindByDatabaseAsync(databaseName, ct);
                if (tenant is null)
                {
                    _logger.LogDebug("Ignoring outbox insert in database {Database}, which is not an active tenant", databaseName);
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
            _logger.LogWarning(ex, "Outbox change stream cannot resume from its last position; starting from now");
            ForgetResumeToken();
            return await WatchAsync(ct);
        }
    }

    private Task<IChangeStreamCursor<BsonDocument>> WatchAsync(CancellationToken ct)
    {
        var settings = _options.Value;
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

        _logger.LogInformation("Opening outbox change stream {Position}", _resumeToken is null ? "from now" : "from the last position");
        return _client.WatchAsync(pipeline, watchOptions, ct);
    }

    private void ForgetResumeToken()
    {
        _resumeToken = null;
        _mayHaveMissedEvents = true;
    }
}
