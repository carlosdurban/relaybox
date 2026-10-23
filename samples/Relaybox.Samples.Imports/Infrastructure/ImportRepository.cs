using MongoDB.Driver;
using Relaybox.MongoDb;
using Relaybox.Samples.Imports.Contracts;
using Relaybox.Samples.Imports.Domain;

namespace Relaybox.Samples.Imports.Infrastructure;

/// <summary>
/// Saves the whole aggregate with optimistic concurrency and its events in the tenant's outbox,
/// in one transaction.
/// </summary>
public sealed class ImportRepository(
    IMongoClient client,
    IMongoTenantDatabaseProvider databases,
    ITenantContext tenantContext,
    IOutbox outbox)
{
    public async Task<Import?> GetAsync(Guid id, CancellationToken ct)
    {
        var imports = await CollectionAsync(ct);
        return await imports.Find(i => i.Id == id).FirstOrDefaultAsync(ct);
    }

    public async Task SaveAsync(Import import, CancellationToken ct)
    {
        var imports = await CollectionAsync(ct);
        var expectedVersion = import.Version;

        using var session = await client.StartSessionAsync(cancellationToken: ct);
        session.StartTransaction();

        import.IncrementVersion();
        if (expectedVersion == 0)
        {
            await imports.InsertOneAsync(session, import, cancellationToken: ct);
        }
        else
        {
            var result = await imports.ReplaceOneAsync(
                session, i => i.Id == import.Id && i.Version == expectedVersion, import, cancellationToken: ct);
            if (result.ModifiedCount == 0)
            {
                await session.AbortTransactionAsync(ct);
                throw new ConcurrencyException(import.Id, expectedVersion);
            }
        }

        await outbox.AddRangeAsync(session, import.DomainEvents.Select(ToContract), ct);
        await session.CommitTransactionAsync(ct);
        import.ClearDomainEvents();
    }

    private async Task<IMongoCollection<Import>> CollectionAsync(CancellationToken ct)
    {
        var database = await databases.GetDatabaseAsync(tenantContext.TenantId, ct);
        return database.GetCollection<Import>("imports");
    }

    private static object ToContract(IDomainEvent @event) => @event switch
    {
        ImportStarted e => new ImportStartedV1(e.ImportId, e.TenantId, e.FileName, e.ExpectedRows, e.OccurredAt),
        ImportCompleted e => new ImportCompletedV1(
            e.ImportId, e.TenantId, e.FileName, e.Status == ImportStatus.CompletedWithErrors, e.ProcessedRows, e.FailedRows, e.OccurredAt),
        ImportFailed e => new ImportFailedV1(e.ImportId, e.TenantId, e.FileName, e.Reason, e.OccurredAt),
        _ => throw new InvalidOperationException($"Domain event {@event.GetType().Name} has no public contract."),
    };
}

public sealed class ConcurrencyException(Guid id, int expectedVersion)
    : Exception($"Import {id} was modified by someone else (expected version {expectedVersion}).");
