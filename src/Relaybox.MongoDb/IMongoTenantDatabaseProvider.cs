using MongoDB.Driver;

namespace Relaybox.MongoDb;

/// <summary>Resolves the database of a tenant.</summary>
public interface IMongoTenantDatabaseProvider
{
    /// <summary>Throws <see cref="UnknownTenantException"/> if the tenant is not active.</summary>
    ValueTask<IMongoDatabase> GetDatabaseAsync(string tenantId, CancellationToken ct);
}

internal sealed class MongoTenantDatabaseProvider(IMongoClient client, ITenantRegistry registry) : IMongoTenantDatabaseProvider
{
    public async ValueTask<IMongoDatabase> GetDatabaseAsync(string tenantId, CancellationToken ct)
    {
        var tenant = await registry.FindByIdAsync(tenantId, ct) ?? throw new UnknownTenantException(tenantId);
        return client.GetDatabase(tenant.DatabaseName);
    }
}

/// <summary>The tenant is not in the list of active tenants.</summary>
public sealed class UnknownTenantException(string tenantId) : Exception($"Tenant '{tenantId}' is not an active tenant.")
{
    public string TenantId { get; } = tenantId;
}
