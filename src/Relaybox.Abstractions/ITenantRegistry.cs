namespace Relaybox;

/// <summary>
/// Cached view of the active tenants. Provided by Relaybox on top of <see cref="ITenantSource"/>.
/// </summary>
public interface ITenantRegistry
{
    ValueTask<IReadOnlyList<TenantInfo>> GetActiveTenantsAsync(CancellationToken ct);

    ValueTask<TenantInfo?> FindByIdAsync(string tenantId, CancellationToken ct);

    ValueTask<TenantInfo?> FindByDatabaseAsync(string databaseName, CancellationToken ct);
}
