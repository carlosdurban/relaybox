namespace Relaybox;

/// <summary>
/// Loads the active tenants from wherever the application keeps them (a catalog database, configuration...).
/// Implemented by the application. Relaybox caches the result and refreshes it periodically,
/// so this can be an expensive call.
/// </summary>
public interface ITenantSource
{
    Task<IReadOnlyList<TenantInfo>> LoadActiveTenantsAsync(CancellationToken ct);
}
