namespace Relaybox.Tenancy;

internal sealed class StaticTenantSource(IReadOnlyList<TenantInfo> tenants) : ITenantSource
{
    public Task<IReadOnlyList<TenantInfo>> LoadActiveTenantsAsync(CancellationToken ct) => Task.FromResult(tenants);
}
