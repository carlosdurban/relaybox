using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Relaybox.Tenancy;

/// <summary>
/// Caches <see cref="ITenantSource"/> and refreshes it every <see cref="RelayboxOptions.TenantRefreshInterval"/>.
/// A lookup miss forces a refresh (at most once per <see cref="RelayboxOptions.TenantMissRefreshCooldown"/>),
/// so a tenant created a moment ago is found without waiting for the next periodic refresh.
/// If the source fails, the last known tenants keep being served.
/// </summary>
internal sealed class CachingTenantRegistry(
    ITenantSource source,
    IOptions<RelayboxOptions> options,
    TimeProvider time,
    ILogger<CachingTenantRegistry> logger) : ITenantRegistry
{
    private readonly SemaphoreSlim _refreshGate = new(1, 1);
    private volatile Snapshot? _snapshot;

    public async ValueTask<IReadOnlyList<TenantInfo>> GetActiveTenantsAsync(CancellationToken ct) =>
        (await GetSnapshotAsync(options.Value.TenantRefreshInterval, ct)).Tenants;

    public ValueTask<TenantInfo?> FindByIdAsync(string tenantId, CancellationToken ct) =>
        FindAsync(s => s.ById, tenantId, ct);

    public ValueTask<TenantInfo?> FindByDatabaseAsync(string databaseName, CancellationToken ct) =>
        FindAsync(s => s.ByDatabase, databaseName, ct);

    private async ValueTask<TenantInfo?> FindAsync(
        Func<Snapshot, IReadOnlyDictionary<string, TenantInfo>> index, string key, CancellationToken ct)
    {
        var snapshot = await GetSnapshotAsync(options.Value.TenantRefreshInterval, ct);
        if (index(snapshot).TryGetValue(key, out var tenant))
        {
            return tenant;
        }

        snapshot = await GetSnapshotAsync(options.Value.TenantMissRefreshCooldown, ct);
        return index(snapshot).GetValueOrDefault(key);
    }

    private async ValueTask<Snapshot> GetSnapshotAsync(TimeSpan maxAge, CancellationToken ct)
    {
        var current = _snapshot;
        if (current is not null && Age(current) < maxAge)
        {
            return current;
        }

        await _refreshGate.WaitAsync(ct);
        try
        {
            current = _snapshot;
            if (current is not null && Age(current) < maxAge)
            {
                return current;
            }

            try
            {
                var tenants = await source.LoadActiveTenantsAsync(ct);
                var loaded = new Snapshot(tenants, time.GetUtcNow());
                _snapshot = loaded;
                return loaded;
            }
            catch (Exception ex) when (current is not null && !ct.IsCancellationRequested)
            {
                logger.LogWarning(ex, "Could not refresh the tenant list; using the last known {TenantCount} tenants",
                    current.Tenants.Count);
                return current;
            }
        }
        finally
        {
            _refreshGate.Release();
        }
    }

    private TimeSpan Age(Snapshot snapshot) => time.GetUtcNow() - snapshot.LoadedAt;

    private sealed class Snapshot
    {
        public Snapshot(IReadOnlyList<TenantInfo> tenants, DateTimeOffset loadedAt)
        {
            Tenants = tenants;
            LoadedAt = loadedAt;
            ById = tenants.ToDictionary(t => t.TenantId, StringComparer.Ordinal);
            ByDatabase = tenants.ToDictionary(t => t.DatabaseName, StringComparer.Ordinal);
        }

        public IReadOnlyList<TenantInfo> Tenants { get; }
        public DateTimeOffset LoadedAt { get; }
        public IReadOnlyDictionary<string, TenantInfo> ById { get; }
        public IReadOnlyDictionary<string, TenantInfo> ByDatabase { get; }
    }
}
