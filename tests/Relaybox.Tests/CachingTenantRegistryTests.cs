using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;

namespace Relaybox.Tests;

public sealed class CachingTenantRegistryTests
{
    private readonly FakeTimeProvider _time = new();
    private readonly CountingTenantSource _source = new();
    private readonly ITenantRegistry _registry;

    public CachingTenantRegistryTests()
    {
        var services = new ServiceCollection().AddLogging().AddSingleton<TimeProvider>(_time);
        services.AddRelaybox(o =>
        {
            o.TenantRefreshInterval = TimeSpan.FromMinutes(1);
            o.TenantMissRefreshCooldown = TimeSpan.FromSeconds(10);
        });
        services.AddSingleton<ITenantSource>(_source);
        _registry = services.BuildServiceProvider().GetRequiredService<ITenantRegistry>();
        _source.Tenants = [new TenantInfo("acme", "acme_db")];
    }

    [Fact]
    public async Task Caches_the_tenants_until_the_refresh_interval()
    {
        await _registry.GetActiveTenantsAsync(default);
        await _registry.FindByIdAsync("acme", default);
        Assert.Equal(1, _source.Loads);

        _time.Advance(TimeSpan.FromMinutes(1));
        await _registry.GetActiveTenantsAsync(default);
        Assert.Equal(2, _source.Loads);
    }

    [Fact]
    public async Task Finds_tenants_by_id_and_by_database()
    {
        Assert.Equal("acme_db", (await _registry.FindByIdAsync("acme", default))?.DatabaseName);
        Assert.Equal("acme", (await _registry.FindByDatabaseAsync("acme_db", default))?.TenantId);
    }

    [Fact]
    public async Task A_new_tenant_is_found_after_the_miss_cooldown_without_waiting_for_the_refresh()
    {
        await _registry.GetActiveTenantsAsync(default);
        _source.Tenants = [.. _source.Tenants, new TenantInfo("globex", "globex_db")];

        Assert.Null(await _registry.FindByDatabaseAsync("globex_db", default));

        _time.Advance(TimeSpan.FromSeconds(10));
        Assert.Equal("globex", (await _registry.FindByDatabaseAsync("globex_db", default))?.TenantId);
    }

    [Fact]
    public async Task Keeps_serving_the_last_known_tenants_when_the_source_fails()
    {
        await _registry.GetActiveTenantsAsync(default);
        _source.Fail = true;
        _time.Advance(TimeSpan.FromMinutes(5));

        var tenants = await _registry.GetActiveTenantsAsync(default);

        Assert.Equal("acme", Assert.Single(tenants).TenantId);
    }

    private sealed class CountingTenantSource : ITenantSource
    {
        public IReadOnlyList<TenantInfo> Tenants { get; set; } = [];
        public int Loads { get; private set; }
        public bool Fail { get; set; }

        public Task<IReadOnlyList<TenantInfo>> LoadActiveTenantsAsync(CancellationToken ct)
        {
            Loads++;
            return Fail ? throw new InvalidOperationException("Catalog unavailable") : Task.FromResult(Tenants);
        }
    }
}
