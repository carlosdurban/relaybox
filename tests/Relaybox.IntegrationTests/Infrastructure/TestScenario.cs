namespace Relaybox.IntegrationTests.Infrastructure;

/// <summary>Isolates a test: its own tenant databases, exchange and queue names.</summary>
public sealed class TestScenario
{
    private readonly string _id = Guid.NewGuid().ToString("N")[..8];

    public TestScenario(params string[] tenantIds)
    {
        foreach (var tenantId in tenantIds)
        {
            AddTenant(tenantId);
        }
    }

    public MutableTenantSource Tenants { get; } = new();

    public string Exchange => $"relaybox_{_id}";

    public string Queue => $"queue_{_id}";

    public string DatabaseOf(string tenantId) => $"t_{_id}_{tenantId}";

    public void AddTenant(string tenantId, string? databaseName = null) =>
        Tenants.Add(new TenantInfo(tenantId, databaseName ?? DatabaseOf(tenantId)));
}

public sealed class MutableTenantSource : ITenantSource
{
    private readonly object _gate = new();
    private List<TenantInfo> _tenants = [];

    public void Add(TenantInfo tenant)
    {
        lock (_gate)
        {
            _tenants = [.. _tenants, tenant];
        }
    }

    public Task<IReadOnlyList<TenantInfo>> LoadActiveTenantsAsync(CancellationToken ct)
    {
        lock (_gate)
        {
            return Task.FromResult<IReadOnlyList<TenantInfo>>(_tenants);
        }
    }
}

/// <summary>Contract used by the tests.</summary>
public sealed record ImportCompleted(string ImportId, int Rows);
