using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Relaybox.Diagnostics;

namespace Relaybox.Relay;

/// <summary>
/// Counts pending and failed messages of every tenant periodically and exposes them as gauges.
/// Sampling is decoupled from scraping, so a metrics scrape never queries the databases.
/// </summary>
internal sealed class OutboxStatisticsSampler : BackgroundService
{
    private readonly ITenantRegistry _registry;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IOptions<OutboxRelayOptions> _options;
    private readonly TimeProvider _time;
    private readonly ILogger<OutboxStatisticsSampler> _logger;
    private volatile OutboxStatisticsSnapshot? _latest;

    public OutboxStatisticsSampler(
        ITenantRegistry registry,
        IServiceScopeFactory scopeFactory,
        IOptions<OutboxRelayOptions> options,
        TimeProvider time,
        ILogger<OutboxStatisticsSampler> logger)
    {
        _registry = registry;
        _scopeFactory = scopeFactory;
        _options = options;
        _time = time;
        _logger = logger;

        RelayboxDiagnostics.Meter.CreateObservableGauge("relaybox.outbox.pending",
            () => Measure(s => s.Pending), "{message}", "Messages waiting to be published.");
        RelayboxDiagnostics.Meter.CreateObservableGauge("relaybox.outbox.failed",
            () => Measure(s => s.Failed), "{message}", "Messages given up on, waiting for someone to act.");
        RelayboxDiagnostics.Meter.CreateObservableGauge("relaybox.outbox.oldest_pending_age",
            MeasureOldestPendingAge, "s", "Age of the oldest pending message. The key metric to alert on.");
    }

    /// <summary>The last completed sample, or <c>null</c> before the first one.</summary>
    public OutboxStatisticsSnapshot? Latest => _latest;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(_options.Value.StatisticsInterval, _time);
        try
        {
            do
            {
                await SampleAsync(stoppingToken);
            }
            while (await timer.WaitForNextTickAsync(stoppingToken));
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }

    internal async Task SampleAsync(CancellationToken ct)
    {
        IReadOnlyList<TenantInfo> tenants;
        try
        {
            tenants = await _registry.GetActiveTenantsAsync(ct);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            _logger.LogWarning(ex, "Could not load the tenants to sample outbox statistics");
            return;
        }

        var statistics = new Dictionary<string, OutboxStatistics>(StringComparer.Ordinal);
        var unavailable = new List<string>();
        foreach (var tenant in tenants)
        {
            try
            {
                await using var scope = _scopeFactory.CreateAsyncScope();
                scope.ServiceProvider.GetRequiredService<ITenantContext>().Set(tenant.TenantId);
                var store = scope.ServiceProvider.GetRequiredService<IOutboxStore>();
                statistics[tenant.TenantId] = await store.GetStatisticsAsync(tenant.TenantId, ct);
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                unavailable.Add(tenant.TenantId);
                _logger.LogWarning(ex, "Could not sample the outbox statistics of tenant {TenantId}", tenant.TenantId);
            }
        }

        _latest = new OutboxStatisticsSnapshot(_time.GetUtcNow(), statistics, unavailable);
    }

    private IEnumerable<Measurement<long>> Measure(Func<OutboxStatistics, long> value) =>
        _latest?.Tenants.Select(t => new Measurement<long>(value(t.Value), Tenant(t.Key))) ?? [];

    private IEnumerable<Measurement<double>> MeasureOldestPendingAge()
    {
        if (_latest is not { } snapshot)
        {
            return [];
        }

        var now = _time.GetUtcNow().UtcDateTime;
        return snapshot.Tenants.Select(t => new Measurement<double>(
            t.Value.OldestPendingAt is { } oldest ? Math.Max(0, (now - oldest).TotalSeconds) : 0,
            Tenant(t.Key)));
    }

    private static KeyValuePair<string, object?> Tenant(string tenantId) => new(RelayboxDiagnostics.TenantTag, tenantId);
}

/// <param name="SampledAt">When the sample was taken.</param>
/// <param name="Tenants">Statistics of each tenant that could be sampled.</param>
/// <param name="UnavailableTenants">Tenants whose outbox could not be read.</param>
internal sealed record OutboxStatisticsSnapshot(
    DateTimeOffset SampledAt,
    IReadOnlyDictionary<string, OutboxStatistics> Tenants,
    IReadOnlyList<string> UnavailableTenants);
