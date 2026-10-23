using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

namespace Relaybox.Relay;

/// <summary>Thresholds of the outbox health check.</summary>
public sealed class OutboxHealthCheckOptions
{
    /// <summary>
    /// Unhealthy when a tenant's oldest pending message is older than this: events are not reaching
    /// consumers. Default: 5 minutes.
    /// </summary>
    public TimeSpan MaxPendingAge { get; set; } = TimeSpan.FromMinutes(5);
}

/// <summary>
/// <list type="bullet">
/// <item>Unhealthy: some tenant has a pending message older than <see cref="OutboxHealthCheckOptions.MaxPendingAge"/>.</item>
/// <item>Degraded: a signal source is disconnected (reconciliation still publishes, later), some tenant's
/// outbox cannot be read, or the statistics are stale.</item>
/// </list>
/// Meant for readiness and alerting, not liveness: restarting the instance does not fix a broker outage.
/// </summary>
internal sealed class OutboxHealthCheck(
    OutboxStatisticsSampler sampler,
    IEnumerable<IOutboxSignalSource> signalSources,
    IOptions<OutboxHealthCheckOptions> options,
    IOptions<OutboxRelayOptions> relayOptions,
    TimeProvider time) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default) =>
        Task.FromResult(Check());

    private HealthCheckResult Check()
    {
        if (sampler.Latest is not { } snapshot)
        {
            return HealthCheckResult.Healthy("The outbox has not been sampled yet.");
        }

        var now = time.GetUtcNow();
        var data = new Dictionary<string, object>
        {
            ["pending"] = snapshot.Tenants.Values.Sum(s => s.Pending),
            ["failed"] = snapshot.Tenants.Values.Sum(s => s.Failed),
        };

        var maxPendingAge = options.Value.MaxPendingAge;
        var late = snapshot.Tenants
            .Where(t => t.Value.OldestPendingAt is { } oldest && now.UtcDateTime - oldest > maxPendingAge)
            .Select(t => t.Key)
            .ToList();
        if (late.Count > 0)
        {
            data["late_tenants"] = late;
            return HealthCheckResult.Unhealthy(
                $"{late.Count} tenant(s) have messages pending for more than {maxPendingAge}: {string.Join(", ", late)}.",
                data: data);
        }

        var problems = new List<string>();
        if (now - snapshot.SampledAt > 3 * relayOptions.Value.StatisticsInterval)
        {
            problems.Add($"statistics are stale (last sample at {snapshot.SampledAt:o})");
        }

        if (snapshot.UnavailableTenants.Count > 0)
        {
            data["unavailable_tenants"] = snapshot.UnavailableTenants;
            problems.Add($"the outbox of {string.Join(", ", snapshot.UnavailableTenants)} cannot be read");
        }

        var disconnected = signalSources.OfType<IReportsSignalHealth>().Where(s => !s.IsConnected).Select(s => s.Name).ToList();
        if (disconnected.Count > 0)
        {
            problems.Add($"{string.Join(", ", disconnected)} disconnected; publishing relies on reconciliation");
        }

        return problems.Count > 0
            ? HealthCheckResult.Degraded(string.Join("; ", problems) + ".", data: data)
            : HealthCheckResult.Healthy(data: data);
    }
}
