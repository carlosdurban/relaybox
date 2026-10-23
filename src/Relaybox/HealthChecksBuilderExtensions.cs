using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Relaybox.Relay;

namespace Relaybox;

public static class HealthChecksBuilderExtensions
{
    /// <summary>
    /// Reports whether the outbox is being published in time. Requires <see cref="RelayboxBuilder.AddOutboxRelay"/>.
    /// Use it for readiness and alerting, not for liveness.
    /// </summary>
    public static IHealthChecksBuilder AddRelayboxOutbox(
        this IHealthChecksBuilder builder,
        Action<OutboxHealthCheckOptions>? configure = null,
        string name = "relaybox-outbox",
        IEnumerable<string>? tags = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var options = builder.Services.AddOptions<OutboxHealthCheckOptions>();
        if (configure is not null)
        {
            options.Configure(configure);
        }

        return builder.AddCheck<OutboxHealthCheck>(name, HealthStatus.Unhealthy, tags ?? []);
    }
}
