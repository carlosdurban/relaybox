using System.Runtime.CompilerServices;
using Microsoft.Extensions.Options;

namespace Relaybox.Relay;

/// <summary>
/// Reconciliation: signals every active tenant on start-up and then every
/// <see cref="OutboxRelayOptions.ReconciliationInterval"/>. Always enabled; on its own it is a plain
/// polling publisher, next to a change stream it is the safety net.
/// </summary>
public sealed class PollingSignalSource(
    ITenantRegistry registry,
    IOptions<OutboxRelayOptions> options,
    TimeProvider time) : IOutboxSignalSource
{
    public async IAsyncEnumerable<string> WatchTenantsAsync([EnumeratorCancellation] CancellationToken ct)
    {
        using var timer = new PeriodicTimer(options.Value.ReconciliationInterval, time);
        do
        {
            foreach (var tenant in await registry.GetActiveTenantsAsync(ct))
            {
                yield return tenant.TenantId;
            }
        }
        while (await timer.WaitForNextTickAsync(ct));
    }
}
