using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Relaybox.Diagnostics;

namespace Relaybox.Relay;

/// <summary>
/// Background publisher. Every <see cref="IOutboxSignalSource"/> feeds a shared queue of tenants;
/// a bounded set of workers drains it, one tenant at a time each. A failing tenant is logged and
/// left for the next signal without affecting the others.
/// </summary>
internal sealed class OutboxRelayService(
    IEnumerable<IOutboxSignalSource> signalSources,
    TenantWorkQueue queue,
    TenantOutboxProcessor processor,
    IOptions<OutboxRelayOptions> options,
    TimeProvider time,
    ILogger<OutboxRelayService> logger) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var settings = options.Value;
        logger.LogInformation("Outbox relay {LockOwner} started with {Workers} workers", processor.LockOwner,
            settings.MaxDegreeOfParallelism);

        var pumps = signalSources.Select(source => PumpSignalsAsync(source, settings, stoppingToken));
        var workers = Enumerable.Range(0, settings.MaxDegreeOfParallelism).Select(_ => WorkAsync(stoppingToken));

        return Task.WhenAll(pumps.Concat(workers));
    }

    private async Task PumpSignalsAsync(IOutboxSignalSource source, OutboxRelayOptions settings, CancellationToken ct)
    {
        await Task.Yield();
        var sourceName = source.GetType().Name;

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await foreach (var tenantId in source.WatchTenantsAsync(ct))
                {
                    queue.Signal(tenantId);
                }

                logger.LogWarning("Signal source {SignalSource} ended; restarting", sourceName);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Signal source {SignalSource} failed; restarting in {Delay}", sourceName,
                    settings.SignalSourceRestartDelay);
            }

            try
            {
                await Task.Delay(settings.SignalSourceRestartDelay, time, ct);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    private async Task WorkAsync(CancellationToken ct)
    {
        await Task.Yield();

        while (true)
        {
            string tenantId;
            try
            {
                tenantId = await queue.DequeueAsync(ct);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            var hasMoreWork = false;
            try
            {
                hasMoreWork = await processor.ProcessAsync(tenantId, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                RelayboxDiagnostics.TenantFailures.Add(1, new KeyValuePair<string, object?>(RelayboxDiagnostics.TenantTag, tenantId));
                logger.LogError(ex, "Outbox relay round failed for tenant {TenantId}", tenantId);
            }
            finally
            {
                queue.Complete(tenantId, hasMoreWork);
            }
        }
    }
}
