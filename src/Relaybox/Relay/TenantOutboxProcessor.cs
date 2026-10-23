using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Relaybox.Diagnostics;

namespace Relaybox.Relay;

/// <summary>
/// Publishes the pending outbox messages of one tenant: claim, publish (confirmed), mark processed.
/// </summary>
internal sealed class TenantOutboxProcessor(
    IServiceScopeFactory scopeFactory,
    IOptions<OutboxRelayOptions> options,
    TimeProvider time,
    ILogger<TenantOutboxProcessor> logger)
{
    private static readonly TimeSpan MarkProcessedTimeout = TimeSpan.FromSeconds(10);

    /// <summary>Unique per process, so a lock can always be traced back to the instance that took it.</summary>
    internal string LockOwner { get; } = $"{Environment.MachineName}:{Environment.ProcessId}:{Guid.NewGuid():N}";

    /// <summary>
    /// Publishes up to <see cref="OutboxRelayOptions.MaxMessagesPerTenantPerRound"/> messages (failed ones included).
    /// Returns <c>true</c> when the limit was reached and the tenant may have more.
    /// </summary>
    public async Task<bool> ProcessAsync(string tenantId, CancellationToken ct)
    {
        var settings = options.Value;

        await using var scope = scopeFactory.CreateAsyncScope();
        var services = scope.ServiceProvider;
        services.GetRequiredService<ITenantContext>().Set(tenantId);
        var store = services.GetRequiredService<IOutboxStore>();
        var publisher = services.GetRequiredService<IMessagePublisher>();

        for (var published = 0; published < settings.MaxMessagesPerTenantPerRound; published++)
        {
            var message = await store.ClaimNextAsync(tenantId, LockOwner, settings.LockDuration, ct);
            if (message is null)
            {
                return false;
            }

            if (message.TenantId != tenantId)
            {
                logger.LogCritical(
                    "Outbox message {MessageId} belongs to tenant {MessageTenantId} but was found in the outbox of tenant {TenantId}. " +
                    "Publication for this tenant is stopped until the data is fixed",
                    message.Id, message.TenantId, tenantId);
                throw new TenantMismatchException(tenantId, message);
            }

            if (!await TryPublishAsync(store, publisher, message, settings, ct))
            {
                return false;
            }
        }

        return true;
    }

    private async Task<bool> TryPublishAsync(
        IOutboxStore store, IMessagePublisher publisher, OutboxMessage message, OutboxRelayOptions settings, CancellationToken ct)
    {
        var tags = RelayboxDiagnostics.Tags(message.TenantId, message.Type);
        using var activity = RelayboxDiagnostics.StartActivity($"{message.Type} publish", ActivityKind.Producer, message.TraceParent);
        activity?.SetTag(RelayboxDiagnostics.TenantTag, message.TenantId);
        activity?.SetTag("messaging.message.id", message.Id.ToString());

        try
        {
            await publisher.PublishAsync(message, ct);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            RelayboxDiagnostics.PublishFailures.Add(1, tags);
            var error = $"{ex.GetType().Name}: {ex.Message}";

            if (ex is MessageRejectedException rejection
                && (rejection.IsPermanent || message.Attempts >= settings.MaxAttempts))
            {
                logger.LogError(ex,
                    "Outbox message {MessageId} ({MessageType}) of tenant {TenantId} was rejected by the broker after {Attempt} attempts and is marked as failed",
                    message.Id, message.Type, message.TenantId, message.Attempts);
                await RecordAsync(message, () => store.MarkFailedAsync(message.TenantId, message.Id, error, ct), ct);
                RelayboxDiagnostics.MessagesFailed.Add(1, tags);

                // Only this message is affected: carry on with the rest of the outbox.
                return true;
            }

            var retryAt = time.GetUtcNow().UtcDateTime + settings.RetryDelay(message.Attempts);
            logger.LogWarning(ex,
                "Failed to publish outbox message {MessageId} ({MessageType}) of tenant {TenantId}, attempt {Attempt}. Retrying at {RetryAt:o}",
                message.Id, message.Type, message.TenantId, message.Attempts, retryAt);
            await RecordAsync(message, () => store.ScheduleRetryAsync(message.TenantId, message.Id, error, retryAt, ct), ct);

            // The broker is probably unreachable: stop this round instead of failing every message.
            return false;
        }

        // The message is already on the broker: try hard to record it even during shutdown,
        // otherwise it will be published again (the consumer inbox would discard the duplicate).
        using var markTimeout = new CancellationTokenSource(MarkProcessedTimeout);
        await store.MarkProcessedAsync(message.TenantId, message.Id, markTimeout.Token);

        RelayboxDiagnostics.MessagesPublished.Add(1, tags);
        RelayboxDiagnostics.PublishAttempts.Record(message.Attempts, tags);
        RelayboxDiagnostics.PublishLatency.Record((time.GetUtcNow().UtcDateTime - message.OccurredAt).TotalSeconds, tags);
        logger.LogDebug("Published outbox message {MessageId} ({MessageType}) of tenant {TenantId}",
            message.Id, message.Type, message.TenantId);

        return true;
    }

    private async Task RecordAsync(OutboxMessage message, Func<Task> record, CancellationToken ct)
    {
        try
        {
            await record();
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            logger.LogError(ex,
                "Could not record the failure of outbox message {MessageId} of tenant {TenantId}; it will be retried when its lock expires",
                message.Id, message.TenantId);
        }
    }
}

/// <summary>An outbox contains a message that belongs to a different tenant.</summary>
public sealed class TenantMismatchException(string tenantId, OutboxMessage message)
    : Exception($"Outbox message {message.Id} belongs to tenant '{message.TenantId}' but was read from the outbox of tenant '{tenantId}'.");
