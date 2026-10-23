using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Relaybox.Diagnostics;

/// <summary>
/// OpenTelemetry instrumentation. Subscribe with
/// <c>.AddSource(RelayboxDiagnostics.Name)</c> and <c>.AddMeter(RelayboxDiagnostics.Name)</c>.
/// </summary>
public static class RelayboxDiagnostics
{
    public const string Name = "Relaybox";

    public const string TenantTag = "relaybox.tenant_id";
    public const string MessageTypeTag = "messaging.message.type";
    public const string OutcomeTag = "relaybox.outcome";

    private static readonly string? Version = typeof(RelayboxDiagnostics).Assembly.GetName().Version?.ToString();

    internal static readonly ActivitySource ActivitySource = new(Name, Version);
    internal static readonly Meter Meter = new(Name, Version);

    internal static readonly Counter<long> MessagesPublished =
        Meter.CreateCounter<long>("relaybox.outbox.published", "{message}", "Outbox messages confirmed by the broker.");

    internal static readonly Counter<long> PublishFailures =
        Meter.CreateCounter<long>("relaybox.outbox.publish_failures", "{message}", "Failed publication attempts.");

    internal static readonly Counter<long> MessagesFailed =
        Meter.CreateCounter<long>("relaybox.outbox.marked_failed", "{message}",
            "Messages given up on because the broker keeps rejecting them.");

    internal static readonly Histogram<double> PublishLatency =
        Meter.CreateHistogram<double>("relaybox.outbox.publish_latency", "s",
            "Time from the event being added to the outbox to its broker confirmation.");

    internal static readonly Histogram<int> PublishAttempts =
        Meter.CreateHistogram<int>("relaybox.outbox.attempts", "{attempt}",
            "Attempts needed to publish a message.");

    internal static readonly Counter<long> TenantFailures =
        Meter.CreateCounter<long>("relaybox.relay.tenant_failures", "{failure}",
            "Relay rounds aborted because of an error in a tenant.");

    internal static readonly Counter<long> MessagesConsumed =
        Meter.CreateCounter<long>("relaybox.inbox.consumed", "{message}",
            "Inbound messages by outcome (processed, duplicate, dead_lettered, requeued).");

    internal static TagList Tags(string tenantId, string messageType) =>
        new() { { TenantTag, tenantId }, { MessageTypeTag, messageType } };

    internal static Activity? StartActivity(string name, ActivityKind kind, string? traceParent)
    {
        ActivityContext.TryParse(traceParent, null, out var parent);
        return ActivitySource.StartActivity(name, kind, parent);
    }
}
