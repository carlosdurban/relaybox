using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Relaybox.Tests;

/// <summary>In-memory outbox with the same claim semantics as the real stores.</summary>
internal sealed class InMemoryOutboxStore(TimeProvider time) : IOutboxStore
{
    private readonly object _gate = new();
    private readonly Dictionary<string, List<Entry>> _outboxes = new();

    public HashSet<string> BrokenTenants { get; } = [];

    public List<(Guid Id, DateTime RetryAt, string Error)> Failures { get; } = [];

    public OutboxMessage Add(string tenantId, string type = "TestEvent", string? ownerTenantId = null)
    {
        lock (_gate)
        {
            var message = new OutboxMessage(Guid.NewGuid(), ownerTenantId ?? tenantId, type, "{}",
                time.GetUtcNow().UtcDateTime, 0, null);
            if (!_outboxes.TryGetValue(tenantId, out var entries))
            {
                _outboxes[tenantId] = entries = [];
            }

            entries.Add(new Entry(message));
            return message;
        }
    }

    public IReadOnlyList<OutboxMessage> Pending(string tenantId)
    {
        lock (_gate)
        {
            return _outboxes.GetValueOrDefault(tenantId)?.Where(e => e.ProcessedAt is null).Select(e => e.Message).ToList()
                ?? [];
        }
    }

    public Task<OutboxMessage?> ClaimNextAsync(string tenantId, string lockOwner, TimeSpan lockDuration, CancellationToken ct)
    {
        ThrowIfBroken(tenantId);
        lock (_gate)
        {
            var now = time.GetUtcNow().UtcDateTime;
            var entry = _outboxes.GetValueOrDefault(tenantId)?
                .Where(e => e.ProcessedAt is null && (e.LockedUntil is null || e.LockedUntil < now))
                .OrderBy(e => e.Message.OccurredAt)
                .FirstOrDefault();
            if (entry is null)
            {
                return Task.FromResult<OutboxMessage?>(null);
            }

            entry.LockedUntil = now + lockDuration;
            entry.Message = entry.Message with { Attempts = entry.Message.Attempts + 1 };
            return Task.FromResult<OutboxMessage?>(entry.Message);
        }
    }

    public Task MarkProcessedAsync(string tenantId, Guid messageId, CancellationToken ct)
    {
        lock (_gate)
        {
            Find(tenantId, messageId).ProcessedAt = time.GetUtcNow().UtcDateTime;
        }

        return Task.CompletedTask;
    }

    public Task MarkFailedAsync(string tenantId, Guid messageId, string error, DateTime retryAt, CancellationToken ct)
    {
        lock (_gate)
        {
            Find(tenantId, messageId).LockedUntil = retryAt;
            Failures.Add((messageId, retryAt, error));
        }

        return Task.CompletedTask;
    }

    private Entry Find(string tenantId, Guid messageId) => _outboxes[tenantId].Single(e => e.Message.Id == messageId);

    private void ThrowIfBroken(string tenantId)
    {
        if (BrokenTenants.Contains(tenantId))
        {
            throw new InvalidOperationException($"Database of tenant {tenantId} is down.");
        }
    }

    private sealed class Entry(OutboxMessage message)
    {
        public OutboxMessage Message { get; set; } = message;
        public DateTime? LockedUntil { get; set; }
        public DateTime? ProcessedAt { get; set; }
    }
}

/// <summary>Records published messages together with the tenant of the scope that published them.</summary>
internal sealed class RecordingPublisher
{
    public ConcurrentQueue<(OutboxMessage Message, string ScopeTenant)> Published { get; } = new();

    public bool Down { get; set; }
}

internal sealed class ScopedRecordingPublisher(RecordingPublisher recorder, ITenantContext tenantContext) : IMessagePublisher
{
    public Task PublishAsync(OutboxMessage message, CancellationToken ct)
    {
        if (recorder.Down)
        {
            throw new IOException("Broker unreachable");
        }

        recorder.Published.Enqueue((message, tenantContext.TenantId));
        return Task.CompletedTask;
    }
}

/// <summary>Signal source driven by the test.</summary>
internal sealed class ManualSignalSource : IOutboxSignalSource
{
    private readonly Channel<string> _signals = Channel.CreateUnbounded<string>();

    public void Signal(params string[] tenantIds)
    {
        foreach (var tenantId in tenantIds)
        {
            _signals.Writer.TryWrite(tenantId);
        }
    }

    public async IAsyncEnumerable<string> WatchTenantsAsync([EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var tenantId in _signals.Reader.ReadAllAsync(ct))
        {
            yield return tenantId;
        }
    }
}

/// <summary>In-memory inbox: records ids per tenant only when the handler succeeds.</summary>
internal sealed class InMemoryInbox
{
    private readonly ConcurrentDictionary<(string Tenant, string Id), byte> _processed = new();

    public int Count => _processed.Count;

    public async Task<InboxResult> ExecuteOnceAsync(MessageContext context, Func<CancellationToken, Task> handler, CancellationToken ct)
    {
        if (_processed.ContainsKey((context.TenantId, context.MessageId)))
        {
            return InboxResult.Duplicate;
        }

        await handler(ct);
        _processed[(context.TenantId, context.MessageId)] = 0;
        return InboxResult.Processed;
    }
}

internal sealed class InMemoryInboxAdapter(InMemoryInbox inbox) : IInbox
{
    public Task<InboxResult> ExecuteOnceAsync(MessageContext context, Func<CancellationToken, Task> handler, CancellationToken ct) =>
        inbox.ExecuteOnceAsync(context, handler, ct);
}

internal static class Eventually
{
    public static async Task TrueAsync(Func<bool> condition, TimeSpan? timeout = null)
    {
        using var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(10));
        while (!condition())
        {
            await Task.Delay(20, cts.Token);
        }
    }
}
