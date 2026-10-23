using System.Threading.Channels;

namespace Relaybox.Relay;

/// <summary>
/// Queue of tenants with pending work. A tenant is never queued twice nor processed by two workers at once:
/// signals received while it is being processed are remembered and the tenant is queued again when it finishes.
/// </summary>
internal sealed class TenantWorkQueue
{
    private readonly Channel<string> _channel = Channel.CreateUnbounded<string>();
    private readonly Dictionary<string, State> _states = new(StringComparer.Ordinal);
    private readonly object _gate = new();

    private enum State
    {
        Queued,
        Running,
        RunningAndSignaled,
    }

    public void Signal(string tenantId)
    {
        lock (_gate)
        {
            if (!_states.TryGetValue(tenantId, out var state))
            {
                _states[tenantId] = State.Queued;
                _channel.Writer.TryWrite(tenantId);
            }
            else if (state == State.Running)
            {
                _states[tenantId] = State.RunningAndSignaled;
            }
        }
    }

    public async ValueTask<string> DequeueAsync(CancellationToken ct)
    {
        var tenantId = await _channel.Reader.ReadAsync(ct);
        lock (_gate)
        {
            _states[tenantId] = State.Running;
        }

        return tenantId;
    }

    public void Complete(string tenantId, bool hasMoreWork)
    {
        lock (_gate)
        {
            if (hasMoreWork || _states.GetValueOrDefault(tenantId) == State.RunningAndSignaled)
            {
                _states[tenantId] = State.Queued;
                _channel.Writer.TryWrite(tenantId);
            }
            else
            {
                _states.Remove(tenantId);
            }
        }
    }
}
