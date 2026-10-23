using Relaybox.Relay;

namespace Relaybox.Tests;

public sealed class TenantWorkQueueTests
{
    private readonly TenantWorkQueue _queue = new();

    [Fact]
    public async Task Repeated_signals_queue_a_tenant_once()
    {
        _queue.Signal("acme");
        _queue.Signal("acme");
        _queue.Signal("globex");

        Assert.Equal("acme", await DequeueAsync());
        Assert.Equal("globex", await DequeueAsync());
        await AssertEmptyAsync();
    }

    [Fact]
    public async Task A_signal_while_running_queues_the_tenant_again_when_it_completes()
    {
        _queue.Signal("acme");
        var tenant = await DequeueAsync();

        _queue.Signal("acme");
        await AssertEmptyAsync();

        _queue.Complete(tenant, hasMoreWork: false);
        Assert.Equal("acme", await DequeueAsync());
    }

    [Fact]
    public async Task A_tenant_with_more_work_is_queued_again_behind_the_others()
    {
        _queue.Signal("acme");
        _queue.Signal("globex");
        var tenant = await DequeueAsync();

        _queue.Complete(tenant, hasMoreWork: true);

        Assert.Equal("globex", await DequeueAsync());
        Assert.Equal("acme", await DequeueAsync());
    }

    [Fact]
    public async Task A_completed_tenant_without_signals_leaves_the_queue()
    {
        _queue.Signal("acme");
        _queue.Complete(await DequeueAsync(), hasMoreWork: false);

        await AssertEmptyAsync();
        _queue.Signal("acme");
        Assert.Equal("acme", await DequeueAsync());
    }

    private async Task<string> DequeueAsync()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        return await _queue.DequeueAsync(timeout.Token);
    }

    private async Task AssertEmptyAsync()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await _queue.DequeueAsync(timeout.Token));
    }
}
