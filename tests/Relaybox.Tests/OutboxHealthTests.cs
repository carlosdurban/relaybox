using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Relaybox.Relay;

namespace Relaybox.Tests;

public sealed class OutboxHealthTests
{
    private readonly FakeTimeProvider _time = new(new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero));
    private readonly InMemoryOutboxStore _store;
    private readonly FakeSignalHealth _changeStream = new();
    private readonly ServiceProvider _services;

    public OutboxHealthTests()
    {
        _store = new InMemoryOutboxStore(_time);
        var services = new ServiceCollection()
            .AddLogging()
            .AddSingleton<TimeProvider>(_time)
            .AddSingleton<IOutboxStore>(_store)
            .AddSingleton<IOutboxSignalSource>(_changeStream)
            .AddSingleton(Options.Create(new OutboxRelayOptions()))
            .AddSingleton<OutboxStatisticsSampler>();
        services.AddRelaybox().AddTenants(new TenantInfo("acme", "acme_db"), new TenantInfo("globex", "globex_db"));
        services.AddHealthChecks().AddRelayboxOutbox(o => o.MaxPendingAge = TimeSpan.FromMinutes(5));
        _services = services.BuildServiceProvider();
    }

    [Fact]
    public async Task Samples_pending_and_failed_messages_per_tenant()
    {
        _store.Add("acme");
        _time.Advance(TimeSpan.FromMinutes(1));
        _store.Add("acme");
        var failed = _store.Add("globex");
        await _store.MarkFailedAsync("globex", failed.Id, "rejected", default);

        await SampleAsync();

        var snapshot = Sampler.Latest!;
        Assert.Equal(new OutboxStatistics(2, 0, _time.GetUtcNow().UtcDateTime - TimeSpan.FromMinutes(1)), snapshot.Tenants["acme"]);
        Assert.Equal(new OutboxStatistics(0, 1, null), snapshot.Tenants["globex"]);
    }

    [Fact]
    public async Task Is_healthy_when_messages_flow()
    {
        _store.Add("acme");
        await SampleAsync();

        Assert.Equal(HealthStatus.Healthy, (await CheckAsync()).Status);
    }

    [Fact]
    public async Task Is_unhealthy_when_a_message_waits_longer_than_the_threshold()
    {
        _store.Add("acme");
        _time.Advance(TimeSpan.FromMinutes(6));
        await SampleAsync();

        var report = await CheckAsync();

        Assert.Equal(HealthStatus.Unhealthy, report.Status);
        Assert.Contains("acme", report.Description);
    }

    [Fact]
    public async Task Is_degraded_while_the_change_stream_is_down()
    {
        _changeStream.IsConnected = false;
        await SampleAsync();

        Assert.Equal(HealthStatus.Degraded, (await CheckAsync()).Status);
    }

    [Fact]
    public async Task Is_degraded_when_a_tenant_outbox_cannot_be_read()
    {
        _store.BrokenTenants.Add("globex");
        await SampleAsync();

        var report = await CheckAsync();

        Assert.Equal(HealthStatus.Degraded, report.Status);
        Assert.Contains("globex", report.Description);
    }

    [Fact]
    public async Task Is_degraded_when_the_statistics_are_stale()
    {
        await SampleAsync();
        _time.Advance(TimeSpan.FromMinutes(5));

        Assert.Equal(HealthStatus.Degraded, (await CheckAsync()).Status);
    }

    private OutboxStatisticsSampler Sampler => _services.GetRequiredService<OutboxStatisticsSampler>();

    private Task SampleAsync() => Sampler.SampleAsync(CancellationToken.None);

    private async Task<HealthReportEntry> CheckAsync()
    {
        var report = await _services.GetRequiredService<HealthCheckService>().CheckHealthAsync();
        return report.Entries["relaybox-outbox"];
    }

    private sealed class FakeSignalHealth : IOutboxSignalSource, IReportsSignalHealth
    {
        public string Name => "change stream";

        public bool IsConnected { get; set; } = true;

        public async IAsyncEnumerable<string> WatchTenantsAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
        {
            await Task.CompletedTask;
            yield break;
        }
    }
}
