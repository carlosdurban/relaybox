using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Relaybox.Tests;

public sealed class OutboxRelayServiceTests : IAsyncLifetime
{
    private readonly InMemoryOutboxStore _store = new(TimeProvider.System);
    private readonly RecordingPublisher _publisher = new();
    private readonly ManualSignalSource _signals = new();
    private IHost _host = null!;

    public async Task InitializeAsync()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services
            .AddSingleton<IOutboxStore>(_store)
            .AddSingleton(_publisher)
            .AddScoped<IMessagePublisher, ScopedRecordingPublisher>()
            .AddSingleton<IOutboxSignalSource>(_signals);
        builder.Services.AddRelaybox()
            .AddTenants(new TenantInfo("acme", "acme_db"), new TenantInfo("globex", "globex_db"), new TenantInfo("broken", "broken_db"))
            .AddOutboxRelay(o =>
            {
                o.ReconciliationInterval = TimeSpan.FromHours(1);
                o.RetryBaseDelay = TimeSpan.FromMilliseconds(200);
            });

        _host = builder.Build();
        await _host.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _host.StopAsync();
        _host.Dispose();
    }

    [Fact]
    public async Task Publishes_the_outbox_of_every_signaled_tenant()
    {
        _store.Add("acme");
        _store.Add("globex");

        _signals.Signal("acme", "globex");

        await Eventually.TrueAsync(() => _publisher.Published.Count == 2);
        Assert.Empty(_store.Pending("acme"));
        Assert.Empty(_store.Pending("globex"));
    }

    [Fact]
    public async Task A_broken_tenant_does_not_stop_the_others()
    {
        _store.BrokenTenants.Add("broken");
        _store.Add("broken");
        _store.Add("acme");

        _signals.Signal("broken", "acme");

        await Eventually.TrueAsync(() => _publisher.Published.Count == 1);
        Assert.Equal("acme", _publisher.Published.Single().Message.TenantId);

        _store.Add("acme");
        _signals.Signal("broken", "acme");
        await Eventually.TrueAsync(() => _publisher.Published.Count == 2);
    }

    [Fact]
    public async Task Messages_left_pending_while_the_broker_is_down_are_published_on_a_later_signal()
    {
        _publisher.Down = true;
        _store.Add("acme");
        _signals.Signal("acme");
        await Eventually.TrueAsync(() => _store.Retries.Count == 1);

        _publisher.Down = false;
        await Task.Delay(TimeSpan.FromMilliseconds(250));
        _signals.Signal("acme");

        await Eventually.TrueAsync(() => _publisher.Published.Count == 1);
    }

    [Fact]
    public async Task The_reconciliation_signals_every_active_tenant_on_start_up()
    {
        await _host.StopAsync();
        _store.Add("globex");

        await using var scope = _host.Services.CreateAsyncScope();
        var polling = scope.ServiceProvider.GetServices<IOutboxSignalSource>().OfType<Relay.PollingSignalSource>().Single();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var tenants = new List<string>();
        await foreach (var tenant in polling.WatchTenantsAsync(cts.Token))
        {
            tenants.Add(tenant);
            if (tenants.Count == 3)
            {
                break;
            }
        }

        Assert.Equal(["acme", "globex", "broken"], tenants);
    }
}
