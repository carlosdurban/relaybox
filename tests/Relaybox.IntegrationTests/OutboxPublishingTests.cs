using MongoDB.Bson;
using MongoDB.Driver;
using Relaybox.IntegrationTests.Infrastructure;

namespace Relaybox.IntegrationTests;

[Collection(InfrastructureCollection.Name)]
public sealed class OutboxPublishingTests(InfrastructureFixture infrastructure)
{
    [Fact]
    public async Task Committed_events_are_published_with_their_outbox_id_and_tenant_header()
    {
        var scenario = new TestScenario("acme");
        await using var probe = await BrokerProbe.StartAsync(infrastructure, scenario);
        await using var app = await RelayboxApp.StartAsync(infrastructure, scenario);

        await app.CommitAsync("acme", new ImportCompleted("imp-1", 10));

        await Eventually.TrueAsync(() => probe.Received.Count == 1);
        var stored = await infrastructure.Outbox(scenario, "acme").Find(FilterDefinition<BsonDocument>.Empty).SingleAsync();
        var received = probe.Received.Single();
        Assert.Equal(stored["_id"].AsGuid.ToString(), received.MessageId);
        Assert.Equal("acme", received.TenantId);
        Assert.Equal("import.completed", received.Type);
        Assert.Contains("\"importId\":\"imp-1\"", received.Body);

        await Eventually.TrueAsync(async () =>
            (await infrastructure.Outbox(scenario, "acme").Find(FilterDefinition<BsonDocument>.Empty).SingleAsync())["processedAt"].IsValidDateTime);
    }

    [Fact]
    public async Task Events_of_an_aborted_transaction_are_never_published()
    {
        var scenario = new TestScenario("acme");
        await using var probe = await BrokerProbe.StartAsync(infrastructure, scenario);
        await using var app = await RelayboxApp.StartAsync(infrastructure, scenario);

        await app.AbortAsync("acme", new ImportCompleted("imp-1", 10));
        await app.CommitAsync("acme", new ImportCompleted("imp-2", 20));

        await Eventually.TrueAsync(() => probe.Received.Count == 1);
        await Task.Delay(TimeSpan.FromSeconds(3));
        Assert.Contains("imp-2", Assert.Single(probe.Received).Body);
        Assert.Equal(1, await infrastructure.Outbox(scenario, "acme").CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty));
    }

    [Fact]
    public async Task The_change_stream_publishes_without_waiting_for_the_reconciliation()
    {
        var scenario = new TestScenario("acme");
        await using var probe = await BrokerProbe.StartAsync(infrastructure, scenario);
        await using var app = await RelayboxApp.StartAsync(infrastructure, scenario,
            relay: o => o.ReconciliationInterval = TimeSpan.FromMinutes(10));
        await Task.Delay(TimeSpan.FromSeconds(2)); // let the start-up reconciliation and the stream settle

        await app.CommitAsync("acme", new ImportCompleted("imp-1", 10));

        await Eventually.TrueAsync(() => probe.Received.Count == 1, TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task Competing_replicas_publish_each_message_exactly_once()
    {
        var scenario = new TestScenario("acme", "globex");
        await using var probe = await BrokerProbe.StartAsync(infrastructure, scenario);
        await using var writer = await RelayboxApp.StartAsync(infrastructure, scenario, withRelay: false);
        for (var i = 0; i < 100; i++)
        {
            await writer.CommitAsync("acme", new ImportCompleted($"acme-{i}", i));
            await writer.CommitAsync("globex", new ImportCompleted($"globex-{i}", i));
        }

        var replicas = await Task.WhenAll(Enumerable.Range(0, 3).Select(_ => RelayboxApp.StartAsync(infrastructure, scenario)));
        try
        {
            await Eventually.TrueAsync(() => probe.Received.Count >= 200, TimeSpan.FromSeconds(60));
            await Task.Delay(TimeSpan.FromSeconds(3));

            Assert.Equal(200, probe.Received.Count);
            Assert.Equal(200, probe.Received.Select(m => m.MessageId).Distinct().Count());
        }
        finally
        {
            foreach (var replica in replicas)
            {
                await replica.DisposeAsync();
            }
        }
    }

    [Fact]
    public async Task A_message_locked_by_a_dead_instance_is_published_when_the_lock_expires()
    {
        var scenario = new TestScenario("acme");
        await using var probe = await BrokerProbe.StartAsync(infrastructure, scenario);
        await using (var writer = await RelayboxApp.StartAsync(infrastructure, scenario, withRelay: false))
        {
            await writer.CommitAsync("acme", new ImportCompleted("imp-1", 10));
        }

        var lockedUntil = DateTime.UtcNow.AddSeconds(4);
        await infrastructure.Outbox(scenario, "acme").UpdateOneAsync(
            FilterDefinition<BsonDocument>.Empty,
            Builders<BsonDocument>.Update.Set("lockedUntil", lockedUntil).Set("lockedBy", "dead-pod").Set("attempts", 1));

        await using var app = await RelayboxApp.StartAsync(infrastructure, scenario);

        await Task.Delay(TimeSpan.FromSeconds(2));
        Assert.Empty(probe.Received);
        await Eventually.TrueAsync(() => probe.Received.Count == 1);
        Assert.True(DateTime.UtcNow >= lockedUntil);
    }

    [Fact]
    public async Task Messages_wait_while_the_broker_is_down_and_are_published_when_it_returns()
    {
        var scenario = new TestScenario("acme");
        await using var probe = await BrokerProbe.StartAsync(infrastructure, scenario);
        await using var app = await RelayboxApp.StartAsync(infrastructure, scenario);

        await infrastructure.Rabbit.PauseAsync();
        try
        {
            await app.CommitAsync("acme", new ImportCompleted("imp-1", 10));

            await Eventually.TrueAsync(async () =>
            {
                var stored = await infrastructure.Outbox(scenario, "acme").Find(FilterDefinition<BsonDocument>.Empty).SingleAsync();
                return stored["error"].IsString && stored["processedAt"].IsBsonNull;
            });
        }
        finally
        {
            await infrastructure.Rabbit.UnpauseAsync();
        }

        await Eventually.TrueAsync(() => probe.Received.Count == 1, TimeSpan.FromSeconds(60));
    }

    [Fact]
    public async Task A_tenant_created_after_start_up_is_published()
    {
        var scenario = new TestScenario("acme");
        await using var probe = await BrokerProbe.StartAsync(infrastructure, scenario);
        await using var app = await RelayboxApp.StartAsync(infrastructure, scenario,
            relay: o => o.ReconciliationInterval = TimeSpan.FromMinutes(10));
        await Task.Delay(TimeSpan.FromSeconds(1));

        scenario.AddTenant("newco");
        await Task.Delay(TimeSpan.FromSeconds(1)); // past the tenant miss cooldown
        await app.CommitAsync("newco", new ImportCompleted("imp-1", 10));

        await Eventually.TrueAsync(() => probe.Received.Any(m => m.TenantId == "newco"));
    }

    [Fact]
    public async Task A_tenant_with_a_broken_database_does_not_block_the_others()
    {
        var scenario = new TestScenario("acme");
        scenario.AddTenant("broken", databaseName: "invalid/name");
        await using var probe = await BrokerProbe.StartAsync(infrastructure, scenario);
        await using var app = await RelayboxApp.StartAsync(infrastructure, scenario);

        await app.CommitAsync("acme", new ImportCompleted("imp-1", 10));

        await Eventually.TrueAsync(() => probe.Received.Count == 1);
        await Task.Delay(TimeSpan.FromSeconds(3)); // a couple of reconciliation rounds hitting the broken tenant
        await app.CommitAsync("acme", new ImportCompleted("imp-2", 20));
        await Eventually.TrueAsync(() => probe.Received.Count == 2);
    }
}
