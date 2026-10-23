using Microsoft.Extensions.DependencyInjection;
using MongoDB.Bson;
using MongoDB.Driver;
using Relaybox.IntegrationTests.Infrastructure;
using Relaybox.MongoDb;

namespace Relaybox.IntegrationTests;

[Collection(InfrastructureCollection.Name)]
public sealed class OutboxRoutingAndFailureTests(InfrastructureFixture infrastructure)
{
    [Fact]
    public async Task A_message_is_published_to_its_own_destination_with_its_routing_key_and_headers()
    {
        var scenario = new TestScenario("acme");
        var billing = $"{scenario.Exchange}.billing";
        await using var probe = await BrokerProbe.StartAsync(infrastructure, scenario, exchange: billing);
        await using var app = await RelayboxApp.StartAsync(infrastructure, scenario);

        await app.CommitAsync("acme", new ImportCompleted("imp-1", 10), new OutboxMessageOptions
        {
            Destination = billing,
            RoutingKey = "imports.acme.completed",
            Headers = new Dictionary<string, string> { ["priority"] = "high" },
        });

        await Eventually.TrueAsync(() => probe.Received.Count == 1);
        var received = probe.Received.Single();
        Assert.Equal("imports.acme.completed", received.RoutingKey);
        Assert.Equal("high", received.Headers["priority"]);
        Assert.Equal("acme", received.TenantId);
    }

    [Fact]
    public async Task A_message_for_a_missing_exchange_is_failed_and_does_not_block_the_outbox()
    {
        var scenario = new TestScenario("acme");
        await using var probe = await BrokerProbe.StartAsync(infrastructure, scenario);
        await using var app = await RelayboxApp.StartAsync(infrastructure, scenario);

        await app.CommitAsync("acme", new ImportCompleted("poison", 1),
            new OutboxMessageOptions { Destination = $"{scenario.Exchange}.does-not-exist" });
        await app.CommitAsync("acme", new ImportCompleted("imp-2", 2));

        await Eventually.TrueAsync(() => probe.Received.Count == 1);
        Assert.Contains("imp-2", probe.Received.Single().Body);

        var failed = await infrastructure.Outbox(scenario, "acme")
            .Find(Builders<BsonDocument>.Filter.Eq("status", "failed")).SingleAsync();
        Assert.Contains("404", failed["error"].AsString);
        Assert.Equal(1, failed["attempts"].AsInt32);

        var statistics = await app.Store.GetStatisticsAsync("acme", default);
        Assert.Equal(new OutboxStatistics(Pending: 0, Failed: 1, OldestPendingAt: null), statistics);
    }

    [Fact]
    public async Task The_store_reports_pending_messages_and_the_oldest_one()
    {
        var scenario = new TestScenario("acme");
        await using var writer = await RelayboxApp.StartAsync(infrastructure, scenario, withRelay: false);
        var before = DateTime.UtcNow.AddSeconds(-1);

        await writer.CommitAsync("acme", new ImportCompleted("imp-1", 1));
        await writer.CommitAsync("acme", new ImportCompleted("imp-2", 2));

        var statistics = await writer.Store.GetStatisticsAsync("acme", default);
        Assert.Equal(2, statistics.Pending);
        Assert.Equal(0, statistics.Failed);
        Assert.InRange(statistics.OldestPendingAt!.Value, before, DateTime.UtcNow);
    }

    [Fact]
    public async Task The_change_stream_reports_itself_connected()
    {
        var scenario = new TestScenario("acme");
        await using var app = await RelayboxApp.StartAsync(infrastructure, scenario);

        var changeStream = app.Services.GetServices<IOutboxSignalSource>().OfType<MongoChangeStreamSignalSource>().Single();
        await Eventually.TrueAsync(() => changeStream.IsConnected, TimeSpan.FromSeconds(10));
    }
}
