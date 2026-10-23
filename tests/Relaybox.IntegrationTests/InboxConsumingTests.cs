using MongoDB.Bson;
using MongoDB.Driver;
using Relaybox.IntegrationTests.Infrastructure;
using Relaybox.MongoDb;
using Relaybox.RabbitMq;

namespace Relaybox.IntegrationTests;

[Collection(InfrastructureCollection.Name)]
public sealed class InboxConsumingTests(InfrastructureFixture infrastructure)
{
    private const string Body = """{"importId":"imp-1","rows":10}""";

    [Fact]
    public async Task Published_events_are_handled_by_the_consumer_inside_the_inbox_transaction()
    {
        var scenario = new TestScenario("acme");
        await using var producer = await RelayboxApp.StartAsync(infrastructure, scenario);
        await using var consumer = await StartConsumerAsync(scenario);

        await producer.CommitAsync("acme", new ImportCompleted("imp-1", 10));

        await Eventually.TrueAsync(async () => await CountAsync(scenario, "projections") == 1);
        var outboxId = (await infrastructure.Outbox(scenario, "acme").Find(FilterDefinition<BsonDocument>.Empty).SingleAsync())["_id"].AsGuid;
        var inbox = await infrastructure.Collection(scenario, "acme", "inbox").Find(FilterDefinition<BsonDocument>.Empty).SingleAsync();
        Assert.Equal(outboxId.ToString(), inbox["_id"].AsString);
    }

    [Fact]
    public async Task A_redelivered_message_is_discarded_by_the_inbox()
    {
        var scenario = new TestScenario("acme");
        await using var consumer = await StartConsumerAsync(scenario);
        await using var probe = await BrokerProbe.StartAsync(infrastructure, scenario, listen: false);
        await probe.WaitForConsumerAsync(scenario.Queue);

        // What happens when an instance dies between the broker confirmation and marking the message processed.
        await probe.PublishRawAsync(scenario, "msg-1", "acme", "import.completed", Body);
        await probe.PublishRawAsync(scenario, "msg-1", "acme", "import.completed", Body);

        await Eventually.TrueAsync(async () => await probe.CountAsync(scenario.Queue) == 0
            && await CountAsync(scenario, "inbox") == 1);
        await Task.Delay(TimeSpan.FromSeconds(1));
        Assert.Equal(1, await CountAsync(scenario, "projections"));
    }

    [Fact]
    public async Task A_message_without_tenant_header_goes_to_the_dead_letter_queue()
    {
        var scenario = new TestScenario("acme");
        await using var consumer = await StartConsumerAsync(scenario);
        await using var probe = await BrokerProbe.StartAsync(infrastructure, scenario, listen: false);
        await probe.WaitForConsumerAsync(scenario.Queue);

        await probe.PublishRawAsync(scenario, "msg-1", tenantId: null, "import.completed", Body);

        await Eventually.TrueAsync(async () => await probe.CountAsync($"{scenario.Queue}.dlq") == 1);
        Assert.Equal(0, await CountAsync(scenario, "projections"));
    }

    [Fact]
    public async Task A_failing_handler_leaves_no_trace_and_the_message_is_dead_lettered()
    {
        var scenario = new TestScenario("acme");
        await using var consumer = await StartConsumerAsync(scenario);
        await using var probe = await BrokerProbe.StartAsync(infrastructure, scenario, listen: false);
        await probe.WaitForConsumerAsync(scenario.Queue);

        await probe.PublishRawAsync(scenario, "msg-1", "acme", "import.completed", """{"importId":"imp-1","rows":-1}""");

        await Eventually.TrueAsync(async () => await probe.CountAsync($"{scenario.Queue}.dlq") == 1);
        Assert.Equal(0, await CountAsync(scenario, "projections"));
        Assert.Equal(0, await CountAsync(scenario, "inbox"));
    }

    private Task<RelayboxApp> StartConsumerAsync(TestScenario scenario) =>
        RelayboxApp.StartAsync(infrastructure, scenario, withRelay: false, configure: relaybox => relaybox
            .AddMessageHandler<ImportCompleted, ProjectImportHandler>()
            .AddRabbitMqConsumer(scenario.Queue));

    private Task<long> CountAsync(TestScenario scenario, string collection) =>
        infrastructure.Collection(scenario, "acme", collection).CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty);

    /// <summary>Writes a projection with the inbox session; rejects negative row counts as a domain error.</summary>
    private sealed class ProjectImportHandler(
        IMongoSessionAccessor sessionAccessor,
        IMongoTenantDatabaseProvider databases,
        ITenantContext tenantContext) : IMessageHandler<ImportCompleted>
    {
        public async Task HandleAsync(ImportCompleted message, MessageContext context, CancellationToken ct)
        {
            var database = await databases.GetDatabaseAsync(tenantContext.TenantId, ct);
            await database.GetCollection<BsonDocument>("projections").InsertOneAsync(
                sessionAccessor.Session,
                new BsonDocument { { "importId", message.ImportId }, { "rows", message.Rows } },
                cancellationToken: ct);

            if (message.Rows < 0)
            {
                throw new InvalidOperationException("An import cannot have a negative number of rows.");
            }
        }
    }
}
