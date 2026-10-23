using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;
using Relaybox.MongoDb;
using Relaybox.RabbitMq;
using Relaybox.Relay;

namespace Relaybox.IntegrationTests.Infrastructure;

/// <summary>One running instance ("replica") of an application that uses Relaybox.</summary>
public sealed class RelayboxApp : IAsyncDisposable
{
    private readonly IHost _host;

    private RelayboxApp(IHost host) => _host = host;

    public IServiceProvider Services => _host.Services;

    public static async Task<RelayboxApp> StartAsync(
        InfrastructureFixture infrastructure,
        TestScenario scenario,
        Action<OutboxRelayOptions>? relay = null,
        Action<RelayboxBuilder>? configure = null,
        bool withRelay = true)
    {
        // Read eagerly: the container cannot be inspected while a test has it paused.
        var rabbitConnectionString = infrastructure.RabbitConnectionString;
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        builder.Services.AddSingleton(infrastructure.MongoClient);

        var relaybox = builder.Services
            .AddRelaybox(o => o.TenantMissRefreshCooldown = TimeSpan.FromMilliseconds(500))
            .MapMessage<ImportCompleted>("import.completed")
            .AddMongoDb()
            .AddRabbitMq(o =>
            {
                o.ConnectionString = rabbitConnectionString;
                o.Exchange = scenario.Exchange;
                o.PublishTimeout = TimeSpan.FromSeconds(3);
            });
        builder.Services.AddSingleton<ITenantSource>(scenario.Tenants);

        if (withRelay)
        {
            relaybox.AddOutboxRelay(o =>
            {
                o.ReconciliationInterval = TimeSpan.FromSeconds(2);
                o.RetryBaseDelay = TimeSpan.FromMilliseconds(500);
                o.RetryMaxDelay = TimeSpan.FromSeconds(2);
                relay?.Invoke(o);
            });
        }

        configure?.Invoke(relaybox);

        var host = builder.Build();
        await host.StartAsync();
        return new RelayboxApp(host);
    }

    /// <summary>Adds the events to the tenant's outbox in a transaction, like a repository would.</summary>
    public async Task CommitAsync(string tenantId, params object[] events) =>
        await WriteAsync(tenantId, events, commit: true);

    public async Task AbortAsync(string tenantId, params object[] events) =>
        await WriteAsync(tenantId, events, commit: false);

    public async Task CommitAsync(string tenantId, object @event, OutboxMessageOptions options) =>
        await WriteAsync(tenantId, [@event], commit: true, options);

    public IOutboxStore Store => Services.GetRequiredService<IOutboxStore>();

    private async Task WriteAsync(string tenantId, object[] events, bool commit, OutboxMessageOptions? options = null)
    {
        await using var scope = Services.CreateAsyncScope();
        scope.ServiceProvider.GetRequiredService<ITenantContext>().Set(tenantId);
        var client = scope.ServiceProvider.GetRequiredService<IMongoClient>();
        var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();

        using var session = await client.StartSessionAsync();
        session.StartTransaction();
        if (options is null)
        {
            await outbox.AddRangeAsync(session, events);
        }
        else
        {
            await outbox.AddAsync(session, events.Single(), options);
        }

        if (commit)
        {
            await session.CommitTransactionAsync();
        }
        else
        {
            await session.AbortTransactionAsync();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _host.StopAsync();
        // Asynchronous: disposing synchronously blocks on the RabbitMQ client under xUnit's synchronization context.
        await ((IAsyncDisposable)_host).DisposeAsync();
    }
}

public static class MongoTestExtensions
{
    public static IMongoCollection<BsonDocument> Outbox(this InfrastructureFixture infrastructure, TestScenario scenario, string tenantId) =>
        infrastructure.MongoClient.GetDatabase(scenario.DatabaseOf(tenantId)).GetCollection<BsonDocument>("outbox");

    public static IMongoCollection<BsonDocument> Collection(
        this InfrastructureFixture infrastructure, TestScenario scenario, string tenantId, string name) =>
        infrastructure.MongoClient.GetDatabase(scenario.DatabaseOf(tenantId)).GetCollection<BsonDocument>(name);
}
