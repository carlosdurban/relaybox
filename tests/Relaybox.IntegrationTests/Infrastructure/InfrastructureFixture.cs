using MongoDB.Driver;
using Testcontainers.MongoDb;
using Testcontainers.RabbitMq;

namespace Relaybox.IntegrationTests.Infrastructure;

/// <summary>A real MongoDB replica set and a real RabbitMQ, shared by every test in the collection.</summary>
public sealed class InfrastructureFixture : IAsyncLifetime
{
    public MongoDbContainer Mongo { get; } = new MongoDbBuilder("mongo:7.0").WithReplicaSet("rs0").Build();

    public RabbitMqContainer Rabbit { get; } = new RabbitMqBuilder("rabbitmq:3.13").Build();

    public IMongoClient MongoClient { get; private set; } = null!;

    public string RabbitConnectionString => Rabbit.GetConnectionString();

    public async Task InitializeAsync()
    {
        await Task.WhenAll(Mongo.StartAsync(), Rabbit.StartAsync());
        MongoClient = new MongoClient(Mongo.GetConnectionString());
    }

    public async Task DisposeAsync()
    {
        await Mongo.DisposeAsync();
        await Rabbit.DisposeAsync();
    }
}

[CollectionDefinition(Name)]
public sealed class InfrastructureCollection : ICollectionFixture<InfrastructureFixture>
{
    public const string Name = "infrastructure";
}
