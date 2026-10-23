using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using MongoDB.Driver;
using Relaybox.Inbound;

namespace Relaybox.MongoDb;

public static class MongoRelayboxBuilderExtensions
{
    /// <summary>
    /// Stores the outbox and inbox in each tenant's MongoDB database. Requires an <c>IMongoClient</c> in the
    /// container (or <see cref="MongoRelayboxOptions.ConnectionString"/>) connected to a replica set.
    /// </summary>
    public static RelayboxBuilder AddMongoDb(this RelayboxBuilder builder, Action<MongoRelayboxOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        MongoMappings.Register();

        var services = builder.Services;
        var options = services.AddOptions<MongoRelayboxOptions>()
            .Validate(o => o.OutboxRetention > TimeSpan.Zero, "OutboxRetention must be positive.")
            .Validate(o => o.InboxRetention > TimeSpan.Zero, "InboxRetention must be positive.")
            .ValidateOnStart();
        if (configure is not null)
        {
            options.Configure(configure);
        }

        // Decides what to register; the options themselves are read lazily from the container.
        var registration = new MongoRelayboxOptions();
        configure?.Invoke(registration);

        if (registration.ConnectionString is { } connectionString)
        {
            services.TryAddSingleton<IMongoClient>(_ => new MongoClient(connectionString));
        }

        services.TryAddSingleton<IMongoTenantDatabaseProvider, MongoTenantDatabaseProvider>();
        services.TryAddSingleton<MongoTenantCollections>();
        services.TryAddSingleton<IOutboxStore, MongoOutboxStore>();
        services.TryAddScoped<IOutbox, MongoOutbox>();
        services.TryAddScoped<MongoSessionAccessor>();
        services.TryAddScoped<IMongoSessionAccessor>(sp => sp.GetRequiredService<MongoSessionAccessor>());
        services.TryAddScoped<IInbox, MongoInbox>();

        if (registration.UseChangeStreams)
        {
            services.TryAddEnumerable(ServiceDescriptor.Singleton<IOutboxSignalSource, MongoChangeStreamSignalSource>());
        }

        services.Configure<InboundOptions>(o => o.TransientExceptions.Add(IsTransient));
        return builder;
    }

    private static bool IsTransient(Exception exception) => exception switch
    {
        MongoConnectionException or MongoExecutionTimeoutException or MongoNotPrimaryException
            or MongoNodeIsRecoveringException => true,
        MongoException mongo => mongo.HasErrorLabel("TransientTransactionError")
            || mongo.HasErrorLabel("RetryableWriteError"),
        _ => false,
    };
}
