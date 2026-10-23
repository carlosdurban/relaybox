using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Relaybox.Inbound;
using Relaybox.Relay;
using Relaybox.Serialization;
using Relaybox.Tenancy;

namespace Relaybox;

/// <summary>Configures Relaybox. Database and broker packages add their own extension methods.</summary>
public sealed class RelayboxBuilder
{
    internal RelayboxBuilder(IServiceCollection services, MessageTypeMap messageTypes)
    {
        Services = services;
        MessageTypes = messageTypes;
    }

    public IServiceCollection Services { get; }

    public MessageTypeMap MessageTypes { get; }

    /// <summary>
    /// Gives <typeparamref name="TMessage"/> a stable name on the wire, independent of its CLR name.
    /// Strongly recommended for every published contract.
    /// </summary>
    public RelayboxBuilder MapMessage<TMessage>(string name)
    {
        MessageTypes.Map(typeof(TMessage), name);
        return this;
    }

    /// <summary>Loads the tenants from <typeparamref name="TSource"/> (cached and refreshed periodically).</summary>
    public RelayboxBuilder AddTenantSource<TSource>()
        where TSource : class, ITenantSource
    {
        Services.Replace(ServiceDescriptor.Singleton<ITenantSource, TSource>());
        return this;
    }

    /// <summary>Uses a fixed list of tenants. Handy for samples and tests.</summary>
    public RelayboxBuilder AddTenants(params TenantInfo[] tenants)
    {
        Services.Replace(ServiceDescriptor.Singleton<ITenantSource>(new StaticTenantSource(tenants)));
        return this;
    }

    /// <summary>
    /// Runs the outbox relay in this process: a hosted service that publishes the outbox of every tenant.
    /// Reconciliation (polling) is always on; database packages can add faster signal sources.
    /// </summary>
    public RelayboxBuilder AddOutboxRelay(Action<OutboxRelayOptions>? configure = null)
    {
        var options = Services.AddOptions<OutboxRelayOptions>()
            .Validate(o => o.MaxDegreeOfParallelism > 0, "MaxDegreeOfParallelism must be positive.")
            .Validate(o => o.MaxMessagesPerTenantPerRound > 0, "MaxMessagesPerTenantPerRound must be positive.")
            .Validate(o => o.LockDuration > TimeSpan.Zero, "LockDuration must be positive.")
            .Validate(o => o.ReconciliationInterval > TimeSpan.Zero, "ReconciliationInterval must be positive.")
            .ValidateOnStart();
        if (configure is not null)
        {
            options.Configure(configure);
        }

        Services.TryAddSingleton<TenantWorkQueue>();
        Services.TryAddSingleton<TenantOutboxProcessor>();
        Services.TryAddEnumerable(ServiceDescriptor.Singleton<IOutboxSignalSource, PollingSignalSource>());
        Services.AddHostedService<OutboxRelayService>();
        return this;
    }

    /// <summary>
    /// Registers a handler for inbound messages of type <typeparamref name="TMessage"/>. Unmapped message types
    /// are mapped to their CLR name.
    /// </summary>
    public RelayboxBuilder AddMessageHandler<TMessage, THandler>()
        where THandler : class, IMessageHandler<TMessage>
    {
        MessageTypes.MapIfAbsent(typeof(TMessage));
        Services.AddScoped<IMessageHandler<TMessage>, THandler>();
        Services.AddSingleton(new MessageHandlerRegistration(typeof(TMessage)));
        return this;
    }

    /// <summary>Configures retries and failure classification of inbound messages.</summary>
    public RelayboxBuilder ConfigureInbound(Action<InboundOptions> configure)
    {
        Services.Configure(configure);
        return this;
    }
}
