using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Relaybox.Inbound;
using Relaybox.Serialization;
using Relaybox.Tenancy;

namespace Relaybox;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds the Relaybox core services. Continue with a tenant source, a database package
    /// (e.g. <c>AddMongoDb</c>) and a broker package (e.g. <c>AddRabbitMq</c>).
    /// </summary>
    public static RelayboxBuilder AddRelaybox(this IServiceCollection services, Action<RelayboxOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = services.AddOptions<RelayboxOptions>();
        if (configure is not null)
        {
            options.Configure(configure);
        }

        services.TryAddSingleton(TimeProvider.System);
        services.TryAddScoped<ITenantContext, TenantContext>();
        services.TryAddSingleton<ITenantRegistry, CachingTenantRegistry>();
        services.TryAddSingleton<IMessageSerializer, JsonMessageSerializer>();
        services.TryAddSingleton<OutboxMessageFactory>();
        services.TryAddSingleton(sp => new MessageHandlerRegistry(sp.GetServices<MessageHandlerRegistration>()));
        services.TryAddSingleton<InboundMessageDispatcher>();
        services.AddOptions<InboundOptions>();

        var messageTypes = services
            .Select(d => d.ServiceType == typeof(MessageTypeMap) ? d.ImplementationInstance as MessageTypeMap : null)
            .FirstOrDefault(map => map is not null);
        if (messageTypes is null)
        {
            messageTypes = new MessageTypeMap();
            services.AddSingleton(messageTypes);
        }

        return new RelayboxBuilder(services, messageTypes);
    }
}
