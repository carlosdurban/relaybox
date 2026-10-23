using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace Relaybox.RabbitMq;

public static class RabbitMqRelayboxBuilderExtensions
{
    /// <summary>Publishes outbox messages to RabbitMQ with publisher confirms.</summary>
    public static RelayboxBuilder AddRabbitMq(this RelayboxBuilder builder, Action<RabbitMqOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var options = builder.Services.AddOptions<RabbitMqOptions>()
            .Validate(o => Uri.TryCreate(o.ConnectionString, UriKind.Absolute, out _), "ConnectionString must be an AMQP URI.")
            .Validate(o => !string.IsNullOrWhiteSpace(o.Exchange), "Exchange is required.")
            .Validate(o => o.PublishTimeout > TimeSpan.Zero, "PublishTimeout must be positive.")
            .ValidateOnStart();
        if (configure is not null)
        {
            options.Configure(configure);
        }

        builder.Services.TryAddSingleton<RabbitMqConnection>();
        builder.Services.TryAddSingleton<IMessagePublisher, RabbitMqMessagePublisher>();
        return builder;
    }

    /// <summary>
    /// Consumes <paramref name="queue"/> with the registered message handlers. Requires <see cref="AddRabbitMq"/>
    /// and an <see cref="IInbox"/> (e.g. <c>AddMongoDb</c>).
    /// </summary>
    public static RelayboxBuilder AddRabbitMqConsumer(
        this RelayboxBuilder builder, string queue, Action<RabbitMqConsumerOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);

        var consumerOptions = new RabbitMqConsumerOptions();
        configure?.Invoke(consumerOptions);

        builder.Services.AddSingleton<IHostedService>(sp =>
            ActivatorUtilities.CreateInstance<RabbitMqConsumerService>(sp, queue, consumerOptions));
        return builder;
    }
}
