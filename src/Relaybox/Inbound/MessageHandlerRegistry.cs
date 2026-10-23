using System.Reflection;
using Microsoft.Extensions.DependencyInjection;

namespace Relaybox.Inbound;

internal sealed record MessageHandlerRegistration(Type MessageType);

internal delegate Task MessageHandlerInvoker(
    IServiceProvider services, object message, MessageContext context, CancellationToken ct);

/// <summary>Message types with at least one <see cref="IMessageHandler{TMessage}"/>, and how to invoke them.</summary>
public sealed class MessageHandlerRegistry
{
    private static readonly MethodInfo InvokeMethod =
        typeof(MessageHandlerRegistry).GetMethod(nameof(InvokeAsync), BindingFlags.NonPublic | BindingFlags.Static)!;

    private readonly Dictionary<Type, MessageHandlerInvoker> _invokers;

    internal MessageHandlerRegistry(IEnumerable<MessageHandlerRegistration> registrations)
    {
        _invokers = registrations
            .Select(r => r.MessageType)
            .Distinct()
            .ToDictionary(
                type => type,
                type => InvokeMethod.MakeGenericMethod(type).CreateDelegate<MessageHandlerInvoker>());
    }

    /// <summary>Message types with at least one handler.</summary>
    public IReadOnlyCollection<Type> MessageTypes => _invokers.Keys;

    internal bool TryGetInvoker(Type messageType, out MessageHandlerInvoker invoker) =>
        _invokers.TryGetValue(messageType, out invoker!);

    private static async Task InvokeAsync<TMessage>(
        IServiceProvider services, object message, MessageContext context, CancellationToken ct)
    {
        foreach (var handler in services.GetServices<IMessageHandler<TMessage>>())
        {
            await handler.HandleAsync((TMessage)message, context, ct);
        }
    }
}
