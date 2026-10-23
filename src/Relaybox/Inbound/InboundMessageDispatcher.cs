using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Relaybox.Diagnostics;
using Relaybox.Serialization;

namespace Relaybox.Inbound;

/// <summary>
/// Broker-agnostic consumption pipeline: validates the message, opens a scope for its tenant,
/// runs the handlers exactly once through the <see cref="IInbox"/> and classifies failures.
/// Broker integrations translate deliveries into <see cref="InboundMessage"/> and the returned
/// <see cref="DispatchOutcome"/> into ack/nack.
/// </summary>
public sealed class InboundMessageDispatcher
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly MessageTypeMap _types;
    private readonly IMessageSerializer _serializer;
    private readonly MessageHandlerRegistry _handlers;
    private readonly IOptions<InboundOptions> _options;
    private readonly TimeProvider _time;
    private readonly ILogger<InboundMessageDispatcher> _logger;

    public InboundMessageDispatcher(
        IServiceScopeFactory scopeFactory,
        MessageTypeMap types,
        IMessageSerializer serializer,
        MessageHandlerRegistry handlers,
        IOptions<InboundOptions> options,
        TimeProvider time,
        ILogger<InboundMessageDispatcher> logger)
    {
        _scopeFactory = scopeFactory;
        _types = types;
        _serializer = serializer;
        _handlers = handlers;
        _options = options;
        _time = time;
        _logger = logger;
    }

    public async Task<DispatchOutcome> DispatchAsync(InboundMessage message, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(message.TenantId))
        {
            return DeadLetter(message, "the tenant-id header is missing");
        }

        if (string.IsNullOrWhiteSpace(message.MessageId))
        {
            return DeadLetter(message, "the message id is missing");
        }

        if (string.IsNullOrWhiteSpace(message.MessageType)
            || !_types.TryGetType(message.MessageType, out var type)
            || !_handlers.TryGetInvoker(type, out var invoker))
        {
            return DeadLetter(message, "there is no handler for its type");
        }

        object payload;
        try
        {
            payload = _serializer.Deserialize(System.Text.Encoding.UTF8.GetString(message.Body.Span), type);
        }
        catch (Exception ex)
        {
            return DeadLetter(message, "its payload cannot be deserialized", ex);
        }

        using var logScope = _logger.BeginScope(new Dictionary<string, object>
        {
            ["tenantId"] = message.TenantId,
            ["messageId"] = message.MessageId,
        });
        using var activity = RelayboxDiagnostics.StartActivity($"{message.MessageType} process", ActivityKind.Consumer, message.TraceParent);
        activity?.SetTag(RelayboxDiagnostics.TenantTag, message.TenantId);
        activity?.SetTag("messaging.message.id", message.MessageId);

        var settings = _options.Value;
        for (var attempt = 1; ; attempt++)
        {
            var context = new MessageContext(message.MessageId, message.TenantId, message.MessageType, attempt);
            try
            {
                var result = await ExecuteAsync(context, payload, invoker, ct);
                Record(message, result == InboxResult.Duplicate ? "duplicate" : "processed");
                if (result == InboxResult.Duplicate)
                {
                    _logger.LogInformation("Message {MessageId} ({MessageType}) of tenant {TenantId} was already processed; discarded",
                        message.MessageId, message.MessageType, message.TenantId);
                }

                return DispatchOutcome.Ack;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                Record(message, "requeued");
                return DispatchOutcome.Requeue;
            }
            catch (Exception ex) when (settings.IsTransient(ex) && attempt < settings.MaxAttempts)
            {
                var delay = settings.RetryDelay(attempt);
                _logger.LogWarning(ex,
                    "Transient failure processing message {MessageId} ({MessageType}) of tenant {TenantId}, attempt {Attempt}; retrying in {Delay}",
                    message.MessageId, message.MessageType, message.TenantId, attempt, delay);

                try
                {
                    await Task.Delay(delay, _time, ct);
                }
                catch (OperationCanceledException)
                {
                    Record(message, "requeued");
                    return DispatchOutcome.Requeue;
                }
            }
            catch (Exception ex)
            {
                activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                var reason = settings.IsTransient(ex) ? $"it still fails after {attempt} attempts" : "its handler failed with a non-transient error";
                return DeadLetter(message, reason, ex);
            }
        }
    }

    private async Task<InboxResult> ExecuteAsync(
        MessageContext context, object payload, MessageHandlerInvoker invoker, CancellationToken ct)
    {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var services = scope.ServiceProvider;
        services.GetRequiredService<ITenantContext>().Set(context.TenantId);

        var inbox = services.GetRequiredService<IInbox>();
        return await inbox.ExecuteOnceAsync(context, token => invoker(services, payload, context, token), ct);
    }

    private DispatchOutcome DeadLetter(InboundMessage message, string reason, Exception? exception = null)
    {
        _logger.LogError(exception,
            "Message {MessageId} ({MessageType}) of tenant {TenantId} sent to the dead letter queue because {Reason}",
            message.MessageId, message.MessageType, message.TenantId, reason);
        Record(message, "dead_lettered");
        return DispatchOutcome.DeadLetter;
    }

    private static void Record(InboundMessage message, string outcome) =>
        RelayboxDiagnostics.MessagesConsumed.Add(1,
            new KeyValuePair<string, object?>(RelayboxDiagnostics.TenantTag, message.TenantId ?? "unknown"),
            new KeyValuePair<string, object?>(RelayboxDiagnostics.MessageTypeTag, message.MessageType ?? "unknown"),
            new KeyValuePair<string, object?>(RelayboxDiagnostics.OutcomeTag, outcome));
}
