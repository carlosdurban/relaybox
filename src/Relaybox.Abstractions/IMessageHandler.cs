namespace Relaybox;

/// <summary>
/// Handles one message contract. Resolved from a scope that already belongs to the message's tenant
/// and runs inside the inbox transaction, so everything it writes is committed together with the
/// "message processed" record, or not at all.
/// </summary>
public interface IMessageHandler<in TMessage>
{
    Task HandleAsync(TMessage message, MessageContext context, CancellationToken ct);
}

/// <summary>Metadata of the message being handled.</summary>
/// <param name="MessageId">Broker message id (the outbox id of the producer).</param>
/// <param name="TenantId">Tenant taken from the <c>tenant-id</c> header.</param>
/// <param name="MessageType">Stable contract name.</param>
/// <param name="Attempt">Processing attempt within this delivery, starting at 1.</param>
public sealed record MessageContext(string MessageId, string TenantId, string MessageType, int Attempt);
