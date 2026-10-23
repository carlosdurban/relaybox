namespace Relaybox;

/// <summary>
/// Scoped holder of the current tenant. Relaybox sets it at the start of every scope it creates
/// (outbox relay per tenant, consumer per message), so everything resolved from that scope
/// behaves exactly as it does inside an HTTP request for that tenant.
/// </summary>
/// <remarks>
/// Relaybox registers a default implementation. Register your own before calling
/// <c>AddRelaybox</c> if your application already has one.
/// </remarks>
public interface ITenantContext
{
    /// <summary>The current tenant. Throws if no tenant has been set.</summary>
    string TenantId { get; }

    bool HasTenant { get; }

    void Set(string tenantId);
}
