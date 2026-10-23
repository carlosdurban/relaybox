namespace Relaybox;

/// <summary>
/// A tenant and the database that holds its data (including its outbox and inbox).
/// </summary>
public sealed record TenantInfo(string TenantId, string DatabaseName);
