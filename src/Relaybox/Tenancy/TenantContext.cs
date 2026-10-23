namespace Relaybox.Tenancy;

internal sealed class TenantContext : ITenantContext
{
    private string? _tenantId;

    public string TenantId =>
        _tenantId ?? throw new InvalidOperationException("No tenant has been set for the current scope.");

    public bool HasTenant => _tenantId is not null;

    public void Set(string tenantId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);

        if (_tenantId is not null && _tenantId != tenantId)
        {
            throw new InvalidOperationException(
                $"The current scope already belongs to tenant '{_tenantId}' and cannot be switched to '{tenantId}'.");
        }

        _tenantId = tenantId;
    }
}
