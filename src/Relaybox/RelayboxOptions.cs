namespace Relaybox;

/// <summary>General Relaybox options.</summary>
public sealed class RelayboxOptions
{
    /// <summary>How often the tenant list is reloaded from <see cref="ITenantSource"/>. Default: 1 minute.</summary>
    public TimeSpan TenantRefreshInterval { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Minimum time between forced reloads triggered by an unknown tenant or database. Default: 10 seconds.
    /// </summary>
    public TimeSpan TenantMissRefreshCooldown { get; set; } = TimeSpan.FromSeconds(10);
}
