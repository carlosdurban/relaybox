namespace Relaybox.Samples.Imports.Contracts;

// Public message contracts: what other services see. They change only in backwards-compatible ways,
// independently of the domain events they are built from.

public sealed record ImportStartedV1(Guid ImportId, string TenantId, string FileName, int ExpectedRows, DateTime OccurredAt);

public sealed record ImportCompletedV1(
    Guid ImportId, string TenantId, string FileName, bool HasErrors, int ProcessedRows, int FailedRows, DateTime OccurredAt);

public sealed record ImportFailedV1(Guid ImportId, string TenantId, string FileName, string Reason, DateTime OccurredAt);

public static class ImportMessageNames
{
    public const string Started = "imports.started.v1";
    public const string Completed = "imports.completed.v1";
    public const string Failed = "imports.failed.v1";
}
