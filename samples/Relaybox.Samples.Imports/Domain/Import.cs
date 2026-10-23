namespace Relaybox.Samples.Imports.Domain;

/// <summary>
/// The import of a file for one tenant. Rows are recorded in batches as they are processed;
/// the import completes by itself when every expected row has been recorded.
/// </summary>
public sealed class Import : AggregateRoot<Guid>
{
    public string TenantId { get; private set; } = default!;
    public string FileName { get; private set; } = default!;
    public int ExpectedRows { get; private set; }
    public int ProcessedRows { get; private set; }
    public int FailedRows { get; private set; }
    public ImportStatus Status { get; private set; }
    public DateTime StartedAt { get; private set; }
    public DateTime? FinishedAt { get; private set; }
    public string? FailureReason { get; private set; }

    private Import()
    {
    }

    public int RecordedRows => ProcessedRows + FailedRows;

    public static Import Start(string tenantId, string fileName, int expectedRows, DateTime now)
    {
        if (string.IsNullOrWhiteSpace(tenantId))
        {
            throw new DomainException("An import must belong to a tenant.");
        }

        if (string.IsNullOrWhiteSpace(fileName))
        {
            throw new DomainException("An import needs the name of the file being imported.");
        }

        if (expectedRows <= 0)
        {
            throw new DomainException("An import must expect at least one row.");
        }

        var import = new Import
        {
            Id = Guid.NewGuid(),
            TenantId = tenantId,
            FileName = fileName.Trim(),
            ExpectedRows = expectedRows,
            Status = ImportStatus.Processing,
            StartedAt = now,
        };

        import.Raise(new ImportStarted(import.Id, tenantId, import.FileName, expectedRows, now));
        return import;
    }

    /// <summary>Records a processed batch. Completes the import when it was the last one.</summary>
    public void RecordBatch(int processedRows, int failedRows, DateTime now)
    {
        EnsureProcessing();

        if (processedRows < 0 || failedRows < 0)
        {
            throw new DomainException("A batch cannot have a negative number of rows.");
        }

        if (processedRows + failedRows == 0)
        {
            throw new DomainException("A batch must contain at least one row.");
        }

        if (RecordedRows + processedRows + failedRows > ExpectedRows)
        {
            throw new DomainException(
                $"The batch exceeds the expected rows: {RecordedRows} recorded, {processedRows + failedRows} more, {ExpectedRows} expected.");
        }

        ProcessedRows += processedRows;
        FailedRows += failedRows;

        if (RecordedRows == ExpectedRows)
        {
            Complete(now);
        }
    }

    public void Fail(string reason, DateTime now)
    {
        EnsureProcessing();

        if (string.IsNullOrWhiteSpace(reason))
        {
            throw new DomainException("A failed import needs a reason.");
        }

        Status = ImportStatus.Failed;
        FailureReason = reason.Trim();
        FinishedAt = now;
        Raise(new ImportFailed(Id, TenantId, FileName, FailureReason, RecordedRows, now));
    }

    private void Complete(DateTime now)
    {
        Status = FailedRows > 0 ? ImportStatus.CompletedWithErrors : ImportStatus.Completed;
        FinishedAt = now;
        Raise(new ImportCompleted(Id, TenantId, FileName, Status, ProcessedRows, FailedRows, now));
    }

    private void EnsureProcessing()
    {
        if (Status != ImportStatus.Processing)
        {
            throw new DomainException($"The import is {Status}; only imports being processed can change.");
        }
    }
}

public enum ImportStatus
{
    Processing,
    Completed,
    CompletedWithErrors,
    Failed,
}

public sealed record ImportStarted(Guid ImportId, string TenantId, string FileName, int ExpectedRows, DateTime OccurredAt)
    : IDomainEvent;

public sealed record ImportCompleted(
    Guid ImportId, string TenantId, string FileName, ImportStatus Status, int ProcessedRows, int FailedRows, DateTime OccurredAt)
    : IDomainEvent;

public sealed record ImportFailed(
    Guid ImportId, string TenantId, string FileName, string Reason, int RecordedRows, DateTime OccurredAt)
    : IDomainEvent;
