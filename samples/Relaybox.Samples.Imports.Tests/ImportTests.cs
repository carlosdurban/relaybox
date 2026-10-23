using Relaybox.Samples.Imports.Domain;

namespace Relaybox.Samples.Imports.Tests;

public sealed class ImportTests
{
    private static readonly DateTime Now = new(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void Starting_an_import_raises_ImportStarted()
    {
        var import = Import.Start("acme", "customers.csv", 10, Now);

        Assert.Equal(ImportStatus.Processing, import.Status);
        var started = Assert.IsType<ImportStarted>(Assert.Single(import.DomainEvents));
        Assert.Equal("acme", started.TenantId);
        Assert.Equal(10, started.ExpectedRows);
    }

    [Theory]
    [InlineData("", "customers.csv", 10)]
    [InlineData("acme", " ", 10)]
    [InlineData("acme", "customers.csv", 0)]
    public void An_import_cannot_start_with_invalid_data(string tenantId, string fileName, int expectedRows)
    {
        Assert.Throws<DomainException>(() => Import.Start(tenantId, fileName, expectedRows, Now));
    }

    [Fact]
    public void Recording_the_last_rows_completes_the_import()
    {
        var import = Import.Start("acme", "customers.csv", 10, Now);

        import.RecordBatch(6, 0, Now);
        Assert.Equal(ImportStatus.Processing, import.Status);
        import.RecordBatch(4, 0, Now);

        Assert.Equal(ImportStatus.Completed, import.Status);
        Assert.Equal(Now, import.FinishedAt);
        var completed = Assert.IsType<ImportCompleted>(import.DomainEvents.Last());
        Assert.Equal(10, completed.ProcessedRows);
    }

    [Fact]
    public void Failed_rows_complete_the_import_with_errors()
    {
        var import = Import.Start("acme", "customers.csv", 10, Now);

        import.RecordBatch(8, 2, Now);

        Assert.Equal(ImportStatus.CompletedWithErrors, import.Status);
        Assert.Equal(2, Assert.IsType<ImportCompleted>(import.DomainEvents.Last()).FailedRows);
    }

    [Theory]
    [InlineData(-1, 0)]
    [InlineData(0, -1)]
    [InlineData(0, 0)]
    public void A_batch_needs_a_positive_number_of_rows(int processed, int failed)
    {
        var import = Import.Start("acme", "customers.csv", 10, Now);

        Assert.Throws<DomainException>(() => import.RecordBatch(processed, failed, Now));
    }

    [Fact]
    public void A_batch_cannot_exceed_the_expected_rows()
    {
        var import = Import.Start("acme", "customers.csv", 10, Now);
        import.RecordBatch(9, 0, Now);

        Assert.Throws<DomainException>(() => import.RecordBatch(2, 0, Now));
        Assert.Equal(9, import.RecordedRows);
    }

    [Fact]
    public void A_finished_import_cannot_change()
    {
        var import = Import.Start("acme", "customers.csv", 1, Now);
        import.RecordBatch(1, 0, Now);

        Assert.Throws<DomainException>(() => import.RecordBatch(1, 0, Now));
        Assert.Throws<DomainException>(() => import.Fail("too late", Now));
    }

    [Fact]
    public void Failing_an_import_needs_a_reason_and_raises_ImportFailed()
    {
        var import = Import.Start("acme", "customers.csv", 10, Now);
        Assert.Throws<DomainException>(() => import.Fail(" ", Now));

        import.Fail("Corrupted file", Now);

        Assert.Equal(ImportStatus.Failed, import.Status);
        Assert.Equal("Corrupted file", Assert.IsType<ImportFailed>(import.DomainEvents.Last()).Reason);
    }

    [Fact]
    public void Aggregates_have_no_public_setters()
    {
        var publicSetters = typeof(Import).Assembly.GetTypes()
            .Where(t => t.BaseType is { IsGenericType: true } b && b.GetGenericTypeDefinition() == typeof(AggregateRoot<>))
            .SelectMany(t => t.GetProperties().Concat(t.BaseType!.GetProperties()))
            .Where(p => p.SetMethod?.IsPublic == true)
            .Select(p => $"{p.DeclaringType!.Name}.{p.Name}");

        Assert.Empty(publicSetters);
    }
}
