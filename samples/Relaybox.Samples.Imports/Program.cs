using MongoDB.Bson;
using MongoDB.Driver;
using Relaybox;
using Relaybox.MongoDb;
using Relaybox.RabbitMq;
using Relaybox.Samples.Imports.Contracts;
using Relaybox.Samples.Imports.Domain;
using Relaybox.Samples.Imports.Infrastructure;

ImportMappings.Register();

var builder = WebApplication.CreateBuilder(args);
var tenants = builder.Configuration.GetSection("Tenants").Get<TenantInfo[]>() ?? [];

builder.Services.AddRelaybox()
    .AddTenants(tenants)
    .MapMessage<ImportStartedV1>(ImportMessageNames.Started)
    .MapMessage<ImportCompletedV1>(ImportMessageNames.Completed)
    .MapMessage<ImportFailedV1>(ImportMessageNames.Failed)
    .AddMongoDb(o => o.ConnectionString = builder.Configuration.GetConnectionString("Mongo"))
    .AddRabbitMq(o =>
    {
        o.ConnectionString = builder.Configuration.GetConnectionString("RabbitMq")!;
        o.Exchange = "imports";
        o.ClientProvidedName = $"imports-sample-{Environment.MachineName}";
    })
    .AddOutboxRelay()
    .AddMessageHandler<ImportCompletedV1, ImportNotificationHandler>()
    .AddRabbitMqConsumer("imports.notifications");

builder.Services.AddScoped<ImportRepository>();
builder.Services.AddHealthChecks().AddRelayboxOutbox();

var app = builder.Build();

app.Use(async (context, next) =>
{
    try
    {
        await next(context);
    }
    catch (DomainException ex)
    {
        await Results.Problem(ex.Message, statusCode: StatusCodes.Status422UnprocessableEntity).ExecuteAsync(context);
    }
    catch (ConcurrencyException ex)
    {
        await Results.Problem(ex.Message, statusCode: StatusCodes.Status409Conflict).ExecuteAsync(context);
    }
});
app.MapHealthChecks("/health");
app.UseWhen(
    context => !context.Request.Path.StartsWithSegments("/health"),
    tenantScoped => tenantScoped.UseMiddleware<TenantHeaderMiddleware>());

var imports = app.MapGroup("/imports");

imports.MapPost("/", async (StartImport request, ITenantContext tenant, ImportRepository repository, TimeProvider time, CancellationToken ct) =>
{
    var import = Import.Start(tenant.TenantId, request.FileName, request.ExpectedRows, time.GetUtcNow().UtcDateTime);
    await repository.SaveAsync(import, ct);
    return Results.Created($"/imports/{import.Id}", ImportView.From(import));
});

imports.MapGet("/{id:guid}", async (Guid id, ImportRepository repository, CancellationToken ct) =>
    await repository.GetAsync(id, ct) is { } import ? Results.Ok(ImportView.From(import)) : Results.NotFound());

imports.MapPost("/{id:guid}/batches", async (Guid id, RecordBatch request, ImportRepository repository, TimeProvider time, CancellationToken ct) =>
{
    if (await repository.GetAsync(id, ct) is not { } import)
    {
        return Results.NotFound();
    }

    import.RecordBatch(request.ProcessedRows, request.FailedRows, time.GetUtcNow().UtcDateTime);
    await repository.SaveAsync(import, ct);
    return Results.Ok(ImportView.From(import));
});

imports.MapPost("/{id:guid}/fail", async (Guid id, FailImport request, ImportRepository repository, TimeProvider time, CancellationToken ct) =>
{
    if (await repository.GetAsync(id, ct) is not { } import)
    {
        return Results.NotFound();
    }

    import.Fail(request.Reason, time.GetUtcNow().UtcDateTime);
    await repository.SaveAsync(import, ct);
    return Results.Ok(ImportView.From(import));
});

app.MapGet("/notifications", async (ITenantContext tenant, IMongoTenantDatabaseProvider databases, CancellationToken ct) =>
{
    var database = await databases.GetDatabaseAsync(tenant.TenantId, ct);
    var notifications = await database.GetCollection<BsonDocument>("notifications")
        .Find(FilterDefinition<BsonDocument>.Empty)
        .Project(Builders<BsonDocument>.Projection.Exclude("_id"))
        .ToListAsync(ct);
    return Results.Ok(notifications.Select(n => n.ToDictionary()));
});

app.Run();

internal sealed record StartImport(string FileName, int ExpectedRows);

internal sealed record RecordBatch(int ProcessedRows, int FailedRows);

internal sealed record FailImport(string Reason);

internal sealed record ImportView(
    Guid Id, string FileName, string Status, int ExpectedRows, int ProcessedRows, int FailedRows, int Version)
{
    public static ImportView From(Import import) => new(
        import.Id, import.FileName, import.Status.ToString(), import.ExpectedRows, import.ProcessedRows, import.FailedRows,
        import.Version);
}
