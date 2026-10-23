using MongoDB.Bson;
using MongoDB.Driver;
using Relaybox.MongoDb;
using Relaybox.Samples.Imports.Contracts;

namespace Relaybox.Samples.Imports.Infrastructure;

/// <summary>
/// Consumer side of the sample: keeps a notification per finished import. Runs inside the inbox
/// transaction, so a redelivered event never creates a second notification.
/// </summary>
public sealed class ImportNotificationHandler(
    IMongoSessionAccessor sessionAccessor,
    IMongoTenantDatabaseProvider databases,
    ITenantContext tenantContext,
    ILogger<ImportNotificationHandler> logger) : IMessageHandler<ImportCompletedV1>
{
    public async Task HandleAsync(ImportCompletedV1 message, MessageContext context, CancellationToken ct)
    {
        var database = await databases.GetDatabaseAsync(tenantContext.TenantId, ct);
        await database.GetCollection<BsonDocument>("notifications").InsertOneAsync(
            sessionAccessor.Session,
            new BsonDocument
            {
                { "importId", message.ImportId.ToString() },
                { "text", $"{message.FileName}: {message.ProcessedRows} rows imported, {message.FailedRows} failed" },
                { "createdAt", message.OccurredAt },
            },
            cancellationToken: ct);

        logger.LogInformation("Notified the completion of import {ImportId} of tenant {TenantId}",
            message.ImportId, context.TenantId);
    }
}
