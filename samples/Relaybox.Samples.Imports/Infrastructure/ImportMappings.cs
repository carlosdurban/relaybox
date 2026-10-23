using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using Relaybox.Samples.Imports.Domain;

namespace Relaybox.Samples.Imports.Infrastructure;

/// <summary>Maps the domain to MongoDB without attributes on the entities.</summary>
public static class ImportMappings
{
    public static void Register()
    {
        BsonClassMap.RegisterClassMap<AggregateRoot<Guid>>(map =>
        {
            map.MapIdProperty(a => a.Id).SetSerializer(new GuidSerializer(GuidRepresentation.Standard));
            map.MapProperty(a => a.Version).SetElementName("version");
        });

        BsonClassMap.RegisterClassMap<Import>(map =>
        {
            map.MapProperty(i => i.TenantId).SetElementName("tenantId");
            map.MapProperty(i => i.FileName).SetElementName("fileName");
            map.MapProperty(i => i.ExpectedRows).SetElementName("expectedRows");
            map.MapProperty(i => i.ProcessedRows).SetElementName("processedRows");
            map.MapProperty(i => i.FailedRows).SetElementName("failedRows");
            map.MapProperty(i => i.Status).SetElementName("status").SetSerializer(new EnumSerializer<ImportStatus>(BsonType.String));
            map.MapProperty(i => i.StartedAt).SetElementName("startedAt");
            map.MapProperty(i => i.FinishedAt).SetElementName("finishedAt");
            map.MapProperty(i => i.FailureReason).SetElementName("failureReason");
            map.SetIgnoreExtraElements(true);
        });
    }
}
