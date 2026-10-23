using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;

namespace Relaybox.MongoDb;

internal sealed class OutboxDocument
{
    public Guid Id { get; set; }
    public string TenantId { get; set; } = "";
    public string Type { get; set; } = "";
    public string Payload { get; set; } = "";
    public DateTime OccurredAt { get; set; }
    public string Status { get; set; } = OutboxStatus.Pending;
    public DateTime? ProcessedAt { get; set; }
    public DateTime? FailedAt { get; set; }
    public DateTime? LockedUntil { get; set; }
    public string? LockedBy { get; set; }
    public int Attempts { get; set; }
    public string? Error { get; set; }
    public string? TraceParent { get; set; }
    public string? Destination { get; set; }
    public string? RoutingKey { get; set; }
    public Dictionary<string, string>? Headers { get; set; }

    public static OutboxDocument From(OutboxMessage message) => new()
    {
        Id = message.Id,
        TenantId = message.TenantId,
        Type = message.Type,
        Payload = message.Payload,
        OccurredAt = message.OccurredAt,
        Attempts = message.Attempts,
        TraceParent = message.TraceParent,
        Destination = message.Destination,
        RoutingKey = message.RoutingKey,
        Headers = message.Headers is null ? null : new Dictionary<string, string>(message.Headers),
    };

    public OutboxMessage ToMessage() =>
        new(Id, TenantId, Type, Payload, OccurredAt, Attempts, TraceParent, Destination, RoutingKey, Headers);
}

internal static class OutboxStatus
{
    public const string Pending = "pending";
    public const string Processed = "processed";
    public const string Failed = "failed";
}

internal sealed class InboxDocument
{
    public string Id { get; set; } = "";
    public string TenantId { get; set; } = "";
    public string Type { get; set; } = "";
    public DateTime ProcessedAt { get; set; }
}

/// <summary>
/// Maps the stored documents with <see cref="BsonClassMap"/>, keeping the public contracts free of MongoDB attributes.
/// </summary>
internal static class MongoMappings
{
    private static readonly object Gate = new();
    private static bool _registered;

    public static void Register()
    {
        lock (Gate)
        {
            if (_registered)
            {
                return;
            }

            BsonClassMap.RegisterClassMap<OutboxDocument>(map =>
            {
                map.MapIdMember(d => d.Id).SetSerializer(new GuidSerializer(GuidRepresentation.Standard));
                map.MapMember(d => d.TenantId).SetElementName("tenantId");
                map.MapMember(d => d.Type).SetElementName("type");
                map.MapMember(d => d.Payload).SetElementName("payload");
                map.MapMember(d => d.OccurredAt).SetElementName("occurredAt");
                map.MapMember(d => d.Status).SetElementName("status");
                map.MapMember(d => d.ProcessedAt).SetElementName("processedAt");
                map.MapMember(d => d.FailedAt).SetElementName("failedAt").SetIgnoreIfNull(true);
                map.MapMember(d => d.LockedUntil).SetElementName("lockedUntil");
                map.MapMember(d => d.LockedBy).SetElementName("lockedBy");
                map.MapMember(d => d.Attempts).SetElementName("attempts");
                map.MapMember(d => d.Error).SetElementName("error");
                map.MapMember(d => d.TraceParent).SetElementName("traceParent").SetIgnoreIfNull(true);
                map.MapMember(d => d.Destination).SetElementName("destination").SetIgnoreIfNull(true);
                map.MapMember(d => d.RoutingKey).SetElementName("routingKey").SetIgnoreIfNull(true);
                map.MapMember(d => d.Headers).SetElementName("headers").SetIgnoreIfNull(true);
            });

            BsonClassMap.RegisterClassMap<InboxDocument>(map =>
            {
                map.MapIdMember(d => d.Id);
                map.MapMember(d => d.TenantId).SetElementName("tenantId");
                map.MapMember(d => d.Type).SetElementName("type");
                map.MapMember(d => d.ProcessedAt).SetElementName("processedAt");
            });

            _registered = true;
        }
    }
}
