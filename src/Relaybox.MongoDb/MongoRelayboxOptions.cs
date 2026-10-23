namespace Relaybox.MongoDb;

/// <summary>Options of the MongoDB integration.</summary>
public sealed class MongoRelayboxOptions
{
    /// <summary>
    /// If set, registers an <c>IMongoClient</c> for this connection string (unless one is already registered).
    /// Transactions require a replica set or sharded cluster.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>Name of the outbox collection inside each tenant database. Default: <c>outbox</c>.</summary>
    public string OutboxCollectionName { get; set; } = "outbox";

    /// <summary>Name of the inbox collection inside each tenant database. Default: <c>inbox</c>.</summary>
    public string InboxCollectionName { get; set; } = "inbox";

    /// <summary>How long published outbox messages are kept (TTL index). Default: 7 days.</summary>
    public TimeSpan OutboxRetention { get; set; } = TimeSpan.FromDays(7);

    /// <summary>
    /// How long processed message ids are kept for deduplication (TTL index).
    /// Must be longer than the longest time a message can be redelivered. Default: 7 days.
    /// </summary>
    public TimeSpan InboxRetention { get; set; } = TimeSpan.FromDays(7);

    /// <summary>
    /// Watch the cluster with a change stream to publish new outbox messages right away.
    /// When disabled, messages are published by reconciliation only. Default: <c>true</c>.
    /// </summary>
    public bool UseChangeStreams { get; set; } = true;

    /// <summary>Maximum time the change stream waits for new events in each round trip. Default: 1 second.</summary>
    public TimeSpan ChangeStreamMaxAwaitTime { get; set; } = TimeSpan.FromSeconds(1);
}
