# Relaybox

Reliable event publishing (transactional **outbox**) and idempotent consumption (**inbox**) for multi-tenant .NET services on **MongoDB** and **RabbitMQ**, with **one database per tenant**.

Relaybox is a library, not a framework: it does not decide how you write your domain model, your repositories or your endpoints. You keep saving your aggregates the way you do today, and add their events to the outbox in the same transaction. Relaybox makes sure they reach the broker, and that each one is handled once on the other side.

```csharp
using var session = await client.StartSessionAsync();
session.StartTransaction();

await imports.ReplaceOneAsync(session, i => i.Id == import.Id && i.Version == expected, import);
await outbox.AddRangeAsync(session, import.DomainEvents.Select(ToContract));

await session.CommitTransactionAsync(); // both or neither
```

## Guarantees

- **No lost events.** An event is stored if and only if the change that raised it is committed. It stays in the outbox until the broker confirms it (publisher confirms).
- **At-least-once delivery, exactly-once processing.** A message can be published more than once (e.g. an instance dies between the broker confirmation and marking it as published). The consumer inbox records every message id in the same transaction as the handler's writes, so duplicates are discarded. Relaybox never claims exactly-once *delivery*.
- **Safe with many replicas.** Every replica can run the relay. Messages are claimed atomically with a lock, so replicas do not publish the same message twice, and a crashed replica's messages are taken over when its lock expires.
- **Tenant isolation.** Each tenant's outbox and inbox live in its own database. Every message carries a `tenant-id` header, and a message without one goes to the dead letter queue: there is no default tenant.
- **No global ordering.** Messages of a tenant are published oldest first, but retries and concurrent consumers can reorder them. If order matters per aggregate, include its version in the event and ignore stale ones.

## How it works

```mermaid
flowchart LR
    subgraph Producer["API replica (×N)"]
        H[Endpoint / handler] -->|"aggregate + events<br/>one transaction"| DB
        CS[Change stream] -.->|"signal: tenant X"| Q[(Tenant queue)]
        RC[Reconciliation<br/>every minute] -.->|"signal: all tenants"| Q
        Q --> W[Workers]
        W -->|"1. claim (atomic)"| DB
        W -->|"2. publish + confirm"| MQ
        W -->|"3. mark processed"| DB
    end
    DB[("MongoDB<br/>tenant DB: data + outbox")]
    MQ{{RabbitMQ}}
    subgraph Consumer["Consumer replica (×N)"]
        C[Consumer] -->|"tenant-id header → tenant scope"| I["Inbox + handler<br/>one transaction"]
    end
    MQ --> C
    I --> TDB[("MongoDB<br/>tenant DB: data + inbox")]
```

1. **Write.** `IOutbox.AddAsync` inserts the event, serialized as its *contract* with a stable name, into the `outbox` collection of the current tenant's database, inside your transaction. It refuses to run outside a transaction.
2. **Signal.** A cluster-wide MongoDB change stream reports which tenant just got an outbox insert. It is only a hint, never the source of what gets published: every replica sees the same insert. A slow **reconciliation** (polling every tenant) is always on as well. It covers start-up, lost change stream events and expired locks.
3. **Claim and publish.** A bounded pool of workers takes tenants from a de-duplicated queue. Each worker claims the oldest pending message with `FindOneAndUpdate`, publishes it with publisher confirms, and only then marks it processed. Failures back off exponentially via the lock. A broken tenant is logged and skipped; the others keep flowing. Busy tenants yield after a configurable number of messages, so one tenant cannot starve the rest.
4. **Consume.** The consumer reads the `tenant-id` header, opens a DI scope for that tenant, and runs your `IMessageHandler<T>` in a transaction together with the inbox insert (`_id = MessageId`). On a duplicate key, the message is acknowledged without running the handler. Transient errors (connection, write conflicts) are retried; anything else (domain rule violations, bugs, bad payloads) goes to the dead letter queue.

## Getting started

```shell
dotnet add package Relaybox.MongoDb
dotnet add package Relaybox.RabbitMq
```

```csharp
builder.Services.AddSingleton<IMongoClient>(new MongoClient(connectionString)); // replica set required

builder.Services.AddRelaybox()
    .AddTenantSource<MyTenantCatalog>()             // your ITenantSource; cached and refreshed by Relaybox
    .MapMessage<ImportCompletedV1>("imports.completed.v1")
    .AddMongoDb()                                    // outbox, inbox, change stream signal
    .AddRabbitMq(o =>
    {
        o.ConnectionString = "amqp://guest:guest@localhost:5672/";
        o.Exchange = "imports";
    })
    .AddOutboxRelay()                                // background publisher in this process
    .AddMessageHandler<ImportCompletedV1, NotifyImportCompleted>()
    .AddRabbitMqConsumer("imports.notifications");   // queue + dead letter queue
```

What you implement:

| Interface | Purpose |
| --- | --- |
| `ITenantSource` | Returns the active tenants and their database names. Relaybox caches it (`ITenantRegistry`), refreshes it every minute, and reloads early when it meets an unknown tenant. |
| `ITenantContext` *(optional)* | Relaybox ships a scoped default. Register yours first if your app already has one; Relaybox sets it in every scope it creates. |
| `IMessageHandler<T>` | Your consumers. Inject `IMongoSessionAccessor` to write in the inbox transaction. |

### The sample

[`samples/Relaybox.Samples.Imports`](samples/Relaybox.Samples.Imports) is a small API with an `Import` aggregate (rich domain model, optimistic concurrency, domain events mapped to versioned contracts) that consumes its own `imports.completed.v1` events.

```shell
docker compose up -d --wait
dotnet run --project samples/Relaybox.Samples.Imports --launch-profile replica-1
dotnet run --project samples/Relaybox.Samples.Imports --launch-profile replica-2   # optional, second replica
```

Then use [`imports.http`](samples/Relaybox.Samples.Imports/imports.http).

## Packages

| Package | Contents |
| --- | --- |
| `Relaybox.Abstractions` | Contracts: `IOutboxStore`, `IOutboxSignalSource`, `IMessagePublisher`, `IInbox`, `IMessageHandler<T>`, tenants. |
| `Relaybox` | The engine, with no database or broker: relay, reconciliation, tenant registry, inbound dispatcher, serialization, telemetry. |
| `Relaybox.MongoDb` | Outbox and inbox stores, atomic claim, TTL indexes, change stream signal source. |
| `Relaybox.RabbitMq` | Publisher with confirms and a channel pool, tenant-aware consumer with dead letter topology. |

The database and the broker sit behind small interfaces. Supporting another engine means implementing `IOutboxStore`, `IInbox` and, optionally, an `IOutboxSignalSource` (e.g. Postgres `LISTEN/NOTIFY`). Supporting another broker means implementing `IMessagePublisher` plus a consumer that calls `InboundMessageDispatcher`.

## Observability

OpenTelemetry-ready through `ActivitySource` and `Meter` named `Relaybox`:

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(t => t.AddSource(RelayboxDiagnostics.Name))
    .WithMetrics(m => m.AddMeter(RelayboxDiagnostics.Name));
```

| Instrument | Meaning |
| --- | --- |
| `relaybox.outbox.published` | Messages confirmed by the broker, per tenant and type. |
| `relaybox.outbox.publish_latency` | Seconds from the outbox insert to the broker confirmation. |
| `relaybox.outbox.publish_failures` / `relaybox.outbox.attempts` | Failed attempts, and attempts needed per message. |
| `relaybox.relay.tenant_failures` | Relay rounds aborted by an error in a tenant. |
| `relaybox.inbox.consumed` | Inbound messages by outcome: `processed`, `duplicate`, `dead_lettered`, `requeued`. |

The trace context is captured when the event is added to the outbox and propagated through the `traceparent` header, so a consumer's span is linked to the request that raised the event. All logs carry `tenantId` and `messageId`.

## Tested against real infrastructure

The integration tests run with Testcontainers against a real MongoDB replica set and a real RabbitMQ:

- Events of an aborted transaction are never published.
- Three replicas competing for 200 messages publish each one exactly once.
- A message locked by a dead instance is published when its lock expires.
- The broker is paused: messages stay pending with backoff and go out when it returns.
- The change stream publishes immediately, without waiting for reconciliation.
- A tenant created after start-up is picked up.
- A tenant with a broken database does not block the others.
- A redelivered message is discarded by the inbox.
- A message without `tenant-id` goes to the dead letter queue.
- A failing handler leaves no trace (neither its writes nor the inbox record) and is dead-lettered.

```shell
dotnet test   # requires Docker
```

## Configuration highlights

| Option | Default | Notes |
| --- | --- | --- |
| `OutboxRelayOptions.MaxDegreeOfParallelism` | 4 | Tenants processed at once per instance. |
| `OutboxRelayOptions.MaxMessagesPerTenantPerRound` | 100 | Fairness between tenants. |
| `OutboxRelayOptions.LockDuration` | 2 min | Must be much longer than a publication. |
| `OutboxRelayOptions.ReconciliationInterval` | 1 min | Safety net next to the change stream. |
| `MongoRelayboxOptions.OutboxRetention` / `InboxRetention` | 7 days | TTL indexes; changes are applied to existing indexes. Inbox retention must exceed your longest redelivery window. |
| `MongoRelayboxOptions.UseChangeStreams` | `true` | Off: reconciliation only. |
| `RabbitMqOptions.PublishTimeout` | 30 s | An unresponsive broker cannot block the relay. |
| `InboundOptions.MaxAttempts` / `TransientExceptions` | 5 / I/O, timeouts, Mongo transient | Anything not transient is dead-lettered immediately. |

## Design notes

- **Why the change stream is only a signal.** With N replicas, every one receives the same change event. Publishing from the stream would require leader election or partitioning. Using it to wake up the claim-based relay keeps the low latency and makes replicas safe for free.
- **Why the resume token is not persisted.** The stream resumes from its in-memory token when it is reopened inside a process. After a restart, the start-up reconciliation already checks every tenant, so a persisted token would add a collection and a failure mode without closing any gap. When the stream cannot resume (history lost), every tenant is signaled.
- **Contracts, not domain classes.** Events are serialized with a stable name (`MapMessage<T>("name")`). Map your domain events to versioned contracts before adding them to the outbox, so refactoring your domain never breaks consumers.

## Alternatives

- **MassTransit**: a complete messaging framework with outbox support; it moved to a commercial license with v9.
- **DotNetCore.CAP** (MIT): outbox/inbox with many storages and brokers, MongoDB and RabbitMQ included. It owns the subscriber model (attribute-based), and one database per tenant is not its primary scenario.
- **Wolverine** (MIT): a full messaging and mediator framework with durable inbox/outbox, built around its own handler model.
- **Debezium**: generic change data capture in Java, running on Kafka Connect or Debezium Server, outside your process.

Choose Relaybox if you want only the reliability part (outbox, relay, inbox), inside your own process, with MongoDB, one database per tenant, and your own handler and domain model.

## Out of scope (for now)

Exactly-once delivery, sagas and state machines, global ordering, and brokers or databases other than RabbitMQ and MongoDB. The abstractions are ready for more engines, but none are implemented yet.

## Requirements

.NET 8+, a MongoDB replica set or sharded cluster (transactions and change streams), and RabbitMQ 3.x+.

## License

[MIT](LICENSE)
