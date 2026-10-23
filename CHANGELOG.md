# Changelog

All notable changes to this project are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- Per-message destination, routing key and headers (`OutboxMessageOptions`).
- Poison message handling: outbox messages are `pending`, `processed` or `failed`. Broker rejections are retried up to `MaxAttempts`; permanent rejections (missing exchange, message too large) fail at once; an unreachable broker never fails messages.
- `MessageRejectedException` for publishers to tell rejections apart from connectivity problems.
- Per-tenant gauges: `relaybox.outbox.pending`, `relaybox.outbox.failed`, `relaybox.outbox.oldest_pending_age`.
- Change stream gauge and counter: `relaybox.changestream.connected`, `relaybox.changestream.restarts`.
- Health check `AddRelayboxOutbox()`: unhealthy when messages wait too long, degraded when the change stream is down or a tenant's outbox cannot be read.

### Changed
- `IOutboxStore.MarkFailedAsync` now gives up on a message; retries are scheduled with `ScheduleRetryAsync`. `IOutboxStore` gained `GetStatisticsAsync`.
- The outbox claim index is now `{ status, occurredAt }` (`relaybox_claim`).

## [0.1.0]

### Added
- Transactional outbox for MongoDB with one database per tenant.
- Outbox relay: atomic claim, publisher confirms, exponential backoff, bounded parallelism across tenants, per-tenant fairness and error isolation.
- Change stream signal source and always-on reconciliation.
- Inbox with `_id = MessageId` in the handler's transaction, tenant-aware RabbitMQ consumer with dead letter queue.
- OpenTelemetry traces and metrics.
