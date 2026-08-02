# Spec: Idempotent `consume`

Status: ready-for-agent

## Problem Statement

`pgx consume` delivers messages from a broker (RabbitMQ, Kafka) through GraphQL
composition into a sink (stdout, Elasticsearch, webhook, KV). Delivery is
at-least-once today: RabbitMQ redelivers unacked messages on crash or requeue,
Kafka redelivers any message whose offset was not committed before a crash, and
`--on-error strict` explicitly requeues failed messages via `nack(requeue)`.
When a message is redelivered after its document was already written, the sink
writes it again:

- Elasticsearch without `--id-field` indexes a brand-new document every time.
- KV without `--key-field` falls back to a fresh random UUID per write — every
  redelivery is a guaranteed duplicate key.
- Webhook POSTs again; the endpoint may process the same event twice.
- Even with `--id-field` / `--key-field` set, a crash between the sink write and
  the ack leaves no way to know the message was already handled.

An operator who wants "at least once, no duplicates" for their broker → GraphQL
→ sink pipeline currently has no lever to pull.

## Solution

Add an opt-in `--idempotent` flag (and `consume.idempotent` config key) that
makes redelivered messages harmless, combining two layers:

1. **Sink-level idempotence.** When idempotence is on, every sink key is stable
   per message. Elasticsearch documents get `_id` = the explicit `--id-field`
   value when present, otherwise the message id. KV keys become
   `<prefix><key-field>` when present, otherwise `<prefix><message-id>` (no more
   random UUID fallback). Webhook POSTs carry an `Idempotency-Key: <message-id>`
   header. Re-processing the same message then overwrites the same document/key
   instead of creating a duplicate.
2. **Loop-level deduplication.** An in-memory, TTL-bounded cache of recently
   seen message ids. A message whose id is already in the cache is acked and
   skipped before GraphQL composition runs. Ids are recorded only after a
   successful sink write, so a message that failed composition or send is never
   marked as done and its requeued retry still runs.

Message identity is stable across redelivery: Kafka records use the record key
(or `partition:offset`), RabbitMQ messages use the AMQP `message_id` property,
which `listen` now sets when publishing.

When `--idempotent` is off, behavior is unchanged. Default is off.

## User Stories

1. As an operator of a `consume` → Elasticsearch pipeline, I want redelivered
   messages to overwrite the existing document instead of creating a duplicate,
   so that my search index reflects each event exactly once even when my
   consumer crashes between write and ack.
2. As an operator of a `consume` → Redis pipeline, I want each event to land
   under one stable key, so that a redelivery never creates a second cache
   entry with a random UUID.
3. As an operator using `--on-error strict`, I want a message that failed
   composition and was requeued to be processed again on redelivery, so that a
   transient error does not permanently drop the event.
4. As an operator using `--on-error strict`, I want a message whose sink write
   succeeded to be skipped if it is redelivered, so that the retry loop does not
   re-index what already landed.
5. As an operator with a webhook sink, I want each POST to carry an
   `Idempotency-Key` header derived from the message, so that idempotency-aware
   endpoints can deduplicate even if the response is lost.
6. As an operator, I want a single `--idempotent` switch that turns on the
   whole guarantee, so that I do not have to reason about per-sink key
   configuration to avoid duplicates.
7. As an operator, I want idempotence to default to off, so that upgrading pgx
   does not silently change my existing pipeline's behavior.
8. As an operator running many messages per second, I want the dedupe window to
   expire old ids, so that my memory does not grow without bound.
9. As an operator, I want to tune how long message ids are remembered, so that I
   can cover the redelivery window my broker actually uses.
10. As an operator of a Kafka-sourced pipeline, I want dedupe identity to use
    the record key or a stable `partition:offset`, so that redelivered records
    are recognized as the same message.
11. As an operator of a RabbitMQ-sourced pipeline, I want dedupe identity to use
    the AMQP `message_id` property, so that redeliveries are recognized even
    though the delivery tag changes.
12. As an operator of a RabbitMQ-sourced pipeline whose producers do not set
    `message_id`, I want a payload hash fallback, so that the feature still
    works without a producer change.
13. As a user of `listen → RabbitMQ → consume`, I want `listen` to set the AMQP
    `message_id` property when it publishes contract messages, so that the
    end-to-end path gets stable message identity for free.
14. As an operator using explicit `--id-field` on Elasticsearch, I want my
    configured key to keep winning over the message id, so that I keep control
    of the document identity.
15. As an operator using explicit `--key-field` on KV, I want my configured key
    to keep winning, so that existing cache-key conventions still work.
16. As an operator, I want a duplicate to be acked quietly rather than treated
    as an error, so that redeliveries do not trip the strict-mode abort path.
17. As an operator who runs multiple `consume` processes on one queue, I want
    each process's dedupe to work independently, so that the feature needs no
    coordination between replicas (and I know the caveat that the cache is
    per-process).
18. As an operator, I want a freshly restarted consumer to still avoid
    duplicates for ES/KV, so that sink-level idempotence covers the
    restart-between-write-and-ack window that the in-memory cache cannot.
19. As a developer, I want the message id derivation and dedupe cache to be
    pure, unit-testable functions, so that the tricky logic is verified without
    broker infrastructure.
20. As a CI maintainer, I want an end-to-end test that publishes the same
    message twice and asserts one sink artifact, so that the user-visible
    guarantee is verified in the integration pipeline.
21. As an operator, I want clear documentation of the resulting delivery
    semantics, so that I know exactly what `--idempotent` buys me and what it
    does not (webhook end-to-end guarantee depends on the endpoint honoring
    `Idempotency-Key`).

## Implementation Decisions

- **Message identity.** A new `message_id: Option<String>` field on
  `BrokerMessage`, populated by each consumer:
  - Kafka: the record key if present, else `"<partition>:<offset>"` (already
    recoverable from the delivery tag encoding).
  - RabbitMQ: the AMQP `message_id` property if present, else SHA-256 of the
    payload bytes.
  The identity is a decision point, not an implementation detail; the fallback
  order is contract.
- **Producer change.** `listen`'s contract RabbitMQ downstream sets the AMQP
  `message_id` property (a per-event UUID) on publish, so end-to-end
  redeliveries carry a stable identity. In scope.
- **Dedupe layer.** A new small module holding an in-memory, TTL-bounded set of
  seen message ids. Entries expire by age; the default TTL is 900 seconds and is
  configurable. No external state store; per-process memory only.
- **Check placement.** In the consume loop, immediately after receiving a
  message and before query-name resolution / GraphQL composition. A hit results
  in `ack(tag)` and `continue`, logged at debug level, in both lenient and
  strict error modes — a duplicate is not an error.
- **Record placement.** A message id is recorded into the dedupe cache only
  after `sink.send()` succeeds, before `ack(tag)`. A message that fails
  composition or send is never recorded, so `nack(requeue)` retries still run.
  This also closes the within-process crash-between-write-and-ack window.
- **Sink interface.** `ConsumeSink::send` gains the message id (as a parameter)
  so sinks can derive their key; the trait is crate-internal so this is safe.
- **Elasticsearch sink.** Under idempotence, `_id` = the `--id-field` value from
  the composed document when present, else the message id. Without idempotence,
  unchanged (no `_id` when no `--id-field`).
- **KV sink.** Under idempotence, key = `<prefix><key-field>` when the field is
  present, else `<prefix><message-id>`. Without idempotence, unchanged (random
  UUID fallback).
- **Webhook sink.** Under idempotence, each POST carries an
  `Idempotency-Key: <message-id>` header. Endpoint support is the endpoint's
  business.
- **CLI and config surface.** New `--idempotent` boolean and `--dedup-ttl
  <secs>` (default 900) arguments; matching `idempotent` and `dedup_ttl` keys on
  `[connections.<name>.consume]`. Both default to the current non-idempotent
  behavior.
- **stdout sink.** Idempotence is irrelevant (prints twice); no key derivation
  is added. It still participates in the dedupe layer like any sink.

## Testing Decisions

A good test for this feature exercises external behavior: a redelivered message
produces exactly one sink artifact with the derived key; a requeued message that
previously failed is processed again; expired ids no longer suppress
processing. Tests assert what lands in the sink, not how the loop is
implemented.

- **Primary seam — end-to-end script** at the CLI boundary, following the prior
  art of `scripts/test_consume_kv.sh` and `scripts/test_listen_consume_es.sh`,
  added to the existing CI integration job (which already provisions Postgres,
  RabbitMQ, Redis, and Elasticsearch). Scenarios: publish the same contract
  message twice with `--idempotent` and assert exactly one KV key / one ES
  document carrying the derived id; assert the webhook `Idempotency-Key` header
  is present; assert a strict-mode requeued message is still processed after its
  first attempt failed.
- **Unit seams — inline `#[cfg(test)]` modules** in the established style (prior
  art: `src/consumer/kv.rs`'s `extract_key_impl` tests): message-id derivation
  for Kafka (key vs offset) and RabbitMQ (property vs hash fallback); dedupe
  cache record/check and TTL expiry; sink key-fallback derivation for ES `_id`
  and KV keys (with and without explicit fields).

## Out of Scope

- Persistent / crash-surviving dedupe state. The in-memory cache is per-process;
  cross-restart protection for ES/KV comes from sink-level idempotence, and
  webhook's crash window depends on the endpoint honoring `Idempotency-Key`.
- The Elasticsearch bulk buffer's existing at-most-once gap (a flush failure
  after ack can still lose a document). Pre-existing behavior, unchanged.
- Kafka producer-side idempotence (`enable.idempotence`); consume is a consumer.
- Exactly-once across multiple concurrently-running `consume` processes sharing
  a queue — dedupe is per-process by design.
- Making the webhook endpoint itself idempotent.

## Further Notes

- The delivery guarantee offered by `--idempotent` is "at-least-once with
  idempotent sinks ≈ exactly-once" for ES and KV. For webhook it is exactly-once
  only if the endpoint honors the `Idempotency-Key` header; in-process
  redeliveries are covered by the dedupe layer regardless.
- The RabbitMQ payload-hash fallback collapses two genuinely distinct events
  with byte-identical payloads; setting `message_id` (as `listen` now does)
  avoids this.
- The dedupe cache is bounded by TTL, not capacity; operators running very high
  message rates should size `--dedup-ttl` to their broker's redelivery window to
  bound memory.
