# CONTEXT.md — pgx domain glossary

Single-context repo. One glossary, shared by the engineering skills and the codebase.

## Glossary

- **message** — a single record delivered by a broker (`BrokerMessage`). Has a payload, a topic, headers, an optional stable identity (`message_id`), and an opaque delivery tag.
- **consume session** — the module that owns an entire `consume` run behind one interface: the dedupe cache lifecycle, the per-message pipeline, and the broker settle protocol. Callers learn `new` + `run`; the lenient/strict policy and settlement are its implementation.
- **session loop** — the generic module that owns reconnection control flow: given a session factory and a shutdown signal, it applies backoff, give-up, and shutdown-during-backoff. Used by the consume session, listen, and replicate.
- **settlement** — the decision a consume session reaches about a delivered message: ack (processed), nack-requeue (retry later), nack-discard (drop), or abort (fatal, strict mode only).
- **delivery** — the deep module of transport handles shared by the sink seams: one handle per transport — `Webhook {retries}`, `Elasticsearch` (owns the client, bulk buffer, flusher, and `_id` derivation via `doc_id`), `Kafka`, `Rabbitmq`, `Shell` — each hiding retry policy, idempotency keying, and buffering behind a small method.
- **idempotency key** — the `Idempotency-Key` header a webhook delivery adds only when a message has a stable identity (`msg_id`), so the receiver can dedupe redeliveries. Retries are a handle-level budget (consume webhook: 0 retries, notify webhook: 2).
- **replication protocol** — `ReplicationProtocol<S>` owns the logical-replication wire protocol behind one seam: startup, SCRAM auth, temp-slot creation, START_REPLICATION, CopyBoth streaming, and periodic standby feedback. It runs over any `AsyncRead + AsyncWrite`.
- **boundary** — the Begin/Commit messages of a replication stream; the client intercepts them with `parse_pgoutput_boundary` and surfaces them as typed `ReplicationEvent::Begin/Commit`. They never reach the decoder, which only decodes row-level messages.
- **shared progress** — `SharedProgress`, the monotonic applied-LSN (last received `end_lsn`) the stream loop updates and the client reads to send standby feedback.
- **qualified name** — a table's `schema.table` name, produced by the single `qualified_name` helper; used for filters, file names, and table lookup.
- **wal env keys** — the typed `PGX_*` constants (`PGX_OP`, `PGX_SCHEMA`, `PGX_TABLE`, `PGX_LSN`, `PGX_PAYLOAD`, …) that pass WAL event fields to child processes and sinks.
- **sink** — an adapter that receives a fully composed GraphQL document (`ConsumeSink::send(doc, msg_id)`). Implements: stdout, elasticsearch, webhook, kv.
- **composition** — turning a message into a GraphQL document by resolving named queries against PostgreSQL.
