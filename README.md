# pgx — PostgreSQL Power CLI

A feature-rich PostgreSQL CLI tool — beyond psql.

## Features

| Command     | Description                                                       |
| ----------- | ----------------------------------------------------------------- |
| `query`     | Run SQL and display results as a table or JSON                    |
| `export`    | Export SQL results to Excel / CSV / JSON                          |
| `info`      | Show server version, databases, tables, connections               |
| `listen`    | Subscribe to NOTIFY channels and forward to downstream sinks      |
| `replicate` | Stream WAL changes via logical replication (INSERT/UPDATE/DELETE) |
| `graphql`   | Validate and run named GraphQL queries with batched SQL resolvers |

---

## Installation

```bash
# Default build (Excel + RabbitMQ + Webhook enabled)
cargo build --release

# With Kafka support (requires librdkafka system library)
cargo build --release --features kafka

# Minimal build (shell downstream only, no Excel)
cargo build --release --no-default-features

# Without Excel export
cargo build --release --no-default-features --features rabbitmq,webhook
```

The binary is placed at `target/release/pgx`.

---

## Connection

```bash
# Via URL flag
pgx -U postgres://user:pass@localhost:5432/mydb <command>

# Via environment variable
export DATABASE_URL=postgres://user:pass@localhost:5432/mydb
pgx <command>

# Via named profile in ~/.pgx/config.toml
pgx -c myprofile <command>
```

### ~/.pgx/config.toml

```toml
default = "local"

[connections.local]
url = "postgres://postgres:postgres@localhost:5432/mydb"
description = "Local dev database"

[connections.staging]
url = "postgres://user:pass@staging-host:5432/mydb"
```

---

## replicate — PostgreSQL Logical Replication

Stream every INSERT, UPDATE, DELETE, and TRUNCATE directly from the WAL — no
application changes needed. Uses a self-contained implementation of the
PostgreSQL replication wire protocol (no libpq, no external replication crate).

### Comparison: `listen` vs `replicate`

|            | `listen`                            | `replicate`                                |
| ---------- | ----------------------------------- | ------------------------------------------ |
| Source     | Explicit `pg_notify()` calls        | Any INSERT / UPDATE / DELETE automatically |
| Payload    | Whatever the app puts in the NOTIFY | Full row images, before + after            |
| Setup      | None                                | `wal_level=logical` + publication          |
| Durability | At-most-once                        | Exactly-once via replication slot          |
| Resume     | No                                  | Yes — stores LSN checkpoint in slot        |

> **Note:** `replicate` always emits full WAL event JSON. The contract
> routing metadata (custom exchange, topic, headers) available in `listen` sinks
> is driven by application-layer `pg_notify()` payloads and is not available
> in the replication stream.

### PostgreSQL prerequisites

```sql
-- 1. Set in postgresql.conf, then restart:
wal_level = logical

-- 2. Grant the replication role to your user:
ALTER USER myuser REPLICATION;

-- 3. Create a publication (choose which tables to capture):
CREATE PUBLICATION my_pub FOR TABLE orders, inventory;

-- Or capture every table in the database:
CREATE PUBLICATION my_pub FOR ALL TABLES;
```

### Downstream: stdout

Best for debugging or piping to `jq`.

```bash
pgx -U $DATABASE_URL replicate \
  --slot pgx_slot \
  --publication my_pub \
  stdout --pretty
```

**Example output:**

```json
{
  "op": "insert",
  "rel_id": 16391,
  "schema": "public",
  "table": "orders",
  "new": {
    "id": "42",
    "customer": "Alice",
    "status": "pending",
    "total": "99.95"
  }
}
```

### Downstream: shell

```bash
pgx -U $DATABASE_URL replicate \
  --slot pgx_slot \
  --publication my_pub \
  shell \
  --command 'echo "[$PGX_OP] $PGX_SCHEMA.$PGX_TABLE new=$PGX_NEW"'
```

**Environment variables available in the shell command:**

| Variable      | Description                                                             |
| ------------- | ----------------------------------------------------------------------- |
| `PGX_OP`      | `insert`, `update`, `delete`, `truncate`, `begin`, `commit`, `relation` |
| `PGX_SCHEMA`  | Schema name (DML events)                                                |
| `PGX_TABLE`   | Table name (DML events)                                                 |
| `PGX_LSN`     | WAL position of this event (e.g. `0/1A2B3C`)                            |
| `PGX_XID`     | Transaction ID (BEGIN events, requires `--emit-txn-boundaries`)         |
| `PGX_NEW`     | JSON of new row values (INSERT / UPDATE)                                |
| `PGX_OLD`     | JSON of old row values (UPDATE / DELETE)                                |
| `PGX_PAYLOAD` | Full event JSON                                                         |

### Downstream: webhook

```bash
pgx -U $DATABASE_URL replicate \
  --slot pgx_slot \
  --publication my_pub \
  --op insert --op update \
  webhook \
  --url https://example.com/hooks/wal \
  --header "Authorization=Bearer mytoken"
```

The full event JSON is POSTed as the body with `Content-Type: application/json`.

### Downstream: RabbitMQ

```bash
pgx -U $DATABASE_URL replicate \
  --slot pgx_slot \
  --publication my_pub \
  rabbitmq \
  --amqp-url amqp://guest:guest@localhost:5672/%2F \
  --exchange wal-events \
  --routing-key pgx.wal
```

AMQP headers `pgx-op`, `pgx-schema`, `pgx-table`, `pgx-lsn` are injected automatically.

### Downstream: Kafka

```bash
pgx -U $DATABASE_URL replicate \
  --slot pgx_slot \
  --publication my_pub \
  kafka \
  --brokers localhost:9092 \
  --topic pgx-wal
```

The Kafka message key is set to `schema.table` so events naturally partition by table.

---

### Filtering

```bash
# Only inserts and updates on the orders table
pgx -U $DATABASE_URL replicate \
  --slot pgx_slot --publication my_pub \
  --table public.orders \
  --op insert --op update \
  stdout --pretty

# Also emit BEGIN / COMMIT transaction boundaries
pgx -U $DATABASE_URL replicate \
  --slot pgx_slot --publication my_pub \
  --emit-txn-boundaries \
  stdout --pretty

# Also emit RELATION (schema) events
pgx -U $DATABASE_URL replicate \
  --slot pgx_slot --publication my_pub \
  --emit-schema \
  stdout --pretty
```

### Slot management

| Flag                 | Description                                                       |
| -------------------- | ----------------------------------------------------------------- |
| `--slot <name>`      | Slot name (default: `pgx_slot`). Created automatically if absent. |
| `--reset-slot`       | Drop and recreate the slot. **Loses acknowledged progress.**      |
| `--temporary`        | Create a temporary slot — dropped when the session ends.          |
| `--start-lsn <A/BB>` | Resume from a specific WAL position.                              |

### Reconnection & retry

When the PostgreSQL connection or replication stream breaks (server restart,
network flap, etc.), `pgx` automatically reconnects with exponential backoff:

| Flag                           | Description                                                     | Default |
| ------------------------------ | --------------------------------------------------------------- | ------- |
| `--max-reconnect-attempts <N>` | Max consecutive failures before giving up. `0` = retry forever. | `0`     |
| `--reconnect-base-ms <N>`      | Initial backoff delay in milliseconds (doubles each attempt).   | `1000`  |
| `--reconnect-max-ms <N>`       | Cap on the backoff delay.                                       | `60000` |

The backoff for attempt _n_ is `base × 2ⁿ⁻¹` with ±20% jitter, capped at
`reconnect_max_ms`. The streaming position resumes from the last confirmed
LSN, so no events are lost or duplicated.

These settings can also be configured per-connection in `~/.pgx/config.toml`:

```toml
[connections.myconn.replicate]
max_reconnect_attempts = 20
reconnect_base_ms = 500
reconnect_max_ms = 30000
```

---

### Understanding column values in old rows

PostgreSQL's WAL contains three distinct states for each column in old-row tuples.
`pgx` represents them precisely:

| JSON value             | Meaning                                   |
| ---------------------- | ----------------------------------------- |
| `"alice"`              | The actual SQL value                      |
| `null`                 | The column is SQL NULL                    |
| `{"$unchanged": true}` | Column not sent by the server (see below) |

The `{"$unchanged": true}` marker appears because under the default `REPLICA IDENTITY DEFAULT`,
PostgreSQL only includes the primary key column(s) in old-row tuples. All other
columns receive the `'u'` (unchanged/not-sent) tag in the WAL.

**To get full old-row values**, run once per table:

```sql
ALTER TABLE public.orders REPLICA IDENTITY FULL;
```

With `REPLICA IDENTITY FULL`, every column in the old tuple is sent with its actual
value, and `{"$unchanged": true}` will never appear.

**What you see per operation:**

| Operation    | `REPLICA IDENTITY DEFAULT`                              | `REPLICA IDENTITY FULL`      |
| ------------ | ------------------------------------------------------- | ---------------------------- |
| INSERT `old` | absent                                                  | absent (there is no old row) |
| UPDATE `old` | `null` when no key col changed; key cols only otherwise | all columns                  |
| DELETE `old` | key cols only; rest are `{"$unchanged": true}`          | all columns                  |

---

### Event JSON schema reference

```jsonc
// INSERT — all new columns always present
{ "op": "insert", "rel_id": 16391, "schema": "public", "table": "orders",
  "new": { "id": "42", "status": "pending", "total": "99.95" } }

// UPDATE — old is null when no replica-identity column changed
{ "op": "update", "rel_id": 16391, "schema": "public", "table": "orders",
  "old": null,
  "new": { "id": "42", "status": "shipped", "total": "99.95" } }

// UPDATE with REPLICA IDENTITY FULL — full before image
{ "op": "update", "rel_id": 16391, "schema": "public", "table": "orders",
  "old": { "id": "42", "status": "pending", "total": "99.95" },
  "new": { "id": "42", "status": "shipped", "total": "99.95" } }

// DELETE — non-key columns are {"$unchanged": true} under DEFAULT identity
{ "op": "delete", "rel_id": 16391, "schema": "public", "table": "orders",
  "old": { "id": "42", "status": {"$unchanged": true}, "total": {"$unchanged": true} } }

// DELETE with REPLICA IDENTITY FULL — full before image
{ "op": "delete", "rel_id": 16391, "schema": "public", "table": "orders",
  "old": { "id": "42", "status": "shipped", "total": "99.95" } }

// TRUNCATE
{ "op": "truncate", "rel_ids": [16391], "tables": ["public.orders"],
  "cascade": false, "restart_seqs": false }

// BEGIN (requires --emit-txn-boundaries)
{ "op": "begin", "lsn": "0/1A2B3C", "commit_time": 759638400000000, "xid": 742 }

// COMMIT (requires --emit-txn-boundaries)
{ "op": "commit", "lsn": "0/1A2B40", "end_lsn": "0/1A2B68", "commit_time": 759638400000000 }
```

---

## listen — PostgreSQL NOTIFY → Downstream

Subscribe to one or more NOTIFY channels and forward every notification to a
downstream sink. Unlike `replicate`, this requires the application to call
`pg_notify()` explicitly.

> **Delivery:** at-most-once. If the process exits or crashes between receiving
> a NOTIFY and forwarding it to the downstream, the event is lost. Use
> `replicate` for exactly-once delivery via WAL slots.

### Two forwarding modes

| Mode       | Description                                                                        |
| ---------- | ---------------------------------------------------------------------------------- |
| `simple`   | Pass the raw NOTIFY payload as the message body                                    |
| `contract` | Parse the payload as a structured `ContractMessage` and use embedded routing hints |

### Downstream: RabbitMQ

```bash
# Simple mode — fixed exchange + routing key
pgx -U $DATABASE_URL listen \
  -C orders \
  rabbitmq \
  --amqp-url amqp://guest:guest@localhost:5672/%2F \
  --exchange events \
  --routing-key order.notify \
  --mode simple

# Contract mode — exchange/routing-key/headers driven by the payload
pgx -U $DATABASE_URL listen \
  -C orders -C inventory \
  rabbitmq \
  --amqp-url amqp://guest:guest@localhost:5672/%2F \
  --exchange events \
  --routing-key default.notify \
  --mode contract
```

**Contract payload example** (sent via `pg_notify('orders', '...')`):

```json
{
  "meta": {
    "routing": {
      "rabbitmq_exchange": "orders",
      "rabbitmq_routing_key": "order.created",
      "rabbitmq_headers": { "x-priority": "1", "x-tenant": "acme" }
    },
    "schema_version": "1",
    "event_type": "order.created"
  },
  "data": { "order_id": 42, "customer": "Alice", "total": 99.95 }
}
```

### Downstream: Kafka

```bash
pgx -U $DATABASE_URL listen \
  -C orders \
  kafka \
  --brokers localhost:9092 \
  --topic pgx-notify \
  --mode simple
```

### Downstream: Webhook

```bash
pgx -U $DATABASE_URL listen \
  -C alerts \
  webhook \
  --url https://example.com/hooks/alerts \
  --header "Authorization=Bearer mytoken" \
  --mode simple
```

### Downstream: Shell

```bash
pgx -U $DATABASE_URL listen \
  -C deployments \
  shell \
  --command 'echo "[$PGX_CHANNEL] $PGX_PAYLOAD" >> /var/log/pg_notify.log' \
  --mode simple
```

In contract mode:

| Variable             | Source                                |
| -------------------- | ------------------------------------- |
| `PGX_CHANNEL`        | NOTIFY channel name                   |
| `PGX_PID`            | Sending backend PID                   |
| `PGX_PAYLOAD`        | Business data JSON (the `data` field) |
| `PGX_EVENT_TYPE`     | `meta.event_type`                     |
| `PGX_SCHEMA_VERSION` | `meta.schema_version`                 |
| _custom_             | Any keys in `meta.routing.shell_env`  |

---

## consume — Message Broker → GraphQL → Sink

Consume messages from a broker (RabbitMQ, Kafka), compose them through GraphQL
with batched SQL resolvers, and forward the result to a sink. This enables a
CDC → enrichment → indexed document pipeline.

### Sources

```bash
# RabbitMQ
pgx -U $DATABASE_URL consume \
  --source rabbitmq \
  --amqp-url amqp://guest:guest@localhost:5672/%2F \
  --queue pgx-events \
  --sink stdout

# Kafka
pgx -U $DATABASE_URL consume \
  --source kafka \
  --brokers localhost:9092 \
  --topic pgx-events \
  --group-id pgx \
  --sink stdout
```

### Query modes

| Mode       | Description                                                                     |
| ---------- | ------------------------------------------------------------------------------- |
| `contract` | Query name derived from `meta.event_type` in the ContractMessage payload        |
| `simple`   | Fixed query name specified via `--query`                                        |

### Sinks

#### stdout

Prints the composed GraphQL document as JSON to stdout.

```bash
pgx -U $DATABASE_URL consume \
  --source rabbitmq \
  --queue pgx-events \
  --sink stdout
```

#### elasticsearch

Indexes the composed document into Elasticsearch.

```bash
pgx -U $DATABASE_URL consume \
  --source rabbitmq \
  --queue pgx-events \
  --sink elasticsearch \
  --es-url http://localhost:9200 \
  --index materials \
  --id-field mat_no
```

#### webhook

POSTs the composed document as JSON to an HTTP endpoint.

```bash
pgx -U $DATABASE_URL consume \
  --source rabbitmq \
  --queue pgx-events \
  --sink webhook \
  --webhook-url https://hooks.example.com/events
```

#### kv (Redis / Memcached)

Stores the composed document as a JSON value in a key-value store. The cache key
is derived from a field in the document.

```bash
# Redis
pgx -U $DATABASE_URL consume \
  --source rabbitmq \
  --queue pgx-events \
  --sink kv \
  --kv-url redis://localhost:6379 \
  --key-field mat_no \
  --key-prefix pgx: \
  --ttl 3600

# Memcached
pgx -U $DATABASE_URL consume \
  --source rabbitmq \
  --queue pgx-events \
  --sink kv \
  --kv-url memcached://localhost:11211 \
  --key-field id \
  --key-prefix session:
```

| Flag              | Description                                           | Default    |
| ----------------- | ----------------------------------------------------- | ---------- |
| `--kv-url`        | KV store URL (`redis://...` or `memcached://...`)     | `redis://localhost:6379` |
| `--key-field`     | Document field whose value becomes the cache key      | auto-generates UUID |
| `--key-prefix`    | String prepended to the cache key                     | `pgx:`    |
| `--ttl`           | Time-to-live in seconds (`0` = no expiry)             | `0`       |

The key is constructed as `{key_prefix}{value_of_key_field}`. If `key_field` is
not set or the field is missing, a random UUID is used as the suffix.

---

## Other commands

```bash
# Run a query and display as table
pgx -U $DATABASE_URL query -q "SELECT * FROM users LIMIT 10"

# Run a query and get JSON output
pgx -U $DATABASE_URL query -q "SELECT count(*) FROM orders" --json

# Export to Excel
pgx -U $DATABASE_URL export -q "SELECT * FROM orders" -o orders.xlsx

# Export to CSV
pgx -U $DATABASE_URL export -q "SELECT * FROM orders" -m csv -o orders.csv

# Multi-sheet Excel from a .sql file (each `-- sheet:` starts a new sheet)
pgx -U $DATABASE_URL export -f reports.sql -o report.xlsx
# reports.sql:
#   -- sheet: Users
#   SELECT id, username, email FROM users;
#   -- sheet: Orders
#   SELECT id, total, status FROM orders;

# Server info
pgx -U $DATABASE_URL info --version --databases --tables
```

---

## Architecture

```
src/
├── main.rs                        # CLI entry-point, command dispatch
├── commands/
│   ├── replicate.rs               # `replicate` command + all downstream sinks
│   ├── listen.rs                  # `listen` command
│   ├── export.rs
│   ├── query.rs
│   └── info.rs
├── replication/                   # Self-contained logical replication implementation
│   ├── client.rs                  # ReplicationClient — TCP, auth, streaming, keepalives
│   ├── decoder.rs                 # pgoutput binary → WalEvent parser
│   ├── event.rs                   # WalEvent enum + ColVal (Text/Null/Unchanged)
│   ├── framing.rs                 # PostgreSQL wire protocol read/write helpers
│   ├── lsn.rs                     # Lsn type (parse, display, arithmetic)
│   ├── messages.rs                # Auth message parsing, error response parsing
│   ├── proto.rs                   # CopyData parsing, StandbyStatusUpdate encoding
│   ├── scram.rs                   # SCRAM-SHA-256 authentication
│   ├── error.rs                   # ReplError / ReplResult
│   └── slot.rs                    # Slot management via tokio-postgres
├── downstream/                    # listen command downstream sinks
│   ├── sink.rs                    # Downstream trait
│   ├── contract.rs                # NotifyEvent, ContractMessage
│   ├── rabbitmq.rs
│   ├── kafka.rs
│   ├── webhook.rs
│   └── shell.rs
└── utils/
    ├── config.rs                  # ~/.pgx/config.toml
    └── ...
```

### Replication data flow

```
PostgreSQL (WAL)
    │  TCP  (replication protocol)
    ▼
src/replication/client.rs          startup → auth (SCRAM/cleartext) → START_REPLICATION
    │                              periodic StandbyStatusUpdate keepalives
    │  ReplicationEvent::XLogData { data }   ← raw pgoutput bytes
    │  ReplicationEvent::Begin / Commit      ← transaction boundaries
    │  ReplicationEvent::KeepAlive           ← acknowledged internally
    ▼
src/replication/decoder.rs         decode_pgoutput(data) → WalEvent
    │
    │  WalEvent::Insert / Update / Delete / Relation / Truncate
    ▼
src/commands/replicate.rs          filter (--table, --op) → log → forward
    │
    ▼
stdout / shell / webhook / rabbitmq / kafka
```

---

## Cargo features

| Feature    | Default | Enables                                              |
| ---------- | ------- | ---------------------------------------------------- |
| `excel`    | ✅      | Excel (.xlsx) export via `rust_xlsxwriter`           |
| `rabbitmq` | ✅      | RabbitMQ downstream via `lapin`                      |
| `webhook`  | ✅      | HTTP webhook downstream via `reqwest`                |
| `kafka`    | ❌      | Kafka downstream via `rdkafka` (requires librdkafka) |
| `tls`      | ❌      | TLS for the tokio-postgres control-plane connection  |
| `kv`       | ✅      | Redis / Memcached key-value store sink               |
