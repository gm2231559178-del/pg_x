---
name: pgx-cli
description: Use this skill whenever the user wants to use, configure, or troubleshoot pgx (also called pg_x) — the Rust "PostgreSQL Power CLI" — for running queries, exporting data, streaming NOTIFY events, streaming WAL via logical replication, composing GraphQL over SQL resolvers, or running it as an MCP server. Trigger on mentions of pgx/pg_x, PostgreSQL logical replication CLI, `pgx replicate`, `pgx listen`, `pgx consume`, `pgx graphql`, `pgx mcp`, WAL-to-Kafka/RabbitMQ/Elasticsearch/Parquet pipelines, NOTIFY-to-downstream forwarding, or `~/.pgx/config.toml`. Covers every downstream sink (stdout, shell, webhook, RabbitMQ, Kafka, Parquet, Postgres, Elasticsearch, KV/Redis/Memcached) and how to wire data from Postgres to each one, both via CLI flags and via config-file profiles.
---

# pgx — PostgreSQL Power CLI

`pgx` is a single Rust binary that goes beyond `psql`: it runs queries, exports
data, forwards `NOTIFY` events, streams WAL via logical replication, composes
GraphQL over batched SQL resolvers, and can run as an MCP server for AI
assistants. This skill is a usage reference — how to invoke each command, how
to wire data to each downstream sink, and how to express the same setup as a
reusable config profile.

## Mental model: three ways to move data out of Postgres

| Command     | Trigger                                           | Payload                                           | Delivery guarantee                      | Setup                             |
| ----------- | ------------------------------------------------- | ------------------------------------------------- | --------------------------------------- | --------------------------------- |
| `listen`    | App calls `pg_notify()`                           | Whatever the app put in the NOTIFY                | At-most-once                            | None                              |
| `replicate` | Any INSERT/UPDATE/DELETE/TRUNCATE                 | Full before/after row images                      | Exactly-once (replication slot resumes) | `wal_level=logical` + publication |
| `consume`   | A broker message (from `listen`, or any producer) | Broker payload enriched via GraphQL/SQL resolvers | Depends on broker                       | Broker + resolver config          |

Pick `replicate` when you want a database-level CDC feed with no app changes.
Pick `listen` when the app already knows what changed and wants to route it
(e.g. attach custom headers/exchange per event). Pick `consume` when the
event needs to be joined against other tables before it's useful downstream
(e.g. building a denormalized search document).

## Connecting

Three interchangeable ways to point `pgx` at a database, in precedence order:

1. `pgx -U postgres://user:pass@host:5432/db <command>` — explicit flag
2. `export DATABASE_URL=postgres://user:pass@host:5432/db` — environment variable
3. `pgx -c myprofile <command>` — named profile from `~/.pgx/config.toml`

Using a profile (`-c`) also pulls in any saved `listen`/`replicate`/`consume`
settings for that connection, so a fully-configured profile can be run as
just `pgx -c prod replicate` with zero other flags. See the config reference
below.

---

## `replicate` — WAL streaming to a downstream sink

### Prerequisites (one-time, per database)

- `wal_level = logical` in `postgresql.conf` (requires restart)
- `ALTER USER myuser REPLICATION;`
- `CREATE PUBLICATION my_pub FOR TABLE orders, inventory;` (or `FOR ALL TABLES`)

### Event shape

Every event is `{"op": "insert"|"update"|"delete"|"truncate"|"begin"|"commit"|"relation", "schema": ..., "table": ..., "new": {...}, "old": {...}}`.

- `old` is only populated on UPDATE/DELETE.
- Under the default `REPLICA IDENTITY DEFAULT`, non-key columns in `old` show
  as `{"$unchanged": true}` rather than their real value. Run
  `ALTER TABLE public.orders REPLICA IDENTITY FULL;` once per table if you
  need the full before-image on every UPDATE/DELETE.
- `--emit-txn-boundaries` adds `begin`/`commit` events with `lsn` and `xid`.
- `--emit-schema` adds `relation` (schema) events.

### Universal flags (apply before the sink subcommand)

| Flag                                                                    | Purpose                                                             |
| ----------------------------------------------------------------------- | ------------------------------------------------------------------- |
| `--slot <name>`                                                         | Replication slot name (default `pgx_slot`); auto-created            |
| `--publication <name>`                                                  | Which publication to stream                                         |
| `--table <schema.table>`                                                | Restrict to specific table(s), repeatable                           |
| `--op insert/update/delete`                                             | Restrict to specific operations, repeatable                         |
| `--reset-slot`                                                          | Drop and recreate the slot (loses resume position)                  |
| `--temporary`                                                           | Ephemeral slot, dropped at session end                              |
| `--start-lsn <A/BB>`                                                    | Resume from an explicit WAL position                                |
| `--max-reconnect-attempts`, `--reconnect-base-ms`, `--reconnect-max-ms` | Exponential-backoff reconnect tuning (`0` attempts = retry forever) |

### Downstream sink patterns

Every pattern below is `pgx -U $DATABASE_URL replicate --slot pgx_slot --publication my_pub <filters> <sink> <sink-flags>`.

**stdout** — debugging / piping to `jq`:
`stdout --pretty`

**shell** — run a command per event, data passed as environment variables:
`shell --command 'echo "[$PGX_OP] $PGX_SCHEMA.$PGX_TABLE new=$PGX_NEW"'`
Available vars: `PGX_OP`, `PGX_SCHEMA`, `PGX_TABLE`, `PGX_LSN`, `PGX_XID`
(BEGIN events, requires `--emit-txn-boundaries`), `PGX_NEW`, `PGX_OLD`,
`PGX_PAYLOAD` (full event JSON).

**webhook** — POST the full event JSON:
`webhook --url https://example.com/hooks/wal --header "Authorization=Bearer mytoken"`

**RabbitMQ** — AMQP publish, with `pgx-op`/`pgx-schema`/`pgx-table`/`pgx-lsn`
headers injected automatically:
`rabbitmq --amqp-url amqp://guest:guest@localhost:5672/%2F --exchange wal-events --routing-key pgx.wal`

**Kafka** — message key set to `schema.table` so events partition naturally by table (requires `--features kafka`):
`kafka --brokers localhost:9092 --topic pgx-wal`

**Parquet** — Hive-partitioned files per table (`schema/table/year=/month=/day=/part-*.parquet`), plus `_pgx_op`, `_pgx_lsn`, `_pgx_old` metadata columns:
`parquet --output-dir ./wal_archive --max-rows 50000 --compression zstd`

**Postgres** (fan-out replica) — apply changes to a second Postgres instance, batched:
sink `type = "postgres"` with `target_url` and `batch_size` (config-file only — see below).

### Fan-out to multiple sinks

A single stream can be fanned out to more than one sink at once by combining
a primary `sink` with one or more `additional_sinks` entries in the config
file (not available as a bare CLI flag) — e.g. apply to a Postgres replica
**and** forward to Kafka simultaneously. See the config reference below.

### Row-level filtering and shaping (config-file only)

- `filters = ["public.orders:status = 'active'", "amount > 100"]` — SQL-like predicate per table
- `drop_cols = ["public.orders:internal_note,secret_flag"]` — strip sensitive columns before emitting
- `rename = ["public.orders:order_id=id,customer_name=name"]` — rename columns in the emitted JSON

---

## `listen` — NOTIFY forwarding

Requires the application to call `pg_notify(channel, payload)` explicitly.
Delivery is at-most-once (use `replicate` if you need exactly-once).

### Two modes

| Mode       | Behavior                                                                                                                                                      |
| ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `simple`   | Raw NOTIFY payload is passed through as-is                                                                                                                    |
| `contract` | Payload is parsed as a structured `ContractMessage`; routing hints inside the payload (exchange, routing key, headers, event type) drive delivery per-message |

**Contract payload shape** (what the app sends via `pg_notify`):

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

### Downstream sink patterns

**RabbitMQ**, simple mode (fixed exchange/routing key):
`pgx -U $DATABASE_URL listen -C orders rabbitmq --amqp-url amqp://guest:guest@localhost:5672/%2F --exchange events --routing-key order.notify --mode simple`

**RabbitMQ**, contract mode (per-message routing from the payload):
`pgx -U $DATABASE_URL listen -C orders -C inventory rabbitmq --amqp-url amqp://guest:guest@localhost:5672/%2F --exchange events --routing-key default.notify --mode contract`

> **RabbitMQ prerequisite:** The exchange and queue must exist before pgx
> publishes. Create them with `rabbitmqadmin` or the management UI:
> ```
> rabbitmqadmin declare exchange name=events type=topic durable=true
> rabbitmqadmin declare queue name=pgx-events durable=true
> rabbitmqadmin declare binding source=events destination=pgx-events routing_key="default.notify"
> ```
> pgx does not auto-create exchanges or queues — it will fail with
> `NOT_FOUND - no exchange '<name>' in vhost '/'` if they don't exist.

**Kafka**: `pgx -U $DATABASE_URL listen -C orders kafka --brokers localhost:9092 --topic pgx-notify --mode simple`

**Webhook**: `pgx -U $DATABASE_URL listen -C alerts webhook --url https://example.com/hooks/alerts --header "Authorization=Bearer mytoken" --mode simple`

**Shell**: `pgx -U $DATABASE_URL listen -C deployments shell --command 'echo "[$PGX_CHANNEL] $PGX_PAYLOAD" >> /var/log/pg_notify.log' --mode simple`
In contract mode, shell also gets `PGX_EVENT_TYPE`, `PGX_SCHEMA_VERSION`, and
any custom keys placed in `meta.routing.shell_env`.

**Elasticsearch** (contract mode, resolves a GraphQL query by `event_type` and indexes the composed document directly — see the full example below):
`pgx listen -C materials elasticsearch --index materials --id-field mat_no`

### Backpressure control

`--channel-full-behavior block|drop_oldest|grow` controls what happens when
the downstream can't keep up: `block` propagates backpressure to Postgres
(safest, can stall NOTIFYs), `drop_oldest` (default) never blocks but loses
messages, `grow` never drops but can OOM under sustained load.

---

## `consume` — Broker → GraphQL enrichment → Sink

Reads a message from a broker, composes it through GraphQL with batched SQL
resolvers (avoiding N+1 queries), and forwards the enriched document to a
sink. This is the pattern for turning a thin CDC event (e.g. `{"mat_no": "M001"}`)
into a fully joined document before indexing it.

### Sources

**RabbitMQ**: `--source rabbitmq --amqp-url amqp://guest:guest@localhost:5672/%2F --queue pgx-events`
**Kafka**: `--source kafka --brokers localhost:9092 --topic pgx-events --group-id pgx`

> **RabbitMQ prerequisite:** The queue must exist before pgx connects. If
> publishing via `listen` → RabbitMQ → `consume`, also create the exchange
> and binding first (see `listen` section above).

### Query modes

| Mode       | Behavior                                                            |
| ---------- | ------------------------------------------------------------------- |
| `contract` | GraphQL query name is taken from `meta.event_type` in the message   |
| `simple`   | Fixed query name given via `--query`, raw payload used as variables |

**`--query` flag:** In `contract` mode, the query name is resolved from
`meta.event_type` in each message — no `--query` needed. In `simple` mode,
`--query <name>` is required and specifies which GraphQL query to execute
against the schema in `~/.pgx/queries/`. Example: `pgx consume --source rabbitmq --queue pgx-events --query material --sink stdout`.

### Resolvers (the join logic)

Resolvers map a GraphQL field to a batched SQL query, defined once in
`~/.pgx/config.toml`:

```toml
[resolvers.material]
sql = "SELECT mat_no, name, description FROM materials WHERE mat_no = ANY($1)"
param = "mat_no"
batch_by = "mat_no"
```

- `sql` — query using `$1` for the batched parameter array
- `param` — column on the parent result supplying the parameter values
- `batch_by` — **always set this** — column in the SQL result used to key
  child rows back to parents. Omitting it triggers a runtime warning and
  falls back to a slow per-parent-row (N+1) query pattern.

### Sink patterns

**stdout** (inspect the composed document): `--sink stdout`

**Elasticsearch** (bulk-indexed, buffered at 500 docs / 5s intervals for throughput): `--sink elasticsearch --es-url http://localhost:9200 --index materials --id-field mat_no`

**Webhook**: `--sink webhook --webhook-url https://hooks.example.com/events`

**KV — Redis**: `--sink kv --kv-url redis://localhost:6379 --key-field mat_no --key-prefix pgx: --ttl 3600`
**KV — Memcached**: `--sink kv --kv-url memcached://localhost:11211 --key-field id --key-prefix session:`
Cache key = `{key_prefix}{value_of_key_field}`; if `key_field` is unset or
missing from the doc, a random UUID suffix is used instead. `--ttl 0` (Redis
only) means no expiry.

### Error handling

`--on-error strict` aborts the consumer on the first resolver/composition
failure; `--on-error lenient` (typical for production pipelines) skips the
bad message and continues.

---

## Config file (`~/.pgx/config.toml`) — the declarative form of everything above

Every CLI-flag pattern above has an equivalent, reusable block in the config
file, keyed by connection profile name. This is the recommended way to run
`pgx` as a long-lived service (systemd unit, container) since the whole
pipeline becomes `pgx -c <profile> <command>`.

```toml
default = "local"

[connections.local]
url = "postgres://postgres:postgres@localhost:5432/mydb"
description = "Local dev database"

# --- listen block ---
[connections.local.listen]
channels = ["orders"]
channel_full_behavior = "block"
max_reconnect_attempts = 5
reconnect_base_ms = 2000
reconnect_max_ms = 60000

[connections.local.listen.sink]
type = "shell"
command = "echo [$PGX_CHANNEL] $PGX_PAYLOAD"
mode = "simple"

# --- replicate block ---
[connections.local.replicate]
slot = "my_slot"
publications = ["my_pub"]
temporary = true
max_reconnect_attempts = 10
reconnect_base_ms = 1000
reconnect_max_ms = 60000
# filters = ["public.orders:status = 'active'", "amount > 100"]

[connections.local.replicate.sink]
type = "stdout"
pretty = true

# fan-out: apply to a Postgres replica AND forward to Kafka
# [connections.local.replicate.sink]
# type = "postgres"
# target_url = "postgres://user:pass@replica:5432/db"
# batch_size = 500
#
# [[connections.local.replicate.additional_sinks]]
# type = "kafka"
# brokers = "kafka-prod:9092"
# topic = "pgx-wal"

# --- consume block ---
[connections.local.consume]
source = { type = "rabbitmq", amqp_url = "amqp://guest:guest@localhost:5672/%2F", queue = "pgx-events" }
sink = { type = "elasticsearch", url = "http://localhost:9200", index = "materials", id_field = "mat_no" }
query_mode = "contract"
max_depth = 8
on_error = "lenient"

# --- resolvers (used by graphql / consume / listen elasticsearch) ---
[resolvers.material]
sql = "SELECT mat_no, name, description FROM materials WHERE mat_no = ANY($1)"
param = "mat_no"
batch_by = "mat_no"

[resolvers.sizes]
sql = "SELECT mat_no, size_code, name FROM sizes WHERE mat_no = ANY($1)"
param = "mat_no"
batch_by = "mat_no"
connection = "staging"   # resolvers can target a different named connection
```

With a profile run this way, `pgx -c local replicate`, `pgx -c local listen`,
and `pgx -c local consume` need no other flags — everything (slot, filters,
sink, resolvers) comes from the file.

> **Config-driven `consume` caveat:** The inline TOML table format for
> `source` (e.g. `source = { type = "rabbitmq", amqp_url = "...", queue = "..." }`)
> may not be parsed correctly in some pgx versions. If `pgx -c <profile> consume`
> fails to connect to RabbitMQ while the equivalent CLI flags work, use explicit
> flags instead: `pgx consume --source rabbitmq --amqp-url ... --queue ... --sink ...`.

---

## Full worked example: a 3-tier materials catalog → Elasticsearch pipeline

This walks a change in Postgres all the way to a fully-joined document in
Elasticsearch, and shows both the direct path and the broker-decoupled path.

**Scenario:** a `materials` table has related `sizes`, `colorways`, and
`features` (each feature having nested `attribute_entries`). Any UPDATE to a
material should re-index a single denormalized ES document containing the
whole tree.

**1. Infrastructure & profile**
Bring up Postgres + RabbitMQ + Elasticsearch, then drop a `config.toml`
(with the `[resolvers.*]` blocks for `material`, `sizes`, `colorways`,
`features`, `attribute_entries` as shown above) plus GraphQL schema/query
files into `~/.pgx/`.

**2. Validate the wiring**
`pgx graphql validate` — checks type references, query parsing, resolver
existence, and that every resolver's SQL is valid against the live schema.

**3. Run the composed query on demand**
`pgx graphql run MaterialFull -V mat_no=M001` — executes the full 3-tier
GraphQL composition and prints the joined JSON tree (material → sizes →
colorways → features → attribute_entries). Add `--compact` for one-line
output or `-o result.json` to save it.

**4a. Direct path: `listen` straight to Elasticsearch**
A Postgres trigger fires `pg_notify('materials', <ContractMessage>)` on every
change. `pgx listen -C materials elasticsearch --index materials --id-field mat_no`
subscribes to that channel, resolves the `MaterialFull` query from the
contract's `event_type`, re-runs the 3-tier composition, and POSTs the result
to `http://localhost:9200/materials/_doc/{mat_no}`. Triggering
`UPDATE materials SET name = name WHERE mat_no = 'M001'` and then checking
`curl http://localhost:9200/materials/_search?pretty` shows the full nested
document (sizes, colorways, features, and each feature's attribute entries).

**4b. Decoupled path: `listen` → RabbitMQ → `consume` → Elasticsearch**
Split the NOTIFY subscription from the GraphQL composition so they can scale
and fail independently:

- First, create the RabbitMQ exchange and queue:
  ```
  rabbitmqadmin declare exchange name=pgx type=topic durable=true
  rabbitmqadmin declare queue name=pgx-events durable=true
  rabbitmqadmin declare binding source=pgx destination=pgx-events routing_key="pgx.notify"
  ```
- Terminal 1: `pgx consume --source rabbitmq --queue pgx-events --query material --sink elasticsearch --es-url http://localhost:9200 --index materials --id-field mat_no` — waits on the queue, parses each message as a `ContractMessage`, resolves the query from `meta.event_type`, composes, and indexes.
- Terminal 2: `pgx listen -C materials rabbitmq --exchange pgx --routing-key pgx.notify --mode contract` — forwards every NOTIFY straight to RabbitMQ with per-message routing.
- Triggering the same UPDATE now flows: Postgres → NOTIFY → RabbitMQ → `consume` (GraphQL compose) → Elasticsearch, and the same `_search` check confirms the document.

**5. Same pipeline, config-driven**
Once the source/sink/query_mode are saved under `[connections.local.consume]`
in `config.toml` (see the config reference above), the entire consumer
collapses to `pgx consume -c local` — useful for a systemd unit or container
command line that doesn't need to carry broker credentials as flags.

This one scenario demonstrates every pattern in this skill at once: direct
NOTIFY→sink delivery, broker-decoupled delivery, on-demand GraphQL execution,
resolver-driven joins, and config-file profiles replacing CLI flags.

---

## Real deployment pattern: pg_x as a docker-compose sidecar (from `morphis`)

The example above is the local-dev walkthrough from pg_x's own repo. The
`morphis` project (a GraphQL-over-Postgres service with its own materials
catalog) runs the same `listen → elasticsearch` pattern as an always-on
sidecar container in production, which fills in three things the demo
doesn't show: how to containerize pg_x, what the source-side NOTIFY trigger
actually looks like, and a deeper real resolver chain.

### The sidecar service

```yaml
pgx-listen:
  image: ghcr.io/jyasuu/pg_x:main
  command:
    - "listen"
    - "--channel"
    - "materials_channel"
    - "elasticsearch"
    - "--es-url"
    - "http://elastic:morphis_es_pass@es:9200"
    - "--index"
    - "materials"
    - "--id-field"
    - "mat_no"
  environment:
    DATABASE_URL: postgres://postgres:postgres@db:5432/morphis
  volumes:
    - ./pgx/config.toml:/root/.pgx/config.toml
    - ./pgx/schema:/root/.pgx/schema
    - ./pgx/queries:/root/.pgx/queries
  depends_on:
    db:
      condition: service_healthy
    es:
      condition: service_healthy
  restart: unless-stopped
```

Notable choices for running pg_x as a long-lived service rather than a
one-off CLI call:

- The published `ghcr.io/jyasuu/pg_x:main` image, not a local build.
- `config.toml`, `schema/`, and `queries/` are bind-mounted into
  `/root/.pgx/` read-only, so the resolver/GraphQL definitions live in the
  app repo and are versioned alongside the trigger SQL that produces them.
- `depends_on` with `condition: service_healthy` on both Postgres and
  Elasticsearch, so pg_x doesn't start racing a not-yet-ready DB or index.
- `restart: unless-stopped` — this is meant to run forever, reconnecting
  through the backoff settings described earlier rather than being
  supervised by an external process manager.
- `DATABASE_URL` via environment, credentials via the ES URL's userinfo —
  no secrets baked into the command/config.

### The source-side trigger (the other half of contract mode)

The earlier example showed pg_x's _consumer_ side of contract mode. Here's
the _producer_ side — the actual Postgres trigger that emits the
`ContractMessage` payload on every change:

```sql
CREATE OR REPLACE FUNCTION notify_material_change()
RETURNS trigger AS $$
DECLARE
  mat_no_val TEXT;
BEGIN
  IF TG_OP = 'DELETE' THEN
    mat_no_val := OLD.mat_no;
  ELSE
    mat_no_val := NEW.mat_no;
  END IF;

  PERFORM pg_notify(
    'materials_channel',
    json_build_object(
      'meta', json_build_object('event_type', 'material'),
      'data', json_build_object('mat_no', mat_no_val)
    )::text
  );

  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS material_change_trigger ON materials;
CREATE TRIGGER material_change_trigger
AFTER INSERT OR UPDATE OR DELETE ON materials
FOR EACH ROW EXECUTE FUNCTION notify_material_change();
```

Note the payload only carries the primary key (`mat_no`), not the full row —
`pgx listen elasticsearch` re-fetches and re-joins the full document via the
GraphQL query named by `event_type` ("material") rather than trusting
whatever was in the row at NOTIFY time. This is the right shape for contract
mode: NOTIFY says _what changed_, the resolver chain decides _what to send_.

### A deeper real resolver chain (5 levels, with a cast in `batch_by`)

```toml
[resolvers.material]
sql = "SELECT mat_no, name, status, tenant_id FROM materials WHERE mat_no = $1"
param = "mat_no"
batch_by = "mat_no"

[resolvers.sizes]
sql = "SELECT id, size_code, mat_no, name FROM sizes WHERE mat_no = ANY($1) ORDER BY id"
param = "mat_no"
batch_by = "mat_no"

[resolvers.colorways]
sql = "SELECT id, colorway_code, mat_no, name, hex FROM colorways WHERE mat_no = ANY($1) ORDER BY id"
param = "mat_no"
batch_by = "mat_no"

[resolvers.material_features]
sql = "SELECT id, id AS feature_id, mat_no, feature_name, description FROM material_features WHERE mat_no = ANY($1) ORDER BY id"
param = "mat_no"
batch_by = "mat_no"

[resolvers.feature_attributes]
sql = "SELECT id, feature_id, attr_name, attr_value FROM feature_attributes WHERE feature_id::text = ANY($1) ORDER BY id"
param = "feature_id"
batch_by = "feature_id"
```

Two things worth copying for your own resolver chains:

- `material_features` aliases its own `id` as `feature_id` (`id, id AS feature_id`)
  specifically so the next resolver down (`feature_attributes`) has a
  matching `param`/`batch_by` name to join on — the resolver chain is just
  parent-output-column → child-input-column, so name them to line up.
- `feature_attributes` casts the batched key with `feature_id::text = ANY($1)`
  because the parent's `feature_id` is numeric but pg_x's batched resolver
  parameters arrive as text — cast in the SQL rather than assuming the types
  match.
- `tenant_id` is selected straight through the top-level `material` resolver,
  so multi-tenant filtering/display data rides along in the same composed
  document with no extra resolver needed.

---

## Other useful commands

| Task                                           | Command                                                                                       |
| ---------------------------------------------- | --------------------------------------------------------------------------------------------- |
| Table-formatted query                          | `pgx query -q "SELECT * FROM users LIMIT 10"`                                                 |
| JSON query output                              | `pgx query -q "SELECT count(*) FROM orders" --json`                                           |
| Export to Excel                                | `pgx export -q "SELECT * FROM orders" -o orders.xlsx`                                         |
| Export to CSV                                  | `pgx export -q "SELECT * FROM orders" -m csv -o orders.csv`                                   |
| Export to Iceberg (needs `--features iceberg`) | `pgx export -q "..." -m iceberg --iceberg-table public.orders_snapshot --warehouse-path ./wh` |
| Multi-sheet Excel from a `.sql` file           | Each `-- sheet: Name` comment in the file starts a new sheet                                  |
| Server info                                    | `pgx info --version --databases --tables`                                                     |

## `mcp` — expose pgx as MCP tools for AI assistants

Requires `--features mcp`. Start with `pgx -U $DATABASE_URL mcp --transport stdio`
for local clients (e.g. Claude Desktop), or `--transport sse --host 0.0.0.0 --port 3100`
for remote clients, optionally with `--token <secret>` (static bearer auth)
or `--oauth-issuer <issuer-url>` (OIDC/JWKS validation, e.g. Keycloak).
Exposed tools: `query`, `list_tables`, `describe_table`, `db_info`,
`list_profiles`.

## Cargo feature flags (what needs to be built in)

| Feature    | Default | Enables                                      |
| ---------- | ------- | -------------------------------------------- |
| `excel`    | on      | Excel export                                 |
| `rabbitmq` | on      | RabbitMQ downstream                          |
| `webhook`  | on      | HTTP webhook downstream                      |
| `kv`       | on      | Redis / Memcached sink                       |
| `parquet`  | on      | Parquet file output                          |
| `kafka`    | off     | Kafka downstream (needs system `librdkafka`) |
| `tls`      | off     | TLS for the control-plane connection         |
| `iceberg`  | off     | Apache Iceberg output (export + replicate)   |
| `mcp`      | off     | MCP server mode                              |
