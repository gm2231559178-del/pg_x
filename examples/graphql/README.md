# pgx GraphQL Practice Setup

End-to-end demo: compose a material catalog with 3-tier nested data
via GraphQL, and stream changes from PostgreSQL NOTIFY into Elasticsearch,
optionally via a message broker.

## Prerequisites

- Docker & Docker Compose
- `pgx` binary built (`cargo build --release`)
- RabbitMQ feature enabled (default: on)

## 1. Start infrastructure

```bash
docker compose up -d
```

Starts PostgreSQL (`:5432`) with sample tables + NOTIFY trigger,
RabbitMQ (`:5672`), and Elasticsearch (`:9200`).

## 2. Copy the pgx config

```bash
cp -r examples/graphql/pgx/* ~/.pgx/
```

Sets up `~/.pgx/` with connection profiles, resolvers, type schemas, and queries.

## 3. Validate the setup

```bash
pgx graphql validate
```

Checks all type references, query parses, resolver existence, and SQL validity.

## 4. Run a query on demand (pgx graphql run)

### Pretty-printed

```bash
pgx graphql run MaterialFull -V mat_no=M001
```

### Compact (single line)

```bash
pgx graphql run MaterialFull -V mat_no=M001 --compact
```

### Save to file

```bash
pgx graphql run MaterialFull -V mat_no=M001 -o result.json
```

### Other materials

```bash
pgx graphql run MaterialFull -V mat_no=M002
pgx graphql run MaterialFull -V mat_no=M003
```

## 5. Stream changes directly into Elasticsearch (pgx listen)

### Start the listener

```bash
pgx listen -C materials elasticsearch \
  --index materials \
  --id-field mat_no
```

This subscribes to the `materials` NOTIFY channel. Every time a row changes,
the trigger fires a ContractMessage payload. The Elasticsearch sink:

1. Parses the event
2. Looks up the `MaterialFull` query
3. Executes the 3-tier GraphQL composition against PostgreSQL
4. POSTs the assembled document to `http://localhost:9200/materials/_doc/{mat_no}`

### Trigger a change (separate terminal)

```bash
docker compose exec postgres psql -U postgres \
  -c "UPDATE materials SET name = name WHERE mat_no = 'M001';"
```

### Verify in Elasticsearch

```bash
curl http://localhost:9200/materials/_search?pretty
```

Each document contains the full nested tree:

```json
{
  "mat_no": "M001",
  "name": "Premium Cotton Canvas",
  "status": "active",
  "sizes": [
    { "size_code": "S",  "name": "Small" },
    { "size_code": "M",  "name": "Medium" },
    { "size_code": "L",  "name": "Large" },
    { "size_code": "XL", "name": "Extra Large" }
  ],
  "colorways": [
    { "colorway_code": "WH", "name": "White", "hex": "#FFFFFF" },
    { "colorway_code": "BK", "name": "Black", "hex": "#000000" },
    { "colorway_code": "NV", "name": "Navy",  "hex": "#000080" }
  ],
  "features": [
    {
      "feature_name": "Construction",
      "description": "Plain weave",
      "attribute_entries": [
        { "attr_name": "weave_type",   "attr_value": "plain" },
        { "attr_name": "thread_count", "attr_value": "120" }
      ]
    },
    {
      "feature_name": "Care",
      "description": "Standard care instructions",
      "attribute_entries": [
        { "attr_name": "wash",  "attr_value": "30°C" },
        { "attr_name": "bleach", "attr_value": "No" }
      ]
    }
  ]
}
```

## 6. Pipeline through a message broker (pgx listen → RabbitMQ → pgx consume → ES)

This flow separates the NOTIFY subscription from the GraphQL composition:
`pgx listen` forwards to RabbitMQ, then `pgx consume` reads from RabbitMQ,
composes the GraphQL document, and indexes it.

### Terminal 1: Start the consume command

```bash
pgx consume \
  --source rabbitmq --queue pgx-events --exchange pgx --routing-key 'pgx.notify' \
  --sink elasticsearch --es-url http://localhost:9200 --index materials --id-field mat_no
```

This connects to RabbitMQ, waits for messages on the `pgx-events` queue
(bound to exchange `pgx` with routing key `pgx.notify`), parses each as a
ContractMessage, resolves the query from `meta.event_type` ("MaterialFull"),
executes GraphQL composition, and indexes to Elasticsearch.

### Terminal 2: Forward NOTIFY events to RabbitMQ

```bash
pgx listen -C materials rabbitmq \
  --exchange pgx --routing-key pgx.notify \
  --mode contract
```

This listens on PostgreSQL `materials` channel and publishes every
ContractMessage to RabbitMQ with per-message routing from the contract.

### Terminal 3: Trigger a change

```bash
docker compose exec postgres psql -U postgres \
  -c "UPDATE materials SET name = name WHERE mat_no = 'M002';"
```

### Verify

```bash
curl http://localhost:9200/materials/_search?pretty
```

### Or skip the broker — run consume directly with a file/stdin (simple mode)

```bash
echo '{"mat_no": "M001"}' | pgx consume \
  --source rabbitmq --queue test \
  --query-mode simple --query MaterialFull \
  --sink stdout
```

## 7. Consume examples with different options

### Simple mode — fixed query, raw payload as variables

```bash
# Message body {"mat_no": "M003"} → query MaterialFull(mat_no: "M003")
pgx consume \
  --source rabbitmq --queue events \
  --query-mode simple --query MaterialFull \
  --sink stdout
```

### Strict error mode — abort on first failure

```bash
pgx consume \
  --source kafka --brokers localhost:9092 --topic materials --group-id pgx \
  --on-error strict \
  --sink webhook --webhook-url https://hooks.example.com/materials
```

### Stdout sink — inspect the composed document

```bash
pgx consume \
  --source rabbitmq --queue events \
  --sink stdout
```

### All in one: config-driven consume

Add to `~/.pgx/config.toml`:

```toml
[connections.local.consume]
source = { type = "rabbitmq", amqp_url = "amqp://guest:guest@localhost:5672/%2F", queue = "pgx-events", exchange = "pgx", routing_key = "pgx.notify" }
sink = { type = "elasticsearch", url = "http://localhost:9200", index = "materials", id_field = "mat_no" }
query_mode = "contract"
max_depth = 8
on_error = "lenient"
```

Then run with just the connection profile:

```bash
pgx consume -c local
```

## Architecture

```
                            ┌──────────────────────┐
                            │     PostgreSQL        │
                            │  NOTIFY channel       │
                            │  "materials"          │
                            └──────────┬───────────┘
                                       │ ContractMessage
                                       │ { event_type: "MaterialFull"
                          ┌────────────┤   data: { mat_no } }
                          │            │
                          ▼            ▼
              ┌──────────────────┐  ┌──────────────────┐
              │   pgx listen     │  │   pgx listen     │
              │   ES sink        │  │   RMQ sink       │
              │   direct index   │  │   forward to     │
              │                  │  │   RabbitMQ       │
              └────────┬─────────┘  └────────┬─────────┘
                       │                     │
                       │                     ▼
                       │              ┌──────────────────┐
                       │              │    pgx consume   │
                       │              │  RabbitMQ source  │
                       │              │  GraphQL compose  │
                       │              │  ES sink          │
                       │              └────────┬─────────┘
                       ▼                       │
              ┌──────────────────┐              │
              │  Elasticsearch   │◄─────────────┘
              │  materials index │
              └──────────────────┘
```

## Structure

```
~/.pgx/
  config.toml              # Connection URL + resolvers + listen/consume config
  schema/
    material.graphql       # type Material { ... }
    size.graphql           # type Size { ... }
    colorway.graphql       # type Colorway { ... }
    feature.graphql        # type MaterialFeature / FeatureAttribute { ... }
  queries/
    MaterialFull.graphql   # 3-tier: material -> sizes -> colorways -> features -> attributes

docker-compose.yml         # PostgreSQL + RabbitMQ + Elasticsearch
init.sql                   # Tables, seed data, NOTIFY trigger
```
