# pgx GraphQL Practice Setup

End-to-end demo: compose a material catalog with 3-tier nested data
via GraphQL, and stream changes from PostgreSQL NOTIFY into Elasticsearch.

## Prerequisites

- Docker & Docker Compose
- `pgx` binary built (`cargo build --release`)

## 1. Start infrastructure

```bash
docker compose up -d
```

Starts PostgreSQL (`:5432`) with sample tables + NOTIFY trigger, and
Elasticsearch (`:9200`).

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

## 5. Stream changes into Elasticsearch (pgx listen)

### Start the listener

```bash
pgx listen --channel materials elasticsearch \
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

### With config-driven sink

Instead of CLI flags, add to `~/.pgx/config.toml`:

```toml
[connections.local.listen]
channels = ["materials"]

[connections.local.listen.sink]
type = "elasticsearch"
url = "http://localhost:9200"
index = "materials"
id_field = "mat_no"
```

Then start without subcommand args:

```bash
pgx listen -C materials elasticsearch
```

## Architecture

```
┌──────────────┐     NOTIFY      ┌──────────────────┐
│  PostgreSQL  │ ──────────────> │   pgx listen     │
│              │                 │                  │
│  materials   │  ContractMessage│  Elasticsearch   │
│  trigger     │   {             │  Downstream      │
│              │    meta: {      │       │          │
│              │     event_type  │       │ GraphQL   │
│              │    }            │       │ execute   │
│              │    data: {      │       ▼          │
│              │     mat_no }    │  ┌──────────────┐│
│              │   }             │  │  executor    ││
└──────────────┘                 │  │  DataLoader  ││
                                 │  │  resolvers   ││
                                 │  └──────┬───────┘│
                                 │         │ SQL    │
                                 │         ▼        │
                                 │  ┌──────────────┐│
                                 │  │  PostgreSQL  ││
                                 │  └──────────────┘│
                                 │         │        │
                                 │         ▼        │
                                 │  ┌──────────────┐│
                                 │  │ Elasticsearch ││
                                 │  │  POST _doc    ││
                                 │  └──────────────┘│
                                 └──────────────────┘
```

## Structure

```
~/.pgx/
  config.toml              # Connection URL + resolvers
  schema/
    material.graphql       # type Material { ... }
    size.graphql           # type Size { ... }
    colorway.graphql       # type Colorway { ... }
    feature.graphql        # type MaterialFeature / FeatureAttribute { ... }
  queries/
    MaterialFull.graphql   # 3-tier: material -> features -> attributes

docker-compose.yml         # PostgreSQL + Elasticsearch
init.sql                   # Tables, seed data, NOTIFY trigger
```
