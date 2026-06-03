# pgx — GraphQL Engine + Elasticsearch Sink: Todo

## Overview

Add a GraphQL composition engine inside pgx that:
- Accepts a named query + variables from a `pg_notify` payload
- Executes resolver-mapped SQL queries against Postgres (with DataLoader batching)
- Assembles results into a nested JSON document
- Pushes the document to Elasticsearch via `_doc` or `_bulk` API

New module: `src/graphql/`  
New sink: `src/downstream/elasticsearch.rs`  
New command: `pgx graphql validate | run`

---

## Phase 1 — Schema definition + resolver config

> Goal: define the document shape and map every field to SQL before writing any execution logic.

- [ ] **Design the GraphQL schema DSL** `src/graphql/schema.rs`
  - Parse `.graphql` type definition files into an internal type graph
  - Support: Object types, scalar fields, list relations
  - Exclude for now: interfaces, unions, directives, subscriptions, mutations
  - Store parsed types in a `SchemaRegistry` struct shared across resolvers

- [ ] **Extend `config.toml` with resolver mappings** `src/utils/config.rs`
  - Add `[resolvers.TypeName.field]` blocks
  - Each resolver entry:
    - `sql` — the query string or path to a `.sql` file
    - `param` — which variable/column to bind as `$1`
    - `batch_by` — column name used for DataLoader batching (`ANY($1)`)
    - `connection` — optional named connection override (e.g. read replica)
  - Example:
    ```toml
    [resolvers.Material]
    sql   = "SELECT * FROM mat_main WHERE mat_no = $1"
    param = "mat_no"

    [resolvers.Material.sizes]
    sql      = "SELECT * FROM mat_size WHERE mat_no = $1"
    batch_by = "mat_no"

    [resolvers.Material.colorways]
    sql      = "SELECT * FROM mat_colorways WHERE mat_no = $1"
    batch_by = "mat_no"

    [resolvers.Material.colorways.images]
    sql      = "SELECT * FROM colorway_images WHERE colorway_id = ANY($1)"
    batch_by = "colorway_id"

    [resolvers.Material.colorways.stock]
    sql      = "SELECT * FROM colorway_stock WHERE colorway_id = ANY($1)"
    batch_by = "colorway_id"
    ```

- [ ] **Named query document loading** `src/graphql/query.rs`
  - Load `.graphql` query files from `~/.pgx/queries/`
  - Parse selection set into an execution plan tree at startup (not at runtime)
  - Surface schema/resolver mismatch errors early, before any NOTIFY is processed
  - Example query file `~/.pgx/queries/material_full.graphql`:
    ```graphql
    query MaterialFull($mat_no: String!) {
      material(mat_no: $mat_no) {
        mat_no
        name
        status
        sizes { size_code width height }
        colorways {
          color_name
          images { url type }
          stock  { warehouse_code quantity }
        }
        descriptions { lang body }
      }
    }
    ```

---

## Phase 2 — Query executor + resolver runtime

> Goal: walk the selection set, call resolvers, convert rows to JSON.

- [ ] **Selection set walker** `src/graphql/executor.rs`
  - Recursively walk the parsed query execution plan tree
  - At each node: look up resolver config → bind parameters → execute SQL → recurse into child fields
  - Root resolver receives variables from the NOTIFY payload `data` field
  - Child resolvers receive their `param` value from the parent row's column

- [ ] **Variable and parameter binding** `src/graphql/executor.rs`
  - Root level: bind from NOTIFY payload variables (e.g. `mat_no`)
  - Child level: bind from parent row column value
  - Type-aware binding: string, int, float, bool, uuid, timestamptz
  - Return a clear error if a required variable is missing from the payload

- [ ] **Row → JSON value conversion** `src/graphql/row.rs`
  - Convert `tokio-postgres` `Row` into `serde_json::Value`
  - Handle common Postgres types:
    - `text`, `varchar` → `Value::String`
    - `int2`, `int4`, `int8` → `Value::Number`
    - `float4`, `float8` → `Value::Number`
    - `bool` → `Value::Bool`
    - `uuid` → `Value::String`
    - `timestamptz`, `date` → `Value::String` (ISO 8601)
    - `jsonb`, `json` → `Value` (pass through as-is)
    - `NULL` → `Value::Null`
  - Extend or reuse `src/utils/format.rs` where applicable

---

## Phase 3 — DataLoader (batch child resolvers)

> Goal: eliminate N+1 queries when a parent returns multiple rows with child relations.

- [ ] **DataLoader core** `src/graphql/dataloader.rs`
  - Per-resolver batch accumulator
  - Collect all child key values from the full parent result set
  - Deduplicate key values
  - Execute one SQL query: `WHERE id = ANY($1)` with all keys
  - Group result rows into a `HashMap<KeyValue, Vec<Row>>` for O(1) lookup
  - Each parent row then looks up its children from the map without an extra query

- [ ] **Batch vs single dispatch decision** `src/graphql/executor.rs`
  - If resolver has `batch_by` set **and** parent returned multiple rows → use DataLoader
  - If parent returned a single row (root resolver) → direct single-param query is sufficient
  - Avoids DataLoader overhead for simple single-entity syncs

---

## Phase 4 — Elasticsearch sink

> Goal: connect the executor output to Elasticsearch.

- [ ] **ES sink wires into executor** `src/downstream/elasticsearch.rs`
  - Implement the `Downstream` trait (reuse existing `listen` sink interface)
  - On `send(&NotifyEvent)`:
    1. Extract `query` name and variables from `event.payload` (`data` field)
    2. Call `executor::execute(query_name, variables)` → `serde_json::Value`
    3. POST assembled document to ES `_doc` or queue into bulk buffer
  - The sink has no knowledge of GraphQL internals — only calls `execute()`

- [ ] **Index name + document ID resolution** `src/downstream/elasticsearch.rs`
  - Index name and `_id` sourced from (in priority order):
    1. NOTIFY payload override
    2. Named query bundle config
    3. CLI flag `--index` / `--id-field`
  - Support simple field templates: `"{schema}-{table}"`, `"materials"`
  - `_id` defaults to the value of a configured root field (e.g. `mat_no`)

- [ ] **Bulk buffer + retry** `src/downstream/elasticsearch.rs`
  - Accumulate assembled documents up to `bulk_size` (default: 100) or `flush_interval_ms` (default: 500ms)
  - Flush to ES `_bulk` API
  - On ES error: log failure with `mat_no`, query name, and HTTP status
  - Do not silently drop documents — failed docs should be re-triggerable via `pg_notify`

- [ ] **Add `elasticsearch` Cargo feature** `Cargo.toml`
  - Gate behind `elasticsearch` feature flag (reuses existing `reqwest` dependency)
  - Add `deadpool-postgres` for connection pooling
  - Add `async-graphql-parser` for GraphQL document parsing (parser only, not the full server)
  ```toml
  [features]
  elasticsearch = ["reqwest"]

  [dependencies]
  async-graphql-parser = "7"
  deadpool-postgres     = "0.12"
  ```

---

## Phase 5 — Tooling + developer experience

> Goal: make the feature testable and debuggable without a full NOTIFY + ES setup.

- [ ] **`pgx graphql validate` command** `src/commands/graphql.rs`
  - Dry-run startup check:
    - Load schema type definitions
    - Load resolver config
    - Load all named query files from `~/.pgx/queries/`
    - Verify every selected field has a resolver
    - Verify every resolver SQL is syntactically parseable
    - Verify every `batch_by` column exists in the resolver's SQL result columns
  - Exit with descriptive errors before any NOTIFY is processed
  - Usage: `pgx graphql validate`

- [ ] **`pgx graphql run` command** `src/commands/graphql.rs`
  - Execute a named query with variables provided on the CLI
  - Print assembled JSON to stdout (pretty-printed)
  - Supports `--json` and `--compact` flags
  - No NOTIFY or Elasticsearch required — pure local test
  - Usage:
    ```bash
    pgx -U $DATABASE_URL graphql run material_full \
      --var mat_no=MAT-1042
    ```
  - Start implementing this **first** — tight feedback loop for Phases 1 and 2

- [ ] **Connection pool for resolver queries** `src/graphql/pool.rs`
  - Resolvers fire many short concurrent queries — a pool prevents connection exhaustion under burst load
  - Use `deadpool-postgres` (or `bb8-postgres`)
  - Separate pool config for the replica connection used by resolvers:
    ```toml
    [resolvers._pool]
    connection    = "mat_replica"   # named connection from [connections]
    max_size      = 10
    ```

---

## New file layout

```
src/
├── graphql/
│   ├── mod.rs
│   ├── schema.rs       # type graph parsed from .graphql files
│   ├── query.rs        # named query loading + selection set → execution plan
│   ├── executor.rs     # walk plan → call resolvers → assemble JSON
│   ├── dataloader.rs   # batch child resolver SQL calls
│   ├── row.rs          # tokio-postgres Row → serde_json::Value
│   └── pool.rs         # connection pool for resolver queries
├── downstream/
│   └── elasticsearch.rs
└── commands/
    └── graphql.rs      # validate + run subcommands
```

---

## Suggested implementation order

1. `pgx graphql run` skeleton (Phase 5) — establishes the CLI entry point
2. Schema parser + resolver config loading (Phase 1)
3. Row → JSON conversion (Phase 2)
4. Single-resolver executor, root only (Phase 2) — enough to test `graphql run` end to end
5. Child resolver execution, no batching (Phase 2)
6. DataLoader batching (Phase 3)
7. `pgx graphql validate` (Phase 5)
8. Connection pool (Phase 5)
9. Elasticsearch sink (Phase 4)
