# pgx replicate — Feature Roadmap

## Overview

Current `pgx replicate` streams WAL changes to **stdout / shell / webhook / RabbitMQ / Kafka**. This plan adds the missing pieces for full logical replication coverage.

---

## 1. PostgreSQL Subscriber Sink (Apply to another PG)

**Goal:** `pgx replicate --publication my_pub postgres://target/db` applies INSERT/UPDATE/DELETE to another database.

### Design

```
pgx replicate                                    Target PG
   │       ┌──────────────────────────────────┐      │
   ├─Begin─┤ BEGIN;                            │      │
   ├─Rel───┤ CREATE TABLE IF NOT EXISTS ...    │      │
   ├─Insert┤ INSERT INTO ... VALUES (...)      ├──────┤
   ├─Insert┤ INSERT INTO ... VALUES (...)      │      │
   ├─Commit┤ COMMIT;                           │      │
   └───────┴──────────────────────────────────┘      │
```

### Implementation Plan

| Step | File(s) | What |
|------|---------|------|
| 1.1 | `src/commands/replicate.rs` | Add `Postgres(PostgresArgs)` variant to `ReplicateDownstreamCommand` |
| 1.2 | `src/commands/replicate.rs` | Add `PostgresArgs` struct: `--target-url`, `--schema-map` (optional remapping), `--batch-size` |
| 1.3 | `src/commands/replicate.rs` | Implement `PostgresWalSink` struct |
| 1.4 | `src/commands/replicate.rs` | In `send_wal`: buffer events per txn; on Commit → execute batched SQL |
| 1.5 | `src/utils/config.rs` | Add `Postgres { target_url, schema_map, batch_size }` to `DownstreamSinkKind` |
| 1.6 | `src/replication/decoder.rs` | Expose `RelationInfo.columns` type info for CREATE TABLE generation |
| 1.7 | Cargo | Add `deadpool-postgres` or `tokio-postgres` (already present) for target connection pool |

### Key behaviors

- **Schema sync:** On `Relation` event → `CREATE TABLE IF NOT EXISTS` with matching columns + types
- **Replica identity:** Use `is_key` from `ColumnDef` for UPDATE/DELETE WHERE clause
- **Transaction batching:** Buffer DMLs per transaction; execute in one `BEGIN`/`COMMIT` on commit event
- **Conflict handling:** `ON CONFLICT DO NOTHING` for inserts, skip missing rows for updates/deletes
- **Type mapping:** PG type OID → SQL literal (text is fine for most types; special cases for JSON/BYTEA/geometry)

### CLI example

```bash
pgx replicate \
  --publication my_pub \
  --table public.orders \
  postgres \
    --target-url "postgres://user:pass@replica:5432/db" \
    --schema-map "public.orders=public.orders_archive" \
    --batch-size 100
```

---

## 2. Row-Level WHERE Filter

**Goal:** `--where "status = 'active'"` filters WAL events before forwarding.

### Design

Add a `--where <expression>` option (repeatable, per-table) that evaluates against decoded row values.

### Implementation Plan

| Step | File(s) | What |
|------|---------|------|
| 2.1 | `src/commands/replicate.rs` | Add `--where` field to `ReplicateArgs`: `Vec<String>` |
| 2.2 | `src/commands/replicate.rs` | Parse filter expressions into a predicate struct |
| 2.3 | `src/commands/replicate.rs` | Apply predicate in `should_forward()` for DML events |

### Filter expression syntax

```
--where "public.orders:status = 'active' AND amount > 100"
--where "public.users:deleted_at IS NULL"
```

Parsed into a simple AST:

```
enum FilterExpr {
    Eq(String, String),
    Neq(String, String),
    Gt(String, f64),
    Lt(String, f64),
    IsNull(String),
    IsNotNull(String),
    And(Box<FilterExpr>, Box<FilterExpr>),
    Or(Box<FilterExpr>, Box<FilterExpr>),
}
```

### Config support

```toml
[connections.prod.replicate]
filters = ["public.orders:status = 'active'"]
```

---

## 3. Column Remapping / Transformation

**Goal:** Rename, drop, or transform columns before forwarding to the sink.

### Design

Two approaches:
- **Drop columns:** `--drop-cols "public.orders:internal_note,secret_flag"`
- **Rename columns:** `--rename "public.orders:order_id → id, customer_name → name"`

### Implementation Plan

| Step | File(s) | What |
|------|---------|------|
| 3.1 | `src/commands/replicate.rs` | Add `--drop-cols`, `--rename`, `--transform` to `ReplicateArgs` |
| 3.2 | `src/commands/replicate.rs` | Apply transforms in `should_forward()` before serialization |
| 3.3 | `src/replication/event.rs` | Add `WalEvent::apply_transform()` method or similar |

### TBD: Complex transforms

For value transformations (e.g., `json_extract_path_text(data, 'nested')`), consider:
- Built-in: simple SQL expression evaluation (like postgres-filters crate)
- Plugin: WASM-based transform functions (far future)
- Delegate: let the downstream sink handle it (simplest)

---

## 4. Multi-Subscriber Fan-Out

**Goal:** One `pgx replicate` process → multiple sinks simultaneously.

### Design

Allow multiple `--sink` flags or a single downstream config that accepts multiple.

### Options

| Approach | Complexity | Notes |
|----------|-----------|-------|
| **Multiple `--sink` flags** | Low | `WalSink` becomes `Vec<Arc<dyn WalSink>>`; each event fans out |
| **Subprocess mode** | Medium | Spawn N child processes, each with one sink; parent fans out JSON |
| **Config-based fan-out** | Low | `sinks = [{ type = "kafka", ... }, { type = "postgres", ... }]` |

---

## 5. DDL Replication (Out of Scope for Now)

**Problem:** pgoutput does not carry DDL. PostgreSQL has no built-in DDL logical replication.

**Workarounds available today:**
- Use `event_trigger` on the publisher to log DDL to a table, then replicate that table
- Use `pgl_ddl_deploy` extension
- Use `pgx listen` with `NOTIFY` for DDL events

**Not planned** unless `pgoutput` adds DDL support or a clear extension pattern emerges.

---

## 6. Progress & Verification

| Feature | Status | Test strategy |
|---------|--------|---------------|
| 1. PostgreSQL sink | **Done** | `pgx replicate ... postgres --target-url ...` (manual); unit tests for PostgresApplier SQL gen |
| 2. Row-level WHERE | **Done** | Unit tests for `FilterExpr::evaluate` and `RowFilter`; integration via `--where` |
| 3. Column remapping | **Done** | 6 unit tests for `WalEvent::apply_transforms` (drop, rename, swap, update, delete, noop) |
| 4. Multi-sink fan-out | **Done** | `--sink "stdout:pretty=true"` alongside primary subcommand; config `additional_sinks` |
| 5. DDL replication | Skipped | N/A |

---

## 7. Order of Implementation

1. **PostgreSQL sink** — highest value, fills the biggest gap
2. **Multi-sink fan-out** — needed to use PG sink alongside existing sinks
3. **Row-level WHERE** — independent, adds precision
4. **Column remapping** — independent, adds flexibility
5. **DDL replication** — blocked upstream
