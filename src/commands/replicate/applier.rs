use anyhow::{Context, Result};
use async_trait::async_trait;
use std::collections::HashMap;
use tracing::warn;

use super::PostgresArgs;
use crate::replication::event::{ColVal, Row, WalEvent};

// ─────────────────────────────────────────────────────────────────────────────
// Pure SQL layer (D4.1): every WAL event maps to a statement or an explicit,
// verified skip. This layer is unit-tested directly; no I/O.
// ─────────────────────────────────────────────────────────────────────────────

pub(crate) type SchemaMap = HashMap<(String, String), (String, String)>;

/// A single target statement, or an explicit skip. A skipped row is a verified
/// decision (e.g. no usable key) and is never sent to the target.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Sql {
    Stmt(String),
    Skipped {
        op: &'static str,
        reason: &'static str,
    },
}

/// Map every event in a source WAL transaction to its target statement,
/// dropping skips. A skip is logged with the table context so it is visible,
/// not silent data loss.
pub(crate) fn gen_statements(events: &[WalEvent], schema_map: &SchemaMap) -> Vec<String> {
    let mut statements = Vec::new();
    for event in events {
        match gen_sql(event, schema_map) {
            Sql::Stmt(sql) => statements.push(sql),
            Sql::Skipped { op, reason } => {
                if let Some((schema, table)) = event.table_name() {
                    warn!(op, schema = %schema, table = %table, reason, "Skipping WAL event on target");
                } else {
                    warn!(op, reason, "Skipping WAL event on target");
                }
            }
        }
    }
    statements
}

pub(crate) fn gen_sql(event: &WalEvent, schema_map: &SchemaMap) -> Sql {
    match event {
        WalEvent::Insert {
            schema, table, new, ..
        } => gen_insert(schema, table, new, schema_map),
        WalEvent::Update {
            schema,
            table,
            old,
            new,
            ..
        } => gen_update(schema, table, old.as_ref(), new, schema_map),
        WalEvent::Delete {
            schema, table, old, ..
        } => gen_delete(schema, table, old, schema_map),
        WalEvent::Truncate {
            tables,
            cascade,
            restart_seqs,
            ..
        } => gen_truncate(tables, *cascade, *restart_seqs, schema_map),
        WalEvent::Begin { .. }
        | WalEvent::Commit { .. }
        | WalEvent::Relation { .. }
        | WalEvent::Keepalive { .. } => Sql::Skipped {
            op: "event",
            reason: "not a target-applicable statement",
        },
    }
}

fn map_table(schema: &str, table: &str, schema_map: &SchemaMap) -> (String, String) {
    schema_map
        .get(&(schema.to_string(), table.to_string()))
        .cloned()
        .unwrap_or_else(|| (schema.to_string(), table.to_string()))
}

fn gen_insert(schema: &str, table: &str, new: &Row, schema_map: &SchemaMap) -> Sql {
    let (tgt_schema, tgt_table) = map_table(schema, table, schema_map);

    let mut cols: Vec<&String> = new
        .iter()
        .filter(|(_, v)| !matches!(v, ColVal::Unchanged))
        .map(|(c, _)| c)
        .collect();
    cols.sort();

    if cols.is_empty() {
        return Sql::Skipped {
            op: "insert",
            reason: "row has no sendable columns (all Unchanged); set REPLICA IDENTITY FULL",
        };
    }

    let col_names: Vec<String> = cols.iter().map(|c| quote_ident(c)).collect();
    let col_vals: Vec<String> = cols.iter().map(|c| colval_to_sql(&new[*c])).collect();

    Sql::Stmt(format!(
        "INSERT INTO {}.{} ({}) VALUES ({})",
        quote_ident(&tgt_schema),
        quote_ident(&tgt_table),
        col_names.join(", "),
        col_vals.join(", "),
    ))
}

fn gen_update(
    schema: &str,
    table: &str,
    old: Option<&Row>,
    new: &Row,
    schema_map: &SchemaMap,
) -> Sql {
    let (tgt_schema, tgt_table) = map_table(schema, table, schema_map);
    let qualified = format!("{}.{}", quote_ident(&tgt_schema), quote_ident(&tgt_table));

    let mut set_cols: Vec<&String> = new
        .iter()
        .filter(|(_, v)| !matches!(v, ColVal::Unchanged))
        .map(|(c, _)| c)
        .collect();
    set_cols.sort();
    let set_clauses: Vec<String> = set_cols
        .iter()
        .map(|c| format!("{} = {}", quote_ident(c), colval_to_sql(&new[*c])))
        .collect();

    let where_clauses: Vec<String> = match old {
        Some(old_row) => old_row
            .iter()
            .filter(|(_, v)| !matches!(v, ColVal::Unchanged))
            .map(|(c, v)| format!("{} = {}", quote_ident(c), colval_to_sql(v)))
            .collect(),
        None => Vec::new(),
    };

    if set_clauses.is_empty() {
        return Sql::Skipped {
            op: "update",
            reason: "new tuple has no sendable columns (all Unchanged)",
        };
    }

    if where_clauses.is_empty() {
        return Sql::Skipped {
            op: "update",
            reason: "old tuple has no usable WHERE columns; set REPLICA IDENTITY FULL",
        };
    }

    Sql::Stmt(format!(
        "UPDATE {} SET {} WHERE {}",
        qualified,
        set_clauses.join(", "),
        where_clauses.join(" AND "),
    ))
}

fn gen_delete(schema: &str, table: &str, old: &Row, schema_map: &SchemaMap) -> Sql {
    let (tgt_schema, tgt_table) = map_table(schema, table, schema_map);
    let qualified = format!("{}.{}", quote_ident(&tgt_schema), quote_ident(&tgt_table));

    let where_clauses: Vec<String> = old
        .iter()
        .filter(|(_, v)| !matches!(v, ColVal::Unchanged))
        .map(|(c, v)| format!("{} = {}", quote_ident(c), colval_to_sql(v)))
        .collect();

    if where_clauses.is_empty() {
        return Sql::Skipped {
            op: "delete",
            reason: "old tuple has no usable WHERE columns; set REPLICA IDENTITY FULL",
        };
    }

    Sql::Stmt(format!(
        "DELETE FROM {} WHERE {}",
        qualified,
        where_clauses.join(" AND ")
    ))
}

fn gen_truncate(
    tables: &[String],
    cascade: bool,
    restart_seqs: bool,
    schema_map: &SchemaMap,
) -> Sql {
    let qualified: Vec<String> = tables
        .iter()
        .map(|t| {
            let parts: Vec<&str> = t.splitn(2, '.').collect();
            if parts.len() == 2 {
                let (ts, tt) = map_table(parts[0], parts[1], schema_map);
                format!("{}.{}", quote_ident(&ts), quote_ident(&tt))
            } else {
                quote_ident(t)
            }
        })
        .collect();

    let mut sql = format!("TRUNCATE {}", qualified.join(", "));
    if restart_seqs {
        sql.push_str(" RESTART IDENTITY");
    }
    if cascade {
        sql.push_str(" CASCADE");
    }
    Sql::Stmt(sql)
}

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn quote_literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        if c == '\'' {
            out.push_str("''");
        } else {
            out.push(c);
        }
    }
    out.push('\'');
    out
}

fn colval_to_sql(val: &ColVal) -> String {
    match val {
        ColVal::Text(s) => quote_literal(s),
        ColVal::Null | ColVal::Unchanged => "NULL".to_string(),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Apply (D4.2): one source WAL transaction applies as one target transaction.
// ─────────────────────────────────────────────────────────────────────────────

/// Executes statements against the target as a single transaction. The fake in
/// tests records the txn boundary; a dead target returns an error that the
/// session surfaces as a reconnect outcome.
#[async_trait]
pub(crate) trait StatementExecutor: Send {
    async fn with_txn(&mut self, statements: &[String]) -> Result<()>;
}

/// Apply one source WAL transaction's events to the target in a single target
/// transaction. Skipped statements are never executed. When every statement is
/// skipped the target is not touched at all.
pub(crate) async fn apply<E: StatementExecutor + ?Sized>(
    events: &[WalEvent],
    schema_map: &SchemaMap,
    executor: &mut E,
) -> Result<()> {
    let statements = gen_statements(events, schema_map);
    if statements.is_empty() {
        return Ok(());
    }
    executor.with_txn(&statements).await
}

struct PgExecutor<'a> {
    client: &'a mut tokio_postgres::Client,
}

#[async_trait]
impl StatementExecutor for PgExecutor<'_> {
    async fn with_txn(&mut self, statements: &[String]) -> Result<()> {
        let txn = self
            .client
            .transaction()
            .await
            .context("Failed to begin transaction on target")?;
        for sql in statements {
            txn.execute(sql, &[])
                .await
                .with_context(|| format!("Failed to execute on target: {sql:.200}"))?;
        }
        txn.commit()
            .await
            .context("Failed to commit transaction on target")?;
        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// The applier: buffers one source transaction's events and applies them at the
// source commit boundary. `batch_size` pre-sizes the buffer; target transaction
// boundaries always follow source boundaries, so a mid-transaction flush is
// never used.
// ─────────────────────────────────────────────────────────────────────────────

pub(crate) struct PostgresApplier {
    client: tokio_postgres::Client,
    buffer: Vec<WalEvent>,
    schema_map: SchemaMap,
}

impl PostgresApplier {
    pub(crate) async fn connect(args: &PostgresArgs) -> Result<Self> {
        let url = args
            .target_url
            .as_deref()
            .context("Postgres sink: --target-url is required (or set PGX_REPLICATE_TARGET_URL)")?;

        let (client, conn) = tokio_postgres::connect(url, tokio_postgres::NoTls)
            .await
            .context("Failed to connect to target PostgreSQL database")?;

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::error!(error = %e, "Target PG connection error");
            }
        });

        let version: String = client.query_one("SELECT version()", &[]).await?.get(0);
        tracing::info!(version = %version, "Connected to target PostgreSQL");

        let mut schema_map = HashMap::new();
        for mapping in &args.schema_map {
            let parts: Vec<&str> = mapping.splitn(2, '=').collect();
            if parts.len() != 2 {
                anyhow::bail!("Invalid schema-map '{mapping}': expected src_schema.src_table=tgt_schema.tgt_table");
            }
            let src_parts: Vec<&str> = parts[0].splitn(2, '.').collect();
            let tgt_parts: Vec<&str> = parts[1].splitn(2, '.').collect();
            if src_parts.len() != 2 || tgt_parts.len() != 2 {
                anyhow::bail!("Invalid schema-map '{mapping}': expected format src_schema.src_table=tgt_schema.tgt_table");
            }
            schema_map.insert(
                (src_parts[0].to_string(), src_parts[1].to_string()),
                (tgt_parts[0].to_string(), tgt_parts[1].to_string()),
            );
        }

        Ok(Self {
            client,
            buffer: Vec::with_capacity(args.batch_size as usize),
            schema_map,
        })
    }

    pub(crate) fn handle_begin(&mut self) {
        self.buffer.clear();
    }

    /// Buffer a forwarded DML / TRUNCATE event for the current source
    /// transaction. SQL is generated later, in `apply`, at the commit boundary.
    pub(crate) async fn handle_event(&mut self, event: &WalEvent) -> Result<()> {
        match event {
            WalEvent::Begin { .. }
            | WalEvent::Commit { .. }
            | WalEvent::Relation { .. }
            | WalEvent::Keepalive { .. } => {}
            _ => self.buffer.push(event.clone()),
        }
        Ok(())
    }

    /// Apply the buffered source transaction to the target as one transaction.
    pub(crate) async fn handle_commit(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let events = std::mem::take(&mut self.buffer);
        let mut executor = PgExecutor {
            client: &mut self.client,
        };
        let result = apply(&events, &self.schema_map, &mut executor).await;
        result.map(|()| tracing::debug!(applied = events.len(), "Applied transaction to target"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::bail;

    fn row(pairs: &[(&str, &str)]) -> Row {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), ColVal::Text(v.to_string())))
            .collect()
    }

    fn row_unchanged(pairs: &[(&str, &str)]) -> Row {
        pairs
            .iter()
            .map(|(k, _)| (k.to_string(), ColVal::Unchanged))
            .collect()
    }

    fn empty_map() -> SchemaMap {
        SchemaMap::new()
    }

    fn insert(schema: &str, table: &str, new: Row) -> WalEvent {
        WalEvent::Insert {
            rel_id: 1,
            schema: schema.to_string(),
            table: table.to_string(),
            new,
        }
    }

    fn update(schema: &str, table: &str, old: Option<Row>, new: Row) -> WalEvent {
        WalEvent::Update {
            rel_id: 1,
            schema: schema.to_string(),
            table: table.to_string(),
            old,
            new,
        }
    }

    fn delete(schema: &str, table: &str, old: Row) -> WalEvent {
        WalEvent::Delete {
            rel_id: 1,
            schema: schema.to_string(),
            table: table.to_string(),
            old,
        }
    }

    // ── SQL generation (D4.1) ────────────────────────────────────────────────

    #[test]
    fn insert_sql_quotes_identifiers_and_values() {
        let event = insert(
            "public",
            "orders",
            row(&[("amount", "100"), ("status", "O'Brien")]),
        );
        let Sql::Stmt(sql) = gen_sql(&event, &empty_map()) else {
            panic!("expected Stmt");
        };
        assert!(sql.starts_with("INSERT INTO \"public\".\"orders\""));
        assert!(sql.contains("(\"amount\", \"status\")"));
        assert!(sql.contains("('100', 'O''Brien')"), "sql: {sql}");
    }

    #[test]
    fn insert_skips_when_all_columns_unchanged() {
        let event = insert("public", "orders", row_unchanged(&[("a", "1")]));
        assert_eq!(
            gen_sql(&event, &empty_map()),
            Sql::Skipped {
                op: "insert",
                reason: "row has no sendable columns (all Unchanged); set REPLICA IDENTITY FULL",
            }
        );
    }

    #[test]
    fn update_sql_uses_old_as_where() {
        let event = update(
            "public",
            "orders",
            Some(row(&[("id", "42")])),
            row(&[("status", "active")]),
        );
        assert_eq!(
            gen_sql(&event, &empty_map()),
            Sql::Stmt(
                "UPDATE \"public\".\"orders\" SET \"status\" = 'active' WHERE \"id\" = '42'"
                    .to_string()
            )
        );
    }

    #[test]
    fn update_skips_without_old_tuple() {
        let event = update("public", "orders", None, row(&[("status", "active")]));
        assert!(matches!(
            gen_sql(&event, &empty_map()),
            Sql::Skipped { op: "update", .. }
        ));
    }

    #[test]
    fn update_skips_when_new_all_unchanged() {
        let event = update(
            "public",
            "orders",
            Some(row(&[("id", "42")])),
            row_unchanged(&[("status", "active")]),
        );
        assert!(matches!(
            gen_sql(&event, &empty_map()),
            Sql::Skipped { op: "update", .. }
        ));
    }

    #[test]
    fn update_skips_when_old_has_no_where_columns() {
        let event = update(
            "public",
            "orders",
            Some(row_unchanged(&[("id", "1")])),
            row(&[("status", "active")]),
        );
        assert!(matches!(
            gen_sql(&event, &empty_map()),
            Sql::Skipped { op: "update", .. }
        ));
    }

    #[test]
    fn delete_sql() {
        let event = delete("public", "orders", row(&[("id", "42")]));
        assert_eq!(
            gen_sql(&event, &empty_map()),
            Sql::Stmt("DELETE FROM \"public\".\"orders\" WHERE \"id\" = '42'".to_string())
        );
    }

    #[test]
    fn delete_skips_when_old_has_no_where_columns() {
        let event = delete("public", "orders", row_unchanged(&[("id", "1")]));
        assert!(matches!(
            gen_sql(&event, &empty_map()),
            Sql::Skipped { op: "delete", .. }
        ));
    }

    #[test]
    fn truncate_sql() {
        let event = WalEvent::Truncate {
            rel_ids: vec![1],
            tables: vec!["public.orders".to_string(), "public.inventory".to_string()],
            cascade: true,
            restart_seqs: true,
        };
        assert_eq!(
            gen_sql(&event, &empty_map()),
            Sql::Stmt(
                "TRUNCATE \"public\".\"orders\", \"public\".\"inventory\" RESTART IDENTITY CASCADE"
                    .to_string()
            )
        );
    }

    #[test]
    fn schema_map_remaps_target_table() {
        let mut m = SchemaMap::new();
        m.insert(
            ("public".to_string(), "orders".to_string()),
            ("archive".to_string(), "orders_2026".to_string()),
        );
        let event = insert("public", "orders", row(&[("id", "1")]));
        let Sql::Stmt(sql) = gen_sql(&event, &m) else {
            panic!("expected Stmt");
        };
        assert!(
            sql.starts_with("INSERT INTO \"archive\".\"orders_2026\""),
            "sql: {sql}"
        );
    }

    #[test]
    fn non_dml_events_are_skipped() {
        let begin = WalEvent::Begin {
            lsn: "0/1".into(),
            commit_time: 0,
            xid: 1,
        };
        assert!(matches!(
            gen_sql(&begin, &empty_map()),
            Sql::Skipped { op: "event", .. }
        ));
    }

    #[test]
    fn gen_statements_drops_skips() {
        let events = vec![
            insert("public", "orders", row_unchanged(&[("a", "1")])),
            update("public", "orders", None, row(&[("status", "x")])),
            insert("public", "orders", row(&[("id", "1")])),
        ];
        let stmts = gen_statements(&events, &empty_map());
        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].starts_with("INSERT INTO"));
    }

    // ── apply / transaction boundaries (D4.2, D4.3) ─────────────────────────

    struct FakeExecutor {
        txns: Vec<Vec<String>>,
    }

    #[async_trait]
    impl StatementExecutor for FakeExecutor {
        async fn with_txn(&mut self, statements: &[String]) -> Result<()> {
            self.txns.push(statements.to_vec());
            Ok(())
        }
    }

    struct DeadExecutor;

    #[async_trait]
    impl StatementExecutor for DeadExecutor {
        async fn with_txn(&mut self, _statements: &[String]) -> Result<()> {
            bail!("target is dead")
        }
    }

    #[tokio::test]
    async fn apply_runs_one_target_txn_per_call() {
        let events = vec![
            insert("public", "orders", row(&[("id", "1")])),
            insert("public", "orders", row(&[("id", "2")])),
            delete("public", "orders", row(&[("id", "3")])),
        ];
        let mut ex = FakeExecutor { txns: Vec::new() };
        apply(&events, &empty_map(), &mut ex).await.unwrap();
        assert_eq!(ex.txns.len(), 1, "one source txn must be one target txn");
        assert_eq!(ex.txns[0].len(), 3);
    }

    #[tokio::test]
    async fn apply_skips_without_opening_a_txn() {
        let events = vec![
            insert("public", "orders", row_unchanged(&[("a", "1")])),
            update("public", "orders", None, row(&[("status", "x")])),
        ];
        let mut ex = FakeExecutor { txns: Vec::new() };
        apply(&events, &empty_map(), &mut ex).await.unwrap();
        assert!(ex.txns.is_empty(), "an all-skipped txn touches no target");
    }

    #[tokio::test]
    async fn apply_surfaces_a_dead_target() {
        let events = vec![insert("public", "orders", row(&[("id", "1")]))];
        let mut ex = DeadExecutor;
        let err = apply(&events, &empty_map(), &mut ex).await.unwrap_err();
        assert!(err.to_string().contains("target is dead"));
    }
}
