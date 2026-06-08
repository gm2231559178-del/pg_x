use anyhow::{Context, Result};
use std::collections::HashMap;

use crate::replication::event::{ColVal, Row, WalEvent};
use super::PostgresArgs;

pub(crate) struct PostgresApplier {
    client: tokio_postgres::Client,
    buffer: Vec<String>,
    schema_map: HashMap<(String, String), (String, String)>,
    batch_size: usize,
    pending_count: usize,
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
            batch_size: args.batch_size as usize,
            pending_count: 0,
        })
    }

    pub(crate) fn handle_begin(&mut self) {
        self.buffer.clear();
        self.pending_count = 0;
    }

    pub(crate) async fn handle_commit(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let txn = self
            .client
            .transaction()
            .await
            .context("Failed to begin transaction on target")?;

        for sql in &self.buffer {
            txn.execute(sql, &[])
                .await
                .with_context(|| format!("Failed to execute on target: {sql:.200}"))?;
        }

        txn.commit()
            .await
            .context("Failed to commit transaction on target")?;

        let count = self.buffer.len();
        tracing::debug!(applied = count, "Applied batch to target");
        self.buffer.clear();
        self.pending_count = 0;
        Ok(())
    }

    pub(crate) async fn handle_event(&mut self, event: &WalEvent) -> Result<()> {
        match event {
            WalEvent::Relation { .. } => {
                Ok(())
            }

            WalEvent::Insert {
                schema, table, new, ..
            } => {
                let sql = gen_insert_sql(schema, table, new, &self.schema_map);
                tracing::trace!(sql = %sql, "Buffering INSERT");
                self.buffer.push(sql);
                self.pending_count += 1;
                if self.pending_count >= self.batch_size {
                    self.handle_commit().await?;
                    self.handle_begin();
                }
                Ok(())
            }

            WalEvent::Update {
                schema,
                table,
                old,
                new,
                ..
            } => match old {
                Some(old_row) => {
                    let sql = gen_update_sql(schema, table, old_row, new, &self.schema_map);
                    tracing::trace!(sql = %sql, "Buffering UPDATE");
                    self.buffer.push(sql);
                    self.pending_count += 1;
                    if self.pending_count >= self.batch_size {
                        self.handle_commit().await?;
                        self.handle_begin();
                    }
                    Ok(())
                }
                None => {
                    tracing::warn!(
                        schema = %schema, table = %table,
                        "Skipping UPDATE without old tuple — set REPLICA IDENTITY FULL on this table"
                    );
                    Ok(())
                }
            },

            WalEvent::Delete {
                schema, table, old, ..
            } => {
                let sql = gen_delete_sql(schema, table, old, &self.schema_map);
                tracing::trace!(sql = %sql, "Buffering DELETE");
                self.buffer.push(sql);
                self.pending_count += 1;
                if self.pending_count >= self.batch_size {
                    self.handle_commit().await?;
                    self.handle_begin();
                }
                Ok(())
            }

            WalEvent::Truncate {
                tables,
                cascade,
                restart_seqs,
                ..
            } => {
                let sql = gen_truncate_sql(tables, *cascade, *restart_seqs, &self.schema_map);
                tracing::debug!(sql = %sql, "Executing TRUNCATE");
                self.client
                    .execute(&sql, &[])
                    .await
                    .with_context(|| format!("Failed to execute TRUNCATE on target: {sql:.200}"))?;
                Ok(())
            }

            WalEvent::Begin { .. } | WalEvent::Commit { .. } | WalEvent::Keepalive { .. } => {
                Ok(())
            }
        }
    }
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

fn gen_insert_sql(
    schema: &str,
    table: &str,
    new: &Row,
    schema_map: &HashMap<(String, String), (String, String)>,
) -> String {
    let (tgt_schema, tgt_table) = schema_map
        .get(&(schema.to_string(), table.to_string()))
        .cloned()
        .unwrap_or_else(|| (schema.to_string(), table.to_string()));

    let mut cols: Vec<&String> = new
        .iter()
        .filter(|(_, v)| !matches!(v, ColVal::Unchanged))
        .map(|(c, _)| c)
        .collect();
    cols.sort();

    if cols.is_empty() {
        tracing::warn!(
            schema = %schema,
            table = %table,
            "INSERT row has no sendable columns — all Unchanged.              Consider ALTER TABLE ... REPLICA IDENTITY FULL."
        );
        let qualified = format!("{}.{}", quote_ident(&tgt_schema), quote_ident(&tgt_table));
        return format!("SELECT 1 WHERE FALSE -- SKIPPED INSERT {qualified} (no columns)");
    }

    let col_names: Vec<String> = cols.iter().map(|c| quote_ident(c)).collect();
    let col_vals: Vec<String> = cols.iter().map(|c| colval_to_sql(&new[*c])).collect();

    format!(
        "INSERT INTO {}.{} ({}) VALUES ({})",
        quote_ident(&tgt_schema),
        quote_ident(&tgt_table),
        col_names.join(", "),
        col_vals.join(", "),
    )
}

fn gen_update_sql(
    schema: &str,
    table: &str,
    old: &Row,
    new: &Row,
    schema_map: &HashMap<(String, String), (String, String)>,
) -> String {
    let (tgt_schema, tgt_table) = schema_map
        .get(&(schema.to_string(), table.to_string()))
        .cloned()
        .unwrap_or_else(|| (schema.to_string(), table.to_string()));

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

    let where_clauses: Vec<String> = old
        .iter()
        .filter(|(_, v)| !matches!(v, ColVal::Unchanged))
        .map(|(c, v)| format!("{} = {}", quote_ident(c), colval_to_sql(v)))
        .collect();

    if set_clauses.is_empty() {
        tracing::warn!(
            schema = %schema,
            table = %table,
            "Skipping UPDATE: no sendable columns in new tuple — all Unchanged."
        );
        return format!("SELECT 1 WHERE FALSE -- SKIPPED UPDATE {qualified}");
    }

    if where_clauses.is_empty() {
        tracing::warn!(
            schema = %schema,
            table = %table,
            "Skipping UPDATE: no usable WHERE columns in old tuple.              Run ALTER TABLE ... REPLICA IDENTITY FULL to fix this."
        );
        return format!("SELECT 1 WHERE FALSE -- SKIPPED UPDATE {qualified}");
    }

    format!(
        "UPDATE {} SET {} WHERE {}",
        qualified,
        set_clauses.join(", "),
        where_clauses.join(" AND "),
    )
}

fn gen_delete_sql(
    schema: &str,
    table: &str,
    old: &Row,
    schema_map: &HashMap<(String, String), (String, String)>,
) -> String {
    let (tgt_schema, tgt_table) = schema_map
        .get(&(schema.to_string(), table.to_string()))
        .cloned()
        .unwrap_or_else(|| (schema.to_string(), table.to_string()));

    let qualified = format!("{}.{}", quote_ident(&tgt_schema), quote_ident(&tgt_table));

    let where_clauses: Vec<String> = old
        .iter()
        .filter(|(_, v)| !matches!(v, ColVal::Unchanged))
        .map(|(c, v)| format!("{} = {}", quote_ident(c), colval_to_sql(v)))
        .collect();

    if where_clauses.is_empty() {
        tracing::warn!(
            schema = %schema,
            table = %table,
            "Skipping DELETE: no usable WHERE columns in old tuple.              Run ALTER TABLE ... REPLICA IDENTITY FULL to fix this."
        );
        return format!("SELECT 1 WHERE FALSE -- SKIPPED DELETE {qualified}");
    }

    format!(
        "DELETE FROM {} WHERE {}",
        qualified,
        where_clauses.join(" AND ")
    )
}

fn gen_truncate_sql(
    tables: &[String],
    cascade: bool,
    restart_seqs: bool,
    schema_map: &HashMap<(String, String), (String, String)>,
) -> String {
    let qualified: Vec<String> = tables
        .iter()
        .map(|t| {
            let parts: Vec<&str> = t.splitn(2, '.').collect();
            if parts.len() == 2 {
                let (ts, tt) = schema_map
                    .get(&(parts[0].to_string(), parts[1].to_string()))
                    .cloned()
                    .unwrap_or_else(|| (parts[0].to_string(), parts[1].to_string()));
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
    sql
}
