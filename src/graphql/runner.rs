//! The query-runner seam: a narrow async interface for running resolver SQL.
//! The executor drives every resolver query through this seam, so it can be
//! exercised against an in-memory fake — the same adapter trick the consume
//! session uses for composition.

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;

use super::pool::{GlobalDataCache, QueryConn};
use super::row::row_to_json_value;
use tokio_postgres::types::ToSql;

/// Runs resolver SQL and returns each row as a JSON document.
#[async_trait]
pub trait QueryRunner: Send + Sync {
    /// Execute `sql` bound to `params`, returning each row as a JSON object.
    async fn run_rows(&self, sql: &str, params: &[String]) -> Result<Vec<Value>>;
    /// Execute `sql` binding `values` as a single array parameter (`ANY($1)`).
    async fn run_rows_array(&self, sql: &str, values: &[String]) -> Result<Vec<Value>>;
    /// Optional shared cross-message cache for batched child results.
    fn global_cache(&self) -> Option<Arc<GlobalDataCache>>;
}

#[async_trait]
impl QueryRunner for QueryConn {
    async fn run_rows(&self, sql: &str, params: &[String]) -> Result<Vec<Value>> {
        let param_refs: Vec<&(dyn ToSql + Sync)> =
            params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
        let rows = self.query_cached(sql, &param_refs).await?;
        rows.iter().map(row_to_json_value).collect()
    }

    async fn run_rows_array(&self, sql: &str, values: &[String]) -> Result<Vec<Value>> {
        let keys: Vec<String> = values.to_vec();
        let rows = self.query_cached(sql, &[&keys]).await?;
        rows.iter().map(row_to_json_value).collect()
    }

    fn global_cache(&self) -> Option<Arc<GlobalDataCache>> {
        Some(self.global_cache())
    }
}
