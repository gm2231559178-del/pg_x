use anyhow::Result;
use std::collections::HashMap;

use super::pool::QueryPool;
use super::row::cell_as_string;

/// A per-resolver batch accumulator that eliminates N+1 queries.
pub struct DataLoader {
    /// Accumulated key values for the current batch
    keys: Vec<String>,
    /// The resolver SQL with a `WHERE ... = ANY($1)` clause
    sql: String,
    /// The column name to extract from parent rows for batching
    batch_by: String,
    /// The column name in the child result to match against the batch key
    result_key: Option<String>,
    /// Whether a key has already been resolved
    resolved: bool,
    /// Cached result: key -> Vec of JSON rows
    cached: HashMap<String, Vec<serde_json::Value>>,
}

#[allow(dead_code)]
impl DataLoader {
    pub fn new(sql: &str, batch_by: &str) -> Self {
        Self {
            keys: Vec::new(),
            sql: sql.to_string(),
            batch_by: batch_by.to_string(),
            result_key: None,
            resolved: false,
            cached: HashMap::new(),
        }
    }

    /// Set the result column name to match against the batch key.
    /// If not set, defaults to `batch_by`.
    pub fn with_result_key(mut self, key: &str) -> Self {
        self.result_key = Some(key.to_string());
        self
    }

    /// Add a key value from a parent row to the batch.
    pub fn add_key(&mut self, key: String) {
        self.keys.push(key);
    }

    /// Return the number of unique keys accumulated.
    pub fn unique_key_count(&self) -> usize {
        let mut unique = std::collections::HashSet::new();
        for k in &self.keys {
            unique.insert(k.clone());
        }
        unique.len()
    }

    /// Execute the batched SQL query and group results by key.
    pub async fn execute(&mut self, pool: &QueryPool) -> Result<()> {
        if self.resolved {
            return Ok(());
        }

        // Deduplicate keys
        let unique_keys: Vec<String> = {
            let mut seen = std::collections::HashSet::new();
            self.keys
                .iter()
                .filter(|k| seen.insert((*k).clone()))
                .cloned()
                .collect()
        };

        let unique_key_refs: Vec<&str> = unique_keys.iter().map(|s| s.as_str()).collect();

        if unique_keys.is_empty() {
            self.resolved = true;
            return Ok(());
        }

        let client = pool.get().await?;
        let rows = client.query(&self.sql, &[&unique_key_refs]).await?;

        // Determine the result key column: use result_key or fall back to batch_by
        let result_key_col = self.result_key.as_deref().unwrap_or(&self.batch_by);

        for row in &rows {
            let key = cell_as_string(row, result_key_col).unwrap_or_default();
            let json_row = super::row::row_to_json_value(row)?;
            self.cached.entry(key).or_default().push(json_row);
        }

        self.resolved = true;
        Ok(())
    }

    /// Get children for a specific parent key.
    pub fn get_children(&self, key: &str) -> Vec<serde_json::Value> {
        self.cached.get(key).cloned().unwrap_or_default()
    }
}
