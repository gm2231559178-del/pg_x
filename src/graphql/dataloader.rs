use anyhow::Result;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

use super::batch::batch_key;
use super::pool::GlobalDataCache;
use super::runner::QueryRunner;

type Key = String;

/// A per-resolver batch accumulator that eliminates N+1 queries.
/// Optionally backed by a global cross-message cache.
pub struct DataLoader {
    keys: Vec<Key>,
    sql: String,
    batch_by: String,
    result_key: Option<String>,
    resolved: bool,
    cached: HashMap<Key, Vec<Value>>,
    global_cache: Option<Arc<GlobalDataCache>>,
}

impl DataLoader {
    pub fn new(sql: &str, batch_by: &str) -> Self {
        Self {
            keys: Vec::new(),
            sql: sql.to_string(),
            batch_by: batch_by.to_string(),
            result_key: None,
            resolved: false,
            cached: HashMap::new(),
            global_cache: None,
        }
    }

    /// Attach a global cross-message cache. When set, `execute()` will check
    /// the cache before querying and populate it with results.
    pub fn with_global_cache(mut self, cache: Arc<GlobalDataCache>) -> Self {
        self.global_cache = Some(cache);
        self
    }

    /// Set the result column name to match against the batch key.
    /// If not set, defaults to `batch_by`.
    #[allow(dead_code)]
    pub fn with_result_key(mut self, key: &str) -> Self {
        self.result_key = Some(key.to_string());
        self
    }

    /// Add a key value from a parent row to the batch as a JSON Value.
    /// Supports String, Number, Bool - converted to string representation for SQL ANY($1).
    pub fn add_key(&mut self, key: &Value) {
        let s = batch_key(key);
        self.keys.push(s);
    }

    /// Execute the batched SQL query and group results by key.
    /// Checks the global cache first; on cache miss, queries and caches.
    pub async fn execute(&mut self, runner: &dyn QueryRunner) -> Result<()> {
        if self.resolved {
            return Ok(());
        }

        let unique_keys: Vec<String> = {
            let mut seen = std::collections::HashSet::new();
            self.keys
                .iter()
                .filter(|k| seen.insert((*k).clone()))
                .cloned()
                .collect()
        };

        if unique_keys.is_empty() {
            self.resolved = true;
            return Ok(());
        }

        // Check global cache
        if let Some(ref cache) = self.global_cache {
            if let Some(hit) = cache.get(&self.sql, &self.batch_by, &unique_keys) {
                self.cached = hit;
                self.resolved = true;
                return Ok(());
            }
        }

        let rows = runner.run_rows_array(&self.sql, &unique_keys).await?;

        let result_key_col = self.result_key.as_deref().unwrap_or(&self.batch_by);

        for obj in &rows {
            let key = obj.get(result_key_col).map(batch_key).unwrap_or_default();
            self.cached.entry(key).or_default().push(obj.clone());
        }

        // Store in global cache
        if let Some(ref cache) = self.global_cache {
            cache.insert(&self.sql, &self.batch_by, self.cached.clone());
        }

        self.resolved = true;
        Ok(())
    }

    /// Get children for a specific parent key.
    pub fn get_children(&self, key: &str) -> Vec<Value> {
        self.cached.get(key).cloned().unwrap_or_default()
    }
}
