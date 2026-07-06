use anyhow::Result;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio_postgres::{Client, Row, Statement};

use crate::utils::tls;

type ConnWithCache = (Arc<Client>, Mutex<HashMap<String, Statement>>);

/// A simple round-robin connection pool for resolver queries.
/// Each connection maintains its own prepared statement cache.
pub struct QueryConn {
    clients: Vec<ConnWithCache>,
    next: AtomicUsize,
    global_cache: Arc<GlobalDataCache>,
}

const DEFAULT_POOL_SIZE: usize = 4;
const DEFAULT_CACHE_TTL_SECS: u64 = 60;

impl QueryConn {
    /// Create a pool with `DEFAULT_POOL_SIZE` connections.
    pub async fn connect(url: &str, use_tls: bool) -> Result<Self> {
        Self::connect_with_size(url, use_tls, DEFAULT_POOL_SIZE).await
    }

    /// Create a pool with the given number of connections.
    pub async fn connect_with_size(url: &str, use_tls: bool, pool_size: usize) -> Result<Self> {
        let pool_size = pool_size.max(1);
        let mut clients = Vec::with_capacity(pool_size);

        for i in 0..pool_size {
            let conn = tls::build_tls(use_tls)?;
            let (client, connection) = tokio_postgres::connect(url, conn).await?;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    tracing::error!(error = %e, conn = i, "query pool connection error");
                }
            });
            clients.push((Arc::new(client), Mutex::new(HashMap::new())));
        }

        Ok(Self {
            clients,
            next: AtomicUsize::new(0),
            global_cache: Arc::new(GlobalDataCache::new(Duration::from_secs(
                DEFAULT_CACHE_TTL_SECS,
            ))),
        })
    }

    /// Get the next client from the pool (round-robin).
    #[allow(dead_code)]
    pub async fn get(&self) -> Result<Arc<Client>> {
        if self.clients.is_empty() {
            anyhow::bail!("connection pool is empty");
        }
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        Ok(Arc::clone(&self.clients[idx].0))
    }

    /// Execute a query using a prepared statement cache.
    /// The statement is prepared once per connection and reused.
    pub async fn query_cached(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<Vec<Row>> {
        if self.clients.is_empty() {
            anyhow::bail!("connection pool is empty");
        }
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        let (client, cache) = &self.clients[idx];

        let stmt = {
            let guard = cache.lock().unwrap();
            guard.get(sql).cloned()
        };

        let stmt = match stmt {
            Some(s) => s,
            None => {
                let s = client.prepare(sql).await?;
                let mut guard = cache.lock().unwrap();
                guard.insert(sql.to_string(), s.clone());
                s
            }
        };

        let rows = client.query(&stmt, params).await?;
        Ok(rows)
    }

    /// Number of connections in the pool.
    #[allow(dead_code)]
    pub fn size(&self) -> usize {
        self.clients.len()
    }

    /// Shared cross-message cache reference.
    pub fn global_cache(&self) -> Arc<GlobalDataCache> {
        Arc::clone(&self.global_cache)
    }
}

/// Cross-message cache key: (sql, batch_by_column) -> (key -> children)
type CacheKey = (String, String);

struct CacheEntry {
    children: HashMap<String, Vec<Value>>,
    inserted_at: Instant,
}

/// A TTL-based cache for DataLoader results that persists across messages.
/// When the same resolver SQL + batch_by pair is seen with the same keys,
/// cached results are returned instead of executing a new query.
pub struct GlobalDataCache {
    inner: Mutex<HashMap<CacheKey, CacheEntry>>,
    ttl: Duration,
}

impl GlobalDataCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            ttl,
        }
    }

    /// Look up cached children for the given (sql, batch_by, keys) tuple.
    /// Returns `Some` only if all keys are present and the entry is not expired.
    pub fn get(
        &self,
        sql: &str,
        batch_by: &str,
        keys: &[String],
    ) -> Option<HashMap<String, Vec<Value>>> {
        let cache = self.inner.lock().unwrap();
        let entry = cache.get(&(sql.to_string(), batch_by.to_string()))?;
        if entry.inserted_at.elapsed() > self.ttl {
            return None;
        }
        let result: HashMap<_, _> = keys
            .iter()
            .filter_map(|k| entry.children.get(k).map(|v| (k.clone(), v.clone())))
            .collect();
        if result.len() == keys.len() {
            Some(result)
        } else {
            None
        }
    }

    /// Insert children for the given (sql, batch_by) into the cache.
    pub fn insert(&self, sql: &str, batch_by: &str, children: HashMap<String, Vec<Value>>) {
        let mut cache = self.inner.lock().unwrap();
        cache.insert(
            (sql.to_string(), batch_by.to_string()),
            CacheEntry {
                children,
                inserted_at: Instant::now(),
            },
        );
    }

    /// Remove expired entries.
    #[allow(dead_code)]
    pub fn evict_expired(&self) {
        let mut cache = self.inner.lock().unwrap();
        cache.retain(|_, entry| entry.inserted_at.elapsed() < self.ttl);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn global_cache_hit() {
        let cache = GlobalDataCache::new(Duration::from_secs(60));
        let mut children = HashMap::new();
        children.insert("key1".to_string(), vec![Value::String("val1".into())]);
        children.insert("key2".to_string(), vec![Value::String("val2".into())]);
        cache.insert("SELECT * FROM t WHERE id = ANY($1)", "id", children);

        let result = cache.get(
            "SELECT * FROM t WHERE id = ANY($1)",
            "id",
            &["key1".to_string(), "key2".to_string()],
        );
        assert!(result.is_some());
        let map = result.unwrap();
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn global_cache_miss_key_not_found() {
        let cache = GlobalDataCache::new(Duration::from_secs(60));
        let mut children = HashMap::new();
        children.insert("key1".to_string(), vec![]);
        cache.insert("SELECT * FROM t WHERE id = ANY($1)", "id", children);

        let result = cache.get(
            "SELECT * FROM t WHERE id = ANY($1)",
            "id",
            &["missing".to_string()],
        );
        assert!(result.is_none());
    }

    #[test]
    fn global_cache_miss_different_sql() {
        let cache = GlobalDataCache::new(Duration::from_secs(60));
        cache.insert("SELECT * FROM t1", "id", HashMap::new());

        let result = cache.get("SELECT * FROM t2", "id", &["key1".to_string()]);
        assert!(result.is_none());
    }

    #[test]
    fn global_cache_evict_expired() {
        let cache = GlobalDataCache::new(Duration::from_secs(0)); // TTL = 0 → immediate expiry
        cache.insert("SELECT * FROM t", "id", HashMap::new());

        // Small sleep to let time pass
        std::thread::sleep(Duration::from_millis(10));

        let result = cache.get("SELECT * FROM t", "id", &["key".to_string()]);
        assert!(result.is_none());
    }
}
