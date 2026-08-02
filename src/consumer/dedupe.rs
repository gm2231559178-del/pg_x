use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// In-memory, TTL-bounded set of recently seen message ids.
///
/// Used by `consume --idempotent` to skip redelivered messages. The cache is
/// per-process; entries expire by age so memory stays bounded by the TTL window.
pub struct DedupCache {
    ttl: Duration,
    inner: Mutex<HashMap<String, Instant>>,
}

impl DedupCache {
    pub fn new(ttl: Duration) -> Arc<Self> {
        Arc::new(Self {
            ttl,
            inner: Mutex::new(HashMap::new()),
        })
    }

    /// Whether `id` is currently recorded and not yet expired.
    pub async fn contains(&self, id: &str) -> bool {
        let mut inner = self.inner.lock().await;
        match inner.get(id) {
            Some(seen) if seen.elapsed() <= self.ttl => true,
            Some(_) => {
                inner.remove(id);
                false
            }
            None => false,
        }
    }

    /// Record `id` as seen, starting its TTL.
    pub async fn record(&self, id: &str) {
        self.inner
            .lock()
            .await
            .insert(id.to_string(), Instant::now());
    }

    /// Drop all entries whose TTL has elapsed.
    pub async fn expire(&self) {
        let mut inner = self.inner.lock().await;
        inner.retain(|_, seen| seen.elapsed() <= self.ttl);
    }
}

/// Background task that periodically sweeps expired entries so the cache stays
/// bounded even for ids that are never seen again.
pub fn spawn_dedup_sweeper(cache: Arc<DedupCache>, interval: Duration) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            cache.expire().await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn records_and_reports_seen_ids() {
        let cache = DedupCache::new(Duration::from_secs(60));
        assert!(!cache.contains("msg-1").await);
        cache.record("msg-1").await;
        assert!(cache.contains("msg-1").await);
    }

    #[tokio::test]
    async fn expired_ids_are_reported_as_not_seen() {
        let cache = DedupCache::new(Duration::from_millis(50));
        cache.record("msg-1").await;
        assert!(cache.contains("msg-1").await);
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(!cache.contains("msg-1").await);
    }

    #[tokio::test]
    async fn expire_removes_only_stale_entries() {
        let cache = DedupCache::new(Duration::from_millis(50));
        cache.record("stale").await;
        tokio::time::sleep(Duration::from_millis(80)).await;
        cache.record("fresh").await;
        cache.expire().await;
        assert!(!cache.contains("stale").await);
        assert!(cache.contains("fresh").await);
    }

    #[tokio::test]
    async fn record_refreshes_an_existing_id() {
        let cache = DedupCache::new(Duration::from_millis(50));
        cache.record("msg-1").await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        cache.record("msg-1").await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(cache.contains("msg-1").await);
    }
}
