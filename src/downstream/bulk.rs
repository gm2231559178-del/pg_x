use anyhow::{Context, Result};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::warn;

/// Buffers documents and flushes them to Elasticsearch via the `_bulk` API.
/// Threshold-based: flushes when `max_items` is reached.
pub struct BulkBuffer {
    inner: Mutex<Inner>,
    client: reqwest::Client,
    es_url: String,
    max_items: usize,
}

struct Inner {
    lines: Vec<String>,
    item_count: usize,
}

impl BulkBuffer {
    pub fn new(client: reqwest::Client, es_url: String, max_items: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner {
                lines: Vec::with_capacity(max_items * 2),
                item_count: 0,
            }),
            client,
            es_url,
            max_items,
        })
    }

    pub async fn push(self: &Arc<Self>, index: &str, id: Option<&str>, doc: &Value) -> Result<()> {
        let action = if let Some(id) = id {
            format!(r#"{{"index":{{"_index":"{index}","_id":"{id}"}}}}"#)
        } else {
            format!(r#"{{"index":{{"_index":"{index}"}}}}"#)
        };
        let doc_str = serde_json::to_string(doc)?;

        let mut inner = self.inner.lock().await;
        inner.lines.push(action);
        inner.lines.push(doc_str);
        inner.item_count += 1;

        if inner.item_count >= self.max_items {
            drop(inner);
            self.flush_inner().await?;
        }
        Ok(())
    }

    pub async fn flush(self: &Arc<Self>) -> Result<()> {
        self.flush_inner().await
    }

    async fn flush_inner(&self) -> Result<()> {
        let body = {
            let mut inner = self.inner.lock().await;
            if inner.lines.is_empty() {
                return Ok(());
            }
            let body = inner.lines.join("\n") + "\n";
            inner.lines.clear();
            inner.item_count = 0;
            body
        };

        let url = format!("{}/_bulk", self.es_url);
        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/x-ndjson")
            .body(body)
            .send()
            .await
            .with_context(|| format!("ES _bulk POST failed to {}", url))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            anyhow::bail!("ES _bulk failed (HTTP {}): {}", status, text);
        }

        Ok(())
    }
}

/// Background flush task. Runs every `interval_secs` and drains the buffer.
/// Sends `true` via `shutdown_signal` (a `watch::Sender`) to trigger a final flush.
/// When the sender is dropped (struct goes out of scope), the receiver gets `RecvError::Closed`
/// and the task flushes one last time before exiting.
pub fn spawn_bulk_flusher(
    buffer: Arc<BulkBuffer>,
    interval_secs: u64,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
        loop {
            let closed = tokio::select! {
                biased;
                result = shutdown.changed() => result.is_err(),
                _ = ticker.tick() => false,
            };

            if closed || *shutdown.borrow() {
                if let Err(e) = buffer.flush().await {
                    warn!(error = %e, "BulkBuffer final flush failed");
                }
                break;
            }

            if let Err(e) = buffer.flush().await {
                warn!(error = %e, "BulkBuffer periodic flush failed");
            }
        }
    });
}
