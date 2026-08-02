use anyhow::Result;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

use crate::downstream::bulk::{spawn_bulk_flusher, BulkBuffer};

/// Elasticsearch transport handle shared by the sink seams.
///
/// Owns the HTTP client, the bulk buffer, and the periodic flush task; hides
/// buffering and `_id` derivation behind one `push` method.
pub struct Elasticsearch {
    #[allow(dead_code)]
    es_url: String,
    #[allow(dead_code)]
    client: reqwest::Client,
    bulk_buffer: Arc<BulkBuffer>,
    _flush_shutdown: tokio::sync::watch::Sender<bool>,
}

impl Elasticsearch {
    pub fn new(es_url: &str) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;
        let es_url = es_url.trim_end_matches('/').to_string();
        let bulk_buffer = BulkBuffer::new(client.clone(), es_url.clone(), 500);
        let (flush_tx, flush_rx) = tokio::sync::watch::channel(false);
        spawn_bulk_flusher(Arc::clone(&bulk_buffer), 5, flush_rx);

        Ok(Self {
            es_url,
            client,
            bulk_buffer,
            _flush_shutdown: flush_tx,
        })
    }

    /// Derive the ES `_id` for a document: the explicit `--id-field` string
    /// wins when present, then the message id (idempotent mode), then `None`
    /// (ES auto-generates the id).
    pub fn doc_id(id_field: Option<&str>, doc: &Value, msg_id: Option<&str>) -> Option<String> {
        let explicit = id_field.and_then(|idf| match doc {
            Value::Object(m) => m.get(idf).and_then(|v| v.as_str().map(|s| s.to_string())),
            _ => None,
        });
        explicit.or_else(|| msg_id.map(|s| s.to_string()))
    }

    /// Buffer `doc` for the bulk flush under `index`, optionally keyed by
    /// `doc_id`. Flushes eagerly when the buffer reaches its threshold.
    pub async fn push(&self, index: &str, doc_id: Option<&str>, doc: &Value) -> Result<()> {
        self.bulk_buffer.push(index, doc_id, doc).await
    }
}

#[cfg(test)]
mod tests {
    use super::Elasticsearch;
    use serde_json::json;

    #[test]
    fn id_field_wins_over_msg_id() {
        let doc = json!({"id": "abc", "x": 1});
        assert_eq!(
            Elasticsearch::doc_id(Some("id"), &doc, Some("m1")),
            Some("abc".to_string())
        );
    }

    #[test]
    fn id_field_missing_falls_back_to_msg_id() {
        let doc = json!({"x": 1});
        assert_eq!(
            Elasticsearch::doc_id(Some("id"), &doc, Some("m1")),
            Some("m1".to_string())
        );
    }

    #[test]
    fn msg_id_used_when_no_id_field() {
        let doc = json!({"x": 1});
        assert_eq!(
            Elasticsearch::doc_id(None, &doc, Some("m1")),
            Some("m1".to_string())
        );
    }

    #[test]
    fn none_when_no_key_available() {
        let doc = json!({"x": 1});
        assert_eq!(Elasticsearch::doc_id(Some("id"), &doc, None), None);
    }
}
