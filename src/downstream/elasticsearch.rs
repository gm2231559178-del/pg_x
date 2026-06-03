use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::Serialize;
use std::time::Duration;

use super::contract::NotifyEvent;
use super::sink::Downstream;

/// Elasticsearch downstream sink.
/// Receives NOTIFY events, executes the named GraphQL query, and pushes
/// the assembled document to Elasticsearch.
#[allow(dead_code)]
pub struct ElasticsearchDownstream {
    es_url: String,
    index: String,
    id_field: Option<String>,
    client: reqwest::Client,
    bulk_size: usize,
    flush_interval: Duration,
}

#[allow(dead_code)]
#[derive(Debug, Serialize)]
struct BulkItem {
    id: Option<String>,
    document: serde_json::Value,
}

impl ElasticsearchDownstream {
    #[allow(dead_code)]
    pub fn new(es_url: &str, index: &str, id_field: Option<String>) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;

        Ok(Self {
            es_url: es_url.trim_end_matches('/').to_string(),
            index: index.to_string(),
            id_field,
            client,
            bulk_size: 100,
            flush_interval: Duration::from_millis(500),
        })
    }
}

#[async_trait]
impl Downstream for ElasticsearchDownstream {
    fn name(&self) -> &str {
        "elasticsearch"
    }

    async fn send(&self, event: &NotifyEvent) -> Result<()> {
        // Parse the NOTIFY payload as a ContractMessage
        let msg = match super::contract::ContractMessage::try_parse(&event.payload) {
            Some(m) => m,
            None => {
                anyhow::bail!(
                    "Elasticsearch sink requires a contract-format payload: {}",
                    event.payload
                );
            }
        };

        // The assembled JSON document should be in msg.data
        let document = &msg.data;

        // Determine document ID from a configured root field
        let doc_id = self.id_field.as_ref().and_then(|idf| match document {
            serde_json::Value::Object(m) => {
                m.get(idf).and_then(|v| v.as_str().map(|s| s.to_string()))
            }
            _ => None,
        });

        // POST to ES
        let url = if let Some(ref id) = doc_id {
            format!("{}/{}/_doc/{}", self.es_url, self.index, id)
        } else {
            format!("{}/{}/_doc", self.es_url, self.index)
        };

        let response = self
            .client
            .post(&url)
            .json(document)
            .send()
            .await
            .with_context(|| format!("ES POST failed to {}", url))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            tracing::error!(
                status = %status,
                doc_id = ?doc_id,
                error = %text,
                "ES document index failed"
            );
        }

        Ok(())
    }
}
