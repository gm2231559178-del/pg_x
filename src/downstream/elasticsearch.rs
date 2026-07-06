use anyhow::{Context, Result};
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use super::bulk::{spawn_bulk_flusher, BulkBuffer};
use super::contract::NotifyEvent;
use super::sink::Downstream;
use crate::graphql::{executor, pool::QueryConn, query::QueryLoader, schema::SchemaRegistry};
use crate::utils::config::ResolverConfig;

/// Elasticsearch downstream sink.
/// Receives NOTIFY events with a ContractMessage containing query name and variables,
/// executes the named GraphQL query, and pushes the assembled document to Elasticsearch.
pub struct ElasticsearchDownstream {
    #[allow(dead_code)]
    es_url: String,
    index: String,
    id_field: Option<String>,
    #[allow(dead_code)]
    client: reqwest::Client,
    pool: QueryConn,
    queries: QueryLoader,
    resolvers: HashMap<String, ResolverConfig>,
    max_depth: u32,
    bulk_buffer: Arc<BulkBuffer>,
    _flush_shutdown: tokio::sync::watch::Sender<bool>,
}

impl ElasticsearchDownstream {
    pub fn new(
        es_url: &str,
        index: &str,
        id_field: Option<String>,
        max_depth: u32,
        pool: QueryConn,
        resolvers: HashMap<String, ResolverConfig>,
        schema_dir: Option<PathBuf>,
    ) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;

        let schema = match schema_dir {
            Some(ref p) => SchemaRegistry::load_from_dir(p)?,
            None => {
                let home = dirs::home_dir().context("Cannot determine home directory")?;
                let p = home.join(".pgx").join("schema");
                SchemaRegistry::load_from_dir(&p)?
            }
        };
        let queries = QueryLoader::load(&schema)?;

        let es_url = es_url.trim_end_matches('/').to_string();
        let bulk_buffer = BulkBuffer::new(client.clone(), es_url.clone(), 500);
        let (flush_tx, flush_rx) = tokio::sync::watch::channel(false);
        spawn_bulk_flusher(Arc::clone(&bulk_buffer), 5, flush_rx);

        Ok(Self {
            es_url,
            index: index.to_string(),
            id_field,
            max_depth,
            client,
            pool,
            queries,
            resolvers,
            bulk_buffer,
            _flush_shutdown: flush_tx,
        })
    }
}

#[async_trait]
impl Downstream for ElasticsearchDownstream {
    fn name(&self) -> &str {
        "elasticsearch"
    }

    async fn send(&self, event: &NotifyEvent) -> Result<()> {
        let msg = match super::contract::ContractMessage::try_parse(&event.payload) {
            Some(m) => m,
            None => {
                anyhow::bail!(
                    "Elasticsearch sink requires a contract-format payload: {}",
                    event.payload
                );
            }
        };

        // Extract query name from event_type or routing info
        let query_name = msg.meta.event_type.as_deref().unwrap_or("default");

        // Convert msg.data into a variable map (top-level keys become variables)
        let variables: HashMap<String, serde_json::Value> = match &msg.data {
            serde_json::Value::Object(m) => m.clone().into_iter().collect(),
            other => {
                let mut h = HashMap::new();
                h.insert("data".to_string(), other.clone());
                h
            }
        };

        let query = self
            .queries
            .get(query_name)
            .ok_or_else(|| anyhow::anyhow!("No named query '{}' found for ES sink", query_name))?;

        let result: serde_json::Value = executor::execute(
            query,
            &variables,
            &self.resolvers,
            &self.pool,
            self.max_depth,
        )
        .await?;

        let doc_id = self.id_field.as_ref().and_then(|idf| match &result {
            serde_json::Value::Object(m) => {
                m.get(idf).and_then(|v| v.as_str().map(|s| s.to_string()))
            }
            _ => None,
        });

        self.bulk_buffer
            .push(&self.index, doc_id.as_deref(), &result)
            .await?;

        Ok(())
    }
}
