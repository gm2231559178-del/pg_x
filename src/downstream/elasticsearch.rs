use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

use super::contract::NotifyEvent;
use super::delivery::elasticsearch::Elasticsearch;
use super::sink::Downstream;
use crate::graphql::{executor, pool::QueryConn, query::QueryLoader};
use crate::utils::config::ResolverConfig;

/// Elasticsearch downstream sink.
/// Receives NOTIFY events with a ContractMessage containing query name and variables,
/// executes the named GraphQL query, and pushes the assembled document to Elasticsearch.
///
/// The query pool, query loader, and resolvers come from the caller; the seam
/// only owns the ES transport (bulk buffer) and the composition step.
pub struct ElasticsearchDownstream {
    index: String,
    id_field: Option<String>,
    max_depth: u32,
    pool: Arc<QueryConn>,
    queries: Arc<QueryLoader>,
    resolvers: Arc<HashMap<String, ResolverConfig>>,
    es: Elasticsearch,
}

impl ElasticsearchDownstream {
    pub fn new(
        es_url: &str,
        index: &str,
        id_field: Option<String>,
        max_depth: u32,
        pool: Arc<QueryConn>,
        queries: Arc<QueryLoader>,
        resolvers: Arc<HashMap<String, ResolverConfig>>,
    ) -> Result<Self> {
        Ok(Self {
            index: index.to_string(),
            id_field,
            max_depth,
            pool,
            queries,
            resolvers,
            es: Elasticsearch::new(es_url)?,
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
            self.pool.as_ref(),
            self.max_depth,
        )
        .await?;

        let doc_id = Elasticsearch::doc_id(self.id_field.as_deref(), &result, None);

        self.es
            .push(&self.index, doc_id.as_deref(), &result)
            .await?;

        Ok(())
    }
}
