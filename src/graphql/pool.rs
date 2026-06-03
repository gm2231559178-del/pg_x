use anyhow::Result;
use std::sync::Arc;
use tokio_postgres::Client;

use crate::utils::tls;

/// A simple connection pool for resolver queries.
pub struct QueryPool {
    inner: PoolInner,
}

enum PoolInner {
    Single(Arc<Client>),
}

impl QueryPool {
    /// Create a new pool from a database URL.
    pub async fn connect(url: &str, use_tls: bool) -> Result<Self> {
        let connector = tls::build_tls(use_tls)?;
        let (client, connection) = tokio_postgres::connect(url, connector).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!(error = %e, "query pool connection error");
            }
        });
        Ok(Self {
            inner: PoolInner::Single(Arc::new(client)),
        })
    }

    /// Get a client from the pool.
    pub async fn get(&self) -> Result<Arc<Client>> {
        match &self.inner {
            PoolInner::Single(client) => Ok(Arc::clone(client)),
        }
    }
}
