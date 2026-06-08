use anyhow::Result;
use std::sync::Arc;
use tokio_postgres::Client;

use crate::utils::tls;

/// A single-connection wrapper for resolver queries.
/// Not a pool — holds exactly one connection. If the connection drops,
/// the caller must re-create.
pub struct QueryConn {
    inner: ConnInner,
}

enum ConnInner {
    Single(Arc<Client>),
}

impl QueryConn {
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
            inner: ConnInner::Single(Arc::new(client)),
        })
    }

    /// Get the client handle.
    pub async fn get(&self) -> Result<Arc<Client>> {
        match &self.inner {
            ConnInner::Single(client) => Ok(Arc::clone(client)),
        }
    }
}
