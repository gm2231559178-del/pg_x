use anyhow::{Context, Result};
use tokio_postgres::Client;

use super::tls;

const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Connect to Postgres using a URL, optionally with TLS.
/// Returns a connected `tokio_postgres::Client`.
/// The connection task is spawned on the Tokio runtime and runs in the background.
pub async fn connect(url: &str, use_tls: bool) -> Result<Client> {
    let connector = tls::build_tls(use_tls)?;
    let (client, connection) =
        tokio::time::timeout(CONNECT_TIMEOUT, tokio_postgres::connect(url, connector))
            .await
            .map_err(|_| {
                anyhow::anyhow!("Connection timed out after {}s", CONNECT_TIMEOUT.as_secs())
            })?
            .with_context(|| format!("Failed to connect to: {url}"))?;

    // Spawn the connection driver; it will log errors via tracing
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!(error = %e, "postgres connection error");
        }
    });

    Ok(client)
}
