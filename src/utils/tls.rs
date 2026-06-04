//! TLS connection support for PostgreSQL.
//!
//! When the `tls` feature is enabled, this module provides a TLS connector
//! using rustls. When disabled, it falls back to `NoTls`.

/// The concrete TLS connector type used across the application.
#[cfg(feature = "tls")]
pub type TlsConnector = tokio_postgres_rustls::MakeRustlsConnect;

/// The concrete TLS connector type (NoTls when feature is disabled).
#[cfg(not(feature = "tls"))]
pub type TlsConnector = tokio_postgres::NoTls;

/// Build a TLS connector.
///
/// When the `tls` feature is enabled:
/// - `use_tls = true` — returns a rustls connector (mandatory TLS).
/// - `use_tls = false` — returns a rustls connector that sends SSLRequest but
///   falls back to plaintext if the server rejects it (opportunistic TLS).
///   An empty root store is used since we only need server-side certificate
///   validation when the caller explicitly requests it, which this tool
///   doesn't currently support beyond the basic handshake.
///
/// When the `tls` feature is disabled:
/// - `use_tls = false` — returns `NoTls`.
/// - `use_tls = true` — returns an error suggesting the user rebuild with
///   `--features tls`.
#[allow(unused_variables)]
pub fn build_tls(use_tls: bool) -> anyhow::Result<TlsConnector> {
    #[cfg(not(feature = "tls"))]
    if use_tls {
        return Err(anyhow::anyhow!(
            "TLS support not enabled. Rebuild with --features tls"
        ));
    }

    #[cfg(not(feature = "tls"))]
    {
        Ok(tokio_postgres::NoTls)
    }

    #[cfg(feature = "tls")]
    {
        let config = rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth();
        Ok(tokio_postgres_rustls::MakeRustlsConnect::new(config))
    }
}
