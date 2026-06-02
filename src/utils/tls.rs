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

/// Build a TLS connector. When the `tls` feature is enabled and `use_tls` is
/// true, returns a rustls-based connector. Otherwise returns `NoTls`.
#[allow(unused_variables)]
pub fn build_tls(use_tls: bool) -> anyhow::Result<TlsConnector> {
    #[cfg(feature = "tls")]
    if use_tls {
        let config = rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth();
        return Ok(tokio_postgres_rustls::MakeRustlsConnect::new(config));
    }

    #[cfg(not(feature = "tls"))]
    if use_tls {
        return Err(anyhow::anyhow!(
            "TLS support not enabled. Rebuild with --features tls"
        ));
    }

    #[cfg(feature = "tls")]
    {
        let config = rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth();
        Ok(tokio_postgres_rustls::MakeRustlsConnect::new(config))
    }
    #[cfg(not(feature = "tls"))]
    {
        Ok(tokio_postgres::NoTls)
    }
}
