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

/// Build a root certificate store populated with Mozilla's root CA certificates.
#[cfg(feature = "tls")]
pub fn build_root_store() -> rustls::RootCertStore {
    let mut root_store = rustls::RootCertStore::empty();
    root_store.add_trust_anchors(webpki_roots::TLS_SERVER_ROOTS.iter().map(|ta| {
        rustls::OwnedTrustAnchor::from_subject_spki_name_constraints(
            ta.subject,
            ta.spki,
            ta.name_constraints,
        )
    }));
    root_store
}

/// Build a TLS connector.
///
/// When the `tls` feature is enabled, the root store is populated with
/// Mozilla's root CA certificates (via `webpki-roots`).
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
            .with_root_certificates(build_root_store())
            .with_no_client_auth();
        Ok(tokio_postgres_rustls::MakeRustlsConnect::new(config))
    }
}
