//! PostgreSQL logical replication client.
//!
//! Implements the full replication wire protocol: startup, auth (cleartext,
//! SCRAM-SHA-256), START_REPLICATION, CopyBoth streaming, and periodic
//! StandbyStatusUpdate keepalives.
//!
//! This is a self-contained implementation that does not depend on any external
//! replication crate.

use bytes::BytesMut;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::io::{AsyncRead, AsyncWrite, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::{
    error::{ReplError, ReplResult},
    framing::{
        read_backend_message_into, write_copy_data, write_copy_done, write_password_message,
        write_query, write_startup_message, BackendMessage,
    },
    lsn::Lsn,
    messages::{parse_auth_request, parse_error_response, parse_sasl_mechanisms},
    proto::{
        current_pg_timestamp, encode_standby_status_update, parse_copy_data,
        parse_pgoutput_boundary, PgOutputBoundary, ReplicationCopyData,
    },
    scram::ScramClient,
};

// ─────────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct ReplicationConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub slot: String,
    pub publication: String,
    pub start_lsn: Lsn,
    pub temporary: bool,
    pub use_tls: bool,
    pub status_interval_secs: u64,
    pub idle_wakeup_secs: u64,
    pub buffer_events: usize,
}

impl std::fmt::Debug for ReplicationConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicationConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("password", &"<redacted>")
            .field("database", &self.database)
            .field("slot", &self.slot)
            .field("publication", &self.publication)
            .field("start_lsn", &self.start_lsn)
            .field("temporary", &self.temporary)
            .field("use_tls", &self.use_tls)
            .field("status_interval_secs", &self.status_interval_secs)
            .field("idle_wakeup_secs", &self.idle_wakeup_secs)
            .field("buffer_events", &self.buffer_events)
            .finish()
    }
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 5432,
            user: "postgres".into(),
            password: String::new(),
            database: "postgres".into(),
            slot: "pgx_slot".into(),
            publication: "pgx_pub".into(),
            start_lsn: Lsn::ZERO,
            temporary: false,
            use_tls: false,
            status_interval_secs: 10,
            idle_wakeup_secs: 10,
            buffer_events: 8192,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Events
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum ReplicationEvent {
    /// Server keepalive (already acknowledged internally).
    KeepAlive { wal_end: Lsn },
    /// Start of a transaction.
    Begin {
        final_lsn: Lsn,
        xid: u32,
        commit_time: i64,
    },
    /// Raw WAL data (pgoutput bytes for Insert/Update/Delete/Relation/etc.).
    XLogData {
        #[allow(dead_code)]
        wal_start: Lsn,
        wal_end: Lsn,
        data: bytes::Bytes,
    },
    /// End of a transaction.
    Commit {
        lsn: Lsn,
        end_lsn: Lsn,
        commit_time: i64,
    },
}

pub type ReplicationEventReceiver = mpsc::Receiver<ReplResult<ReplicationEvent>>;

// ─────────────────────────────────────────────────────────────────────────────
// Shared progress (atomic, cheap to update from user code)
// ─────────────────────────────────────────────────────────────────────────────

pub struct SharedProgress {
    applied: AtomicU64,
}

impl SharedProgress {
    fn new(start: Lsn) -> Self {
        Self {
            applied: AtomicU64::new(start.as_u64()),
        }
    }

    pub fn load_applied(&self) -> Lsn {
        Lsn::from_u64(self.applied.load(Ordering::Acquire))
    }

    /// Monotonic update — lower LSNs are silently ignored.
    pub fn update_applied(&self, lsn: Lsn) {
        let new = lsn.as_u64();
        let mut cur = self.applied.load(Ordering::Relaxed);
        while new > cur {
            match self
                .applied
                .compare_exchange_weak(cur, new, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(observed) => cur = observed,
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Public client handle
// ─────────────────────────────────────────────────────────────────────────────

pub struct ReplicationClient {
    rx: ReplicationEventReceiver,
    progress: Arc<SharedProgress>,
    stop_tx: watch::Sender<bool>,
    join: Option<JoinHandle<ReplResult<()>>>,
}

impl ReplicationClient {
    /// Connect and start the background streaming worker.
    pub async fn connect(cfg: ReplicationConfig) -> anyhow::Result<Self> {
        let (tx, rx) = mpsc::channel(cfg.buffer_events);
        let progress = Arc::new(SharedProgress::new(cfg.start_lsn));
        let (stop_tx, stop_rx) = watch::channel(false);

        let progress_w = Arc::clone(&progress);
        let cfg_w = cfg.clone();

        let join = tokio::spawn(async move {
            let mut worker = Worker::new(cfg_w, progress_w, stop_rx, tx);
            worker.run().await
        });

        Ok(Self {
            rx,
            progress,
            stop_tx,
            join: Some(join),
        })
    }

    /// Receive the next event.
    ///
    /// - `Ok(Some(ev))` — got an event
    /// - `Ok(None)` — stream closed cleanly
    /// - `Err(e)` — replication error
    pub async fn recv(&mut self) -> anyhow::Result<Option<ReplicationEvent>> {
        match self.rx.recv().await {
            Some(Ok(ev)) => Ok(Some(ev)),
            Some(Err(e)) => Err(anyhow::anyhow!("{e}")),
            None => self.collect_worker_result().await,
        }
    }

    /// Report the last LSN the caller has durably handled.
    ///
    /// The worker will include this in periodic StandbyStatusUpdate messages
    /// so PostgreSQL can reclaim WAL segments.
    #[inline]
    pub fn update_applied_lsn(&self, lsn: Lsn) {
        self.progress.update_applied(lsn);
    }

    /// Return the last LSN that has been durably confirmed by the caller.
    ///
    /// Used to seed `start_lsn` when reconnecting after a drop so the slot
    /// resumes from the last acknowledged position rather than from the
    /// originally requested start LSN.
    #[inline]
    pub fn last_applied_lsn(&self) -> Lsn {
        self.progress.load_applied()
    }

    /// Request graceful shutdown.
    pub fn stop(&self) {
        let _ = self.stop_tx.send(true);
    }

    async fn collect_worker_result(&mut self) -> anyhow::Result<Option<ReplicationEvent>> {
        let join = match self.join.take() {
            Some(j) => j,
            None => return Ok(None),
        };
        match join.await {
            Ok(Ok(())) => Ok(None),
            Ok(Err(e)) => Err(anyhow::anyhow!("replication worker: {e}")),
            Err(e) => Err(anyhow::anyhow!("replication worker panicked: {e}")),
        }
    }
}

impl Drop for ReplicationClient {
    fn drop(&mut self) {
        let _ = self.stop_tx.send(true);
        if let Some(join) = self.join.take() {
            if tokio::runtime::Handle::try_current().is_err() {
                // No runtime active — abort the worker task.
                join.abort();
            }
            // With a runtime active, the worker task runs until it notices the
            // stop signal and terminates cleanly. We cannot block here in an
            // async context, so we detach. The recv() caller will pick up the
            // result via collect_worker_result().
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Replication protocol — the wire protocol over any AsyncRead + AsyncWrite
// stream, decoupled from TCP/TLS connection management so it can be driven
// against a scripted in-memory peer in tests.
// ─────────────────────────────────────────────────────────────────────────────

/// The PostgreSQL replication wire protocol over an arbitrary
/// `AsyncRead + AsyncWrite` stream (`TcpStream`, or a TLS stream under the
/// `tls` feature).
///
/// Owns all wire-level work: startup, authentication (cleartext +
/// SCRAM-SHA-256), temporary slot creation, START_REPLICATION, and the
/// CopyBoth streaming loop with periodic StandbyStatusUpdate feedback.
pub struct ReplicationProtocol<S> {
    stream: BufReader<S>,
    read_buf: BytesMut,
    cfg: ReplicationConfig,
    progress: Arc<SharedProgress>,
    stop_rx: watch::Receiver<bool>,
    out: mpsc::Sender<ReplResult<ReplicationEvent>>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> ReplicationProtocol<S> {
    pub fn new(
        stream: S,
        cfg: ReplicationConfig,
        progress: Arc<SharedProgress>,
        stop_rx: watch::Receiver<bool>,
        out: mpsc::Sender<ReplResult<ReplicationEvent>>,
    ) -> Self {
        Self {
            stream: BufReader::with_capacity(128 * 1024, stream),
            read_buf: BytesMut::with_capacity(4096),
            cfg,
            progress,
            stop_rx,
            out,
        }
    }

    /// Run the full session: startup, auth, optional temporary slot,
    /// START_REPLICATION, then the streaming loop until shutdown or error.
    pub async fn run(&mut self) -> ReplResult<()> {
        self.startup().await?;
        self.authenticate().await?;
        if self.cfg.temporary {
            self.create_temp_slot().await?;
        }
        self.start_replication().await?;
        self.stream_loop().await
    }

    // ── Startup ───────────────────────────────────────────────────────────────

    async fn startup(&mut self) -> ReplResult<()> {
        write_startup_message(
            &mut self.stream,
            196608,
            &[
                ("user", self.cfg.user.as_str()),
                ("database", self.cfg.database.as_str()),
                ("replication", "database"),
                ("client_encoding", "UTF8"),
                ("application_name", "pgx-replicate"),
            ],
        )
        .await
    }

    // ── Authentication ────────────────────────────────────────────────────────

    async fn authenticate(&mut self) -> ReplResult<()> {
        loop {
            let msg = self.read_message().await?;
            match msg.tag {
                b'R' => {
                    let (code, rest) = parse_auth_request(&msg.payload)?;
                    self.handle_auth(code, rest).await?;
                }
                b'E' => return Err(ReplError::Server(parse_error_response(&msg.payload))),
                b'S' | b'K' => {}      // ParameterStatus, BackendKeyData — ignore
                b'Z' => return Ok(()), // ReadyForQuery — done
                _ => {}
            }
        }
    }

    async fn handle_auth(&mut self, code: i32, data: &[u8]) -> ReplResult<()> {
        match code {
            0 => Ok(()), // AuthenticationOk
            3 => {
                tracing::warn!(
                    "Server requested cleartext password — password will be sent in plain text. Use --tls to protect credentials"
                );
                let mut payload = self.cfg.password.as_bytes().to_vec();
                payload.push(0);
                write_password_message(&mut self.stream, &payload).await
            }
            10 => {
                // SASL — try SCRAM-SHA-256
                let mechanisms = parse_sasl_mechanisms(data);
                if !mechanisms.iter().any(|m| m == "SCRAM-SHA-256") {
                    return Err(ReplError::Auth(format!(
                        "server offers {mechanisms:?} but SCRAM-SHA-256 is required"
                    )));
                }
                self.auth_scram().await
            }
            _ => Err(ReplError::Auth(format!("unsupported auth method: {code}"))),
        }
    }

    async fn auth_scram(&mut self) -> ReplResult<()> {
        let scram = ScramClient::new(&self.cfg.user);

        // SASLInitialResponse: mechanism name + client-first
        let mut init = Vec::new();
        init.extend_from_slice(b"SCRAM-SHA-256\0");
        init.extend_from_slice(&(scram.client_first.len() as i32).to_be_bytes());
        init.extend_from_slice(scram.client_first.as_bytes());
        write_password_message(&mut self.stream, &init).await?;

        // AuthenticationSASLContinue (code 11)
        let server_first = self.read_auth_data(11).await?;
        let server_first_str = String::from_utf8_lossy(&server_first);

        // Compute and send client-final
        let (client_final, auth_message, salted_password) =
            scram.client_final(&self.cfg.password, &server_first_str)?;
        write_password_message(&mut self.stream, client_final.as_bytes()).await?;

        // AuthenticationSASLFinal (code 12) — verify server signature
        let server_final = self.read_auth_data(12).await?;
        let server_final_str = String::from_utf8_lossy(&server_final);
        ScramClient::verify_server_final(&server_final_str, &salted_password, &auth_message)?;

        Ok(())
    }

    async fn read_auth_data(&mut self, expected_code: i32) -> ReplResult<Vec<u8>> {
        loop {
            let msg = self.read_message().await?;
            match msg.tag {
                b'R' => {
                    let (code, data) = parse_auth_request(&msg.payload)?;
                    if code == expected_code {
                        return Ok(data.to_vec());
                    }
                    return Err(ReplError::Auth(format!(
                        "unexpected auth code {code}, expected {expected_code}"
                    )));
                }
                b'E' => return Err(ReplError::Server(parse_error_response(&msg.payload))),
                _ => {}
            }
        }
    }

    // ── Start replication ─────────────────────────────────────────────────────

    async fn start_replication(&mut self) -> ReplResult<()> {
        let slot_escaped = self.cfg.slot.replace('"', "\"\"");
        let pub_escaped = self.cfg.publication.replace('\'', "''");
        let sql = format!(
            "START_REPLICATION SLOT \"{slot_escaped}\" LOGICAL {} \
             (proto_version '1', publication_names '{pub_escaped}', messages 'true')",
            self.cfg.start_lsn
        );
        write_query(&mut self.stream, &sql).await?;

        // Wait for CopyBothResponse ('W')
        loop {
            let msg = self.read_message().await?;
            match msg.tag {
                b'W' => return Ok(()), // CopyBothResponse — streaming begins
                b'E' => return Err(ReplError::Server(parse_error_response(&msg.payload))),
                b'N' | b'S' | b'K' => {} // Notice, ParameterStatus, BackendKeyData
                _ => {}
            }
        }
    }

    // ── Temporary slot creation ───────────────────────────────────────────────

    async fn create_temp_slot(&mut self) -> ReplResult<()> {
        let escaped = self.cfg.slot.replace('"', "\"\"");
        let sql = format!(
            "CREATE_REPLICATION_SLOT \"{escaped}\" TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT"
        );
        write_query(&mut self.stream, &sql).await?;
        loop {
            let msg = self.read_message().await?;
            match msg.tag {
                b'Z' => return Ok(()),
                b'E' => return Err(ReplError::Server(parse_error_response(&msg.payload))),
                _ => {}
            }
        }
    }

    // ── Main stream loop ──────────────────────────────────────────────────────

    async fn stream_loop(&mut self) -> ReplResult<()> {
        let status_interval = std::time::Duration::from_secs(self.cfg.status_interval_secs);
        let idle_wakeup = std::time::Duration::from_secs(self.cfg.idle_wakeup_secs);

        let mut last_status = Instant::now() - status_interval;
        let mut last_applied = self.progress.load_applied();

        const DRAIN_BATCH: usize = 256;

        loop {
            // Sync applied LSN from client
            let current_applied = self.progress.load_applied();
            if current_applied != last_applied {
                last_applied = current_applied;
            }

            // Periodic status feedback
            if last_status.elapsed() >= status_interval {
                self.send_feedback(last_applied, false).await?;
                last_status = Instant::now();
            }

            // ── Drain phase: tight loop while BufReader has buffered data ─────
            let mut drained = 0;
            while self.stream.buffer().len() >= 5 && drained < DRAIN_BATCH {
                let msg = self.read_message().await?;
                drained += 1;
                match msg.tag {
                    b'd' => {
                        if self
                            .handle_copy_data(msg.payload, &mut last_applied, &mut last_status)
                            .await?
                        {
                            return Ok(());
                        }
                    }
                    b'E' => return Err(ReplError::Server(parse_error_response(&msg.payload))),
                    _ => {}
                }
            }

            if drained > 0 {
                if self.stop_rx.has_changed().unwrap_or(false) && *self.stop_rx.borrow() {
                    let _ = self.send_copy_done().await;
                    return Ok(());
                }
                continue;
            }

            // ── Wait phase: select on socket vs stop signal vs idle timeout ───
            let msg = tokio::select! {
                biased;

                _ = self.stop_rx.changed() => {
                    if *self.stop_rx.borrow() {
                        let _ = self.send_copy_done().await;
                        return Ok(());
                    }
                    continue;
                }

                res = tokio::time::timeout(
                    idle_wakeup,
                    read_backend_message_into(&mut self.stream, &mut self.read_buf),
                ) => {
                    match res {
                        Ok(inner) => inner?,
                        Err(_) => {
                            // Idle timeout — send status update and continue
                            let applied = self.progress.load_applied();
                            last_applied = applied;
                            self.send_feedback(applied, false).await?;
                            last_status = Instant::now();
                            continue;
                        }
                    }
                }
            };

            match msg.tag {
                b'd' => {
                    if self
                        .handle_copy_data(msg.payload, &mut last_applied, &mut last_status)
                        .await?
                    {
                        return Ok(());
                    }
                }
                b'E' => return Err(ReplError::Server(parse_error_response(&msg.payload))),
                _ => {}
            }
        }
    }

    /// Handle one CopyData message. Returns `true` if the stream should stop.
    async fn handle_copy_data(
        &mut self,
        payload: bytes::Bytes,
        last_applied: &mut Lsn,
        last_status: &mut Instant,
    ) -> ReplResult<bool> {
        let cd = parse_copy_data(payload)?;
        match cd {
            ReplicationCopyData::KeepAlive {
                wal_end,
                reply_requested,
                ..
            } => {
                if reply_requested {
                    let applied = self.progress.load_applied();
                    *last_applied = applied;
                    self.send_feedback(applied, true).await?;
                    *last_status = Instant::now();
                }
                self.emit(Ok(ReplicationEvent::KeepAlive { wal_end })).await;
                Ok(false)
            }

            ReplicationCopyData::XLogData {
                wal_start,
                wal_end,
                data,
                ..
            } => {
                // Check if this is a Begin or Commit boundary message —
                // the worker surfaces those as typed events for convenience.
                if let Some(boundary) = parse_pgoutput_boundary(&data)? {
                    match boundary {
                        PgOutputBoundary::Begin {
                            final_lsn,
                            xid,
                            commit_time,
                        } => {
                            self.emit(Ok(ReplicationEvent::Begin {
                                final_lsn,
                                xid,
                                commit_time,
                            }))
                            .await;
                        }
                        PgOutputBoundary::Commit {
                            lsn,
                            end_lsn,
                            commit_time,
                        } => {
                            self.emit(Ok(ReplicationEvent::Commit {
                                lsn,
                                end_lsn,
                                commit_time,
                            }))
                            .await;
                        }
                    }
                    return Ok(false);
                }

                // All other XLogData (Insert, Update, Delete, Relation, etc.)
                // are forwarded as raw bytes for the pgoutput decoder in replicate.rs.
                self.emit(Ok(ReplicationEvent::XLogData {
                    wal_start,
                    wal_end,
                    data,
                }))
                .await;
                Ok(false)
            }
        }
    }

    async fn read_message(&mut self) -> ReplResult<BackendMessage> {
        read_backend_message_into(&mut self.stream, &mut self.read_buf).await
    }

    async fn emit(&self, ev: ReplResult<ReplicationEvent>) {
        if self.out.send(ev).await.is_err() {
            // Channel closed — client disconnected
        }
    }

    async fn send_copy_done(&mut self) -> ReplResult<()> {
        write_copy_done(&mut self.stream).await
    }

    async fn send_feedback(&mut self, applied: Lsn, reply_requested: bool) -> ReplResult<()> {
        let ts = current_pg_timestamp();
        let payload = encode_standby_status_update(applied, ts, reply_requested);
        write_copy_data(&mut self.stream, &payload).await
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Background worker — owns the connection (TCP/TLS) and drives the protocol
// ─────────────────────────────────────────────────────────────────────────────

struct Worker {
    cfg: ReplicationConfig,
    progress: Arc<SharedProgress>,
    stop_rx: watch::Receiver<bool>,
    out: mpsc::Sender<ReplResult<ReplicationEvent>>,
}

impl Worker {
    fn new(
        cfg: ReplicationConfig,
        progress: Arc<SharedProgress>,
        stop_rx: watch::Receiver<bool>,
        out: mpsc::Sender<ReplResult<ReplicationEvent>>,
    ) -> Self {
        Self {
            cfg,
            progress,
            stop_rx,
            out,
        }
    }

    async fn run(&mut self) -> ReplResult<()> {
        let tcp = TcpStream::connect((self.cfg.host.as_str(), self.cfg.port)).await?;
        tcp.set_nodelay(true)?;

        #[cfg(feature = "tls")]
        if self.cfg.use_tls {
            let tls_stream = self.negotiate_tls(tcp).await?;
            return self.run_protocol(tls_stream).await;
        }

        #[cfg(not(feature = "tls"))]
        if self.cfg.use_tls {
            return Err(ReplError::Protocol(
                "TLS support not enabled. Rebuild with --features tls".into(),
            ));
        }

        self.run_protocol(tcp).await
    }

    async fn run_protocol<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        stream: S,
    ) -> ReplResult<()> {
        let mut proto = ReplicationProtocol::new(
            stream,
            self.cfg.clone(),
            Arc::clone(&self.progress),
            self.stop_rx.clone(),
            self.out.clone(),
        );
        proto.run().await
    }

    /// Perform the PostgreSQL TLS handshake on an already-connected TCP stream:
    /// 1. Send SSLRequest (Int32 8, Int32 80877103)
    /// 2. Read the server's one-byte response ('S' = proceed, 'N' = reject)
    /// 3. Wrap the TCP stream in a rustls TLS session
    #[cfg(feature = "tls")]
    async fn negotiate_tls(
        &self,
        mut tcp: TcpStream,
    ) -> ReplResult<tokio_rustls::client::TlsStream<TcpStream>> {
        use std::sync::Arc;

        use tokio::io::AsyncReadExt;
        use tokio_rustls::TlsConnector as RustlsTlsConnector;

        use super::framing::write_ssl_request;

        write_ssl_request(&mut tcp).await?;

        let mut resp = [0u8; 1];
        tcp.read_exact(&mut resp).await?;

        match resp[0] {
            b'S' => {} // server accepted TLS
            b'N' => {
                return Err(ReplError::Protocol(
                    "PostgreSQL server does not support TLS".into(),
                ))
            }
            other => {
                return Err(ReplError::Protocol(format!(
                    "unexpected SSLRequest response byte: 0x{other:02x}"
                )))
            }
        }

        let config = rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(crate::utils::tls::build_root_store())
            .with_no_client_auth();

        let connector = RustlsTlsConnector::from(Arc::new(config));
        let domain = rustls::ServerName::try_from(self.cfg.host.as_str())
            .map_err(|e| ReplError::Protocol(format!("invalid TLS server name: {e}")))?;

        connector
            .connect(domain, tcp)
            .await
            .map_err(|e| ReplError::Protocol(format!("TLS handshake failed: {e}")))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests — scripted in-memory server peers drive the protocol over a duplex
// stream, exercising the full wire sequence without a real PostgreSQL server.
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

    type TestHmac = Hmac<Sha256>;

    fn duplex() -> (DuplexStream, DuplexStream) {
        tokio::io::duplex(64 * 1024)
    }

    fn test_proto(
        stream: DuplexStream,
        password: &str,
    ) -> (
        ReplicationProtocol<DuplexStream>,
        mpsc::Receiver<ReplResult<ReplicationEvent>>,
        watch::Sender<bool>,
    ) {
        let (tx, rx) = mpsc::channel(16);
        let (stop_tx, stop_rx) = watch::channel(false);
        let progress = Arc::new(SharedProgress::new(Lsn::ZERO));
        let cfg = ReplicationConfig {
            user: "alice".into(),
            password: password.into(),
            ..ReplicationConfig::default()
        };
        let proto = ReplicationProtocol::new(stream, cfg, progress, stop_rx, tx);
        (proto, rx, stop_tx)
    }

    async fn read_exact(rd: &mut DuplexStream, n: usize) -> Vec<u8> {
        let mut buf = vec![0u8; n];
        rd.read_exact(&mut buf).await.unwrap();
        buf
    }

    /// Read one tagged message of the form tag(1) + len(4) + payload.
    async fn read_msg(rd: &mut DuplexStream) -> (u8, Vec<u8>) {
        let hdr = read_exact(rd, 5).await;
        let len = i32::from_be_bytes(hdr[1..5].try_into().unwrap()) as usize;
        let payload = read_exact(rd, len - 4).await;
        (hdr[0], payload)
    }

    async fn write_backend_msg(wr: &mut DuplexStream, tag: u8, payload: &[u8]) {
        let mut buf = Vec::with_capacity(5 + payload.len());
        buf.push(tag);
        buf.extend_from_slice(&((payload.len() + 4) as i32).to_be_bytes());
        buf.extend_from_slice(payload);
        wr.write_all(&buf).await.unwrap();
        wr.flush().await.unwrap();
    }

    fn auth_request(code: i32) -> Vec<u8> {
        let mut b = Vec::with_capacity(4);
        b.extend_from_slice(&code.to_be_bytes());
        b
    }

    // ── Startup ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn startup_writes_protocol_version_and_params() {
        let (client, mut server) = duplex();
        let (mut proto, _rx, _stop) = test_proto(client, "");

        let handle = tokio::spawn(async move { proto.startup().await });

        let len = i32::from_be_bytes(read_exact(&mut server, 4).await.try_into().unwrap()) as usize;
        let body = read_exact(&mut server, len - 4).await;
        assert_eq!(i32::from_be_bytes(body[..4].try_into().unwrap()), 196608);
        let s = String::from_utf8_lossy(&body[4..]);
        assert!(s.contains("user\0alice\0"));
        assert!(s.contains("database\0postgres\0"));
        assert!(s.contains("replication\0database\0"));

        assert!(handle.await.unwrap().is_ok());
    }

    // ── Authentication ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn cleartext_auth_sends_password_and_finishes() {
        let (client, mut server) = duplex();
        let (mut proto, _rx, _stop) = test_proto(client, "s3cret");

        let handle = tokio::spawn(async move { proto.authenticate().await });

        write_backend_msg(&mut server, b'R', &auth_request(3)).await;

        let (tag, body) = read_msg(&mut server).await;
        assert_eq!(tag, b'p');
        assert_eq!(String::from_utf8_lossy(&body), "s3cret\0");

        write_backend_msg(&mut server, b'R', &auth_request(0)).await;
        write_backend_msg(&mut server, b'Z', b"I".as_ref()).await;

        assert!(handle.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn cleartext_auth_rejects_server_error() {
        let (client, mut server) = duplex();
        let (mut proto, _rx, _stop) = test_proto(client, "s3cret");

        let handle = tokio::spawn(async move { proto.authenticate().await });

        let mut err = Vec::new();
        err.push(b'M');
        err.extend_from_slice(b"password authentication failed\0");
        err.push(b'C');
        err.extend_from_slice(b"28P01\0");
        err.push(0);
        write_backend_msg(&mut server, b'E', &err).await;

        let res = handle.await.unwrap();
        assert!(matches!(res, Err(ReplError::Server(m)) if m.contains("28P01")));
    }

    #[tokio::test]
    async fn scram_auth_full_exchange() {
        let (client, mut server) = duplex();
        let password = "correct horse battery staple";
        let (mut proto, _rx, _stop) = test_proto(client, password);

        let handle = tokio::spawn(async move { proto.authenticate().await });

        // 1. SASL request offering SCRAM-SHA-256
        let mut sasl = auth_request(10);
        sasl.extend_from_slice(b"SCRAM-SHA-256\0");
        write_backend_msg(&mut server, b'R', &sasl).await;

        // 2. SASLInitialResponse: mechanism + length + client-first
        let (tag, body) = read_msg(&mut server).await;
        assert_eq!(tag, b'p');
        let nul = body.iter().position(|&b| b == 0).unwrap();
        assert_eq!(String::from_utf8_lossy(&body[..nul]), "SCRAM-SHA-256");
        let rest = &body[nul + 1..];
        let clen = i32::from_be_bytes(rest[..4].try_into().unwrap()) as usize;
        let client_first = String::from_utf8_lossy(&rest[4..4 + clen]).to_string();
        let client_first_bare = client_first
            .strip_prefix("n,,")
            .expect("client-first starts with gs2 header");
        assert!(client_first_bare.starts_with("n=alice,"));
        let nonce = client_first_bare
            .split(',')
            .find_map(|p| p.strip_prefix("r="))
            .expect("client-first has nonce");

        // 3. server-first reuses the client nonce
        let salt = b"\x01\x02\x03\x04\x05\x06\x07\x08";
        let iters = 4096;
        let server_first = format!("r={nonce},s={},i={iters}", B64.encode(salt));
        let mut sasl_cont = auth_request(11);
        sasl_cont.extend_from_slice(server_first.as_bytes());
        write_backend_msg(&mut server, b'R', &sasl_cont).await;

        // 4. client-final
        let (tag, body) = read_msg(&mut server).await;
        assert_eq!(tag, b'p');
        let client_final = String::from_utf8_lossy(&body).to_string();
        let (proof_part, _) = client_final
            .rsplit_once(",p=")
            .expect("client-final has proof");
        assert!(proof_part.starts_with("c=biws,"));
        assert!(proof_part.contains(&format!(",r={nonce}")));
        let client_final_wo_proof = proof_part;

        // 5. server-final with a genuine signature over the auth message
        let auth_message = format!("{client_first_bare},{server_first},{client_final_wo_proof}");
        let salted = hi_sha256(password.as_bytes(), salt, iters);
        let server_key = hmac_sha256(&salted, b"Server Key");
        let sig = hmac_sha256(&server_key, auth_message.as_bytes());
        let server_final = format!("v={}", B64.encode(sig));
        let mut sasl_final = auth_request(12);
        sasl_final.extend_from_slice(server_final.as_bytes());
        write_backend_msg(&mut server, b'R', &sasl_final).await;

        // 6. ReadyForQuery — auth done
        write_backend_msg(&mut server, b'Z', b"I".as_ref()).await;

        assert!(handle.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn scram_rejects_bad_server_signature() {
        let (client, mut server) = duplex();
        let password = "correct horse battery staple";
        let (mut proto, _rx, _stop) = test_proto(client, password);

        let handle = tokio::spawn(async move { proto.authenticate().await });

        let mut sasl = auth_request(10);
        sasl.extend_from_slice(b"SCRAM-SHA-256\0");
        write_backend_msg(&mut server, b'R', &sasl).await;

        let (tag, body) = read_msg(&mut server).await;
        assert_eq!(tag, b'p');
        let nul = body.iter().position(|&b| b == 0).unwrap();
        let rest = &body[nul + 1..];
        let clen = i32::from_be_bytes(rest[..4].try_into().unwrap()) as usize;
        let client_first = String::from_utf8_lossy(&rest[4..4 + clen]).to_string();
        let nonce = client_first
            .split(',')
            .find_map(|p| p.strip_prefix("r="))
            .expect("client-first has nonce");
        let salt = b"abcdefgh";
        let server_first = format!("r={nonce},s={},i=4096", B64.encode(salt));
        let mut sasl_cont = auth_request(11);
        sasl_cont.extend_from_slice(server_first.as_bytes());
        write_backend_msg(&mut server, b'R', &sasl_cont).await;

        let (_tag, _body) = read_msg(&mut server).await;

        let mut sasl_final = auth_request(12);
        sasl_final.extend_from_slice(b"v=QUJDREVGR0g=");
        write_backend_msg(&mut server, b'R', &sasl_final).await;

        let res = handle.await.unwrap();
        assert!(matches!(res, Err(ReplError::Auth(_))));
    }

    // ── Start replication ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn start_replication_sends_query_and_waits_for_copyboth() {
        let (client, mut server) = duplex();
        let (mut proto, _rx, _stop) = test_proto(client, "");

        let handle = tokio::spawn(async move { proto.start_replication().await });

        let (tag, body) = read_msg(&mut server).await;
        assert_eq!(tag, b'Q');
        let sql = String::from_utf8_lossy(&body).to_string();
        assert!(sql.starts_with("START_REPLICATION SLOT \"pgx_slot\" LOGICAL 0/0"));
        assert!(sql.contains("(proto_version '1', publication_names 'pgx_pub', messages 'true')"));

        write_backend_msg(&mut server, b'W', &[0u8; 0]).await;
        assert!(handle.await.unwrap().is_ok());
    }

    // ── Streaming loop ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn stream_loop_emits_events_and_sends_feedback() {
        let (client, mut server) = duplex();
        let (mut proto, mut rx, stop_tx) = test_proto(client, "");

        let handle = tokio::spawn(async move { proto.stream_loop().await });

        // keepalive with reply_requested
        let mut k = Vec::new();
        k.push(b'k');
        k.extend_from_slice(&0xDEADu64.to_be_bytes());
        k.extend_from_slice(&0i64.to_be_bytes());
        k.push(1u8);
        write_backend_msg(&mut server, b'd', &k).await;

        // xlogdata carrying a Begin boundary
        let mut begin = Vec::new();
        begin.push(b'w');
        begin.extend_from_slice(&1u64.to_be_bytes());
        begin.extend_from_slice(&2u64.to_be_bytes());
        begin.extend_from_slice(&0i64.to_be_bytes());
        begin.push(b'B');
        begin.extend_from_slice(&10u64.to_be_bytes());
        begin.extend_from_slice(&123i64.to_be_bytes());
        begin.extend_from_slice(&42i32.to_be_bytes());
        write_backend_msg(&mut server, b'd', &begin).await;

        // xlogdata carrying raw pgoutput bytes (Insert-like)
        let mut raw = Vec::new();
        raw.push(b'w');
        raw.extend_from_slice(&3u64.to_be_bytes());
        raw.extend_from_slice(&4u64.to_be_bytes());
        raw.extend_from_slice(&0i64.to_be_bytes());
        raw.extend_from_slice(b"\x1a\x00\x00\x00");
        write_backend_msg(&mut server, b'd', &raw).await;

        // KeepAlive event emitted; reply feedback already sent
        assert!(matches!(
            rx.recv().await,
            Some(Ok(ReplicationEvent::KeepAlive { wal_end })) if wal_end == Lsn(0xDEAD)
        ));

        // Two StandbyStatusUpdates: the initial one at loop start (reply=false)
        // and the reply to the keepalive (reply=true). Both are 'd'/'r' messages.
        let (tag1, body1) = read_msg(&mut server).await;
        let (tag2, body2) = read_msg(&mut server).await;
        assert_eq!(tag1, b'd');
        assert_eq!(tag2, b'd');
        assert_eq!(body1[0], b'r');
        assert_eq!(body2[0], b'r');
        assert_eq!(body1.len(), 34);
        assert_eq!(body2.len(), 34);
        let flags = [body1.last().unwrap(), body2.last().unwrap()];
        assert!(flags.contains(&&0u8)); // periodic status doesn't request a reply
        assert!(flags.contains(&&1u8)); // keepalive reply does
        let applied = u64::from_be_bytes(body2[1..9].try_into().unwrap());
        assert_eq!(applied, 0);

        // Begin boundary surfaced as a typed event
        assert!(matches!(
            rx.recv().await,
            Some(Ok(ReplicationEvent::Begin { xid, commit_time, .. }))
                if xid == 42 && commit_time == 123
        ));

        // Raw pgoutput forwarded verbatim
        assert!(matches!(
            rx.recv().await,
            Some(Ok(ReplicationEvent::XLogData { data, .. }))
                if data.as_ref() == b"\x1a\x00\x00\x00"
        ));

        // Graceful shutdown via stop signal
        stop_tx.send(true).unwrap();
        assert!(handle.await.unwrap().is_ok());
    }

    // ── Full session ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn full_session_cleartext_streams_and_stops() {
        let (client, mut server) = duplex();
        let (mut proto, mut rx, stop_tx) = test_proto(client, "s3cret");

        let handle = tokio::spawn(async move { proto.run().await });

        // 1. startup
        let len = i32::from_be_bytes(read_exact(&mut server, 4).await.try_into().unwrap()) as usize;
        let body = read_exact(&mut server, len - 4).await;
        assert!(String::from_utf8_lossy(&body).contains("replication\0database\0"));

        // 2. cleartext auth
        write_backend_msg(&mut server, b'R', &auth_request(3)).await;
        let (tag, pbody) = read_msg(&mut server).await;
        assert_eq!(tag, b'p');
        assert_eq!(String::from_utf8_lossy(&pbody), "s3cret\0");
        write_backend_msg(&mut server, b'R', &auth_request(0)).await;
        write_backend_msg(&mut server, b'Z', b"I".as_ref()).await;

        // 3. START_REPLICATION
        let (tag, qbody) = read_msg(&mut server).await;
        assert_eq!(tag, b'Q');
        assert!(String::from_utf8_lossy(&qbody).starts_with("START_REPLICATION"));
        write_backend_msg(&mut server, b'W', &[0u8; 0]).await;

        // 4. stream a keepalive requiring an immediate reply
        let mut k = Vec::new();
        k.push(b'k');
        k.extend_from_slice(&0u64.to_be_bytes());
        k.extend_from_slice(&0i64.to_be_bytes());
        k.push(1u8);
        write_backend_msg(&mut server, b'd', &k).await;

        assert!(matches!(
            rx.recv().await,
            Some(Ok(ReplicationEvent::KeepAlive { .. }))
        ));
        let (tag, fbody) = read_msg(&mut server).await;
        assert_eq!(tag, b'd');
        assert_eq!(fbody[0], b'r');

        // 5. stop
        stop_tx.send(true).unwrap();
        assert!(handle.await.unwrap().is_ok());
    }

    // ── Test-side SCRAM helpers (independent peer implementation) ─────────────

    fn hi_sha256(password: &[u8], salt: &[u8], iters: u32) -> Vec<u8> {
        let mut s1 = Vec::with_capacity(salt.len() + 4);
        s1.extend_from_slice(salt);
        s1.extend_from_slice(&1u32.to_be_bytes());
        let mut u = hmac_sha256(password, &s1);
        let mut out = u.clone();
        for _ in 1..iters {
            u = hmac_sha256(password, &u);
            for (o, ui) in out.iter_mut().zip(u.iter()) {
                *o ^= *ui;
            }
        }
        out
    }

    fn hmac_sha256(key: &[u8], msg: &[u8]) -> Vec<u8> {
        let mut mac = TestHmac::new_from_slice(key).expect("HMAC accepts any key length");
        mac.update(msg);
        mac.finalize().into_bytes().to_vec()
    }
}
