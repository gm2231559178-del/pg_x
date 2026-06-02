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
        write_query, write_startup_message,
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
// Background worker
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
            let mut tls_stream = self.negotiate_tls(tcp).await?;
            return self.run_on_stream(&mut tls_stream).await;
        }

        #[cfg(not(feature = "tls"))]
        if self.cfg.use_tls {
            return Err(ReplError::Protocol(
                "TLS support not enabled. Rebuild with --features tls".into(),
            ));
        }

        let mut tcp = tcp;
        self.run_on_stream(&mut tcp).await
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
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth();

        let connector = RustlsTlsConnector::from(Arc::new(config));
        let domain = rustls::ServerName::try_from(self.cfg.host.as_str())
            .map_err(|e| ReplError::Protocol(format!("invalid TLS server name: {e}")))?;

        connector
            .connect(domain, tcp)
            .await
            .map_err(|e| ReplError::Protocol(format!("TLS handshake failed: {e}")))
    }

    async fn run_on_stream<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        stream: &mut S,
    ) -> ReplResult<()> {
        let mut stream = BufReader::with_capacity(128 * 1024, stream);
        self.startup(&mut stream).await?;
        self.authenticate(&mut stream).await?;
        if self.cfg.temporary {
            self.create_temp_slot(&mut stream).await?;
        }
        self.start_replication(&mut stream).await?;
        self.stream_loop(&mut stream).await
    }

    // ── Startup ───────────────────────────────────────────────────────────────

    async fn startup<S: AsyncWrite + Unpin>(&self, s: &mut S) -> ReplResult<()> {
        write_startup_message(
            s,
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

    async fn authenticate<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        stream: &mut S,
    ) -> ReplResult<()> {
        loop {
            let msg = read_backend_message_into(stream, &mut BytesMut::new()).await?;
            match msg.tag {
                b'R' => {
                    let (code, rest) = parse_auth_request(&msg.payload)?;
                    self.handle_auth(stream, code, rest).await?;
                }
                b'E' => return Err(ReplError::Server(parse_error_response(&msg.payload))),
                b'S' | b'K' => {}      // ParameterStatus, BackendKeyData — ignore
                b'Z' => return Ok(()), // ReadyForQuery — done
                _ => {}
            }
        }
    }

    async fn handle_auth<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        stream: &mut S,
        code: i32,
        data: &[u8],
    ) -> ReplResult<()> {
        match code {
            0 => Ok(()), // AuthenticationOk
            3 => {
                // Cleartext password
                let mut payload = self.cfg.password.as_bytes().to_vec();
                payload.push(0);
                write_password_message(stream, &payload).await
            }
            10 => {
                // SASL — try SCRAM-SHA-256
                let mechanisms = parse_sasl_mechanisms(data);
                if !mechanisms.iter().any(|m| m == "SCRAM-SHA-256") {
                    return Err(ReplError::Auth(format!(
                        "server offers {mechanisms:?} but SCRAM-SHA-256 is required"
                    )));
                }
                self.auth_scram(stream).await
            }
            _ => Err(ReplError::Auth(format!("unsupported auth method: {code}"))),
        }
    }

    async fn auth_scram<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        stream: &mut S,
    ) -> ReplResult<()> {
        let scram = ScramClient::new(&self.cfg.user);

        // SASLInitialResponse: mechanism name + client-first
        let mut init = Vec::new();
        init.extend_from_slice(b"SCRAM-SHA-256\0");
        init.extend_from_slice(&(scram.client_first.len() as i32).to_be_bytes());
        init.extend_from_slice(scram.client_first.as_bytes());
        write_password_message(stream, &init).await?;

        // AuthenticationSASLContinue (code 11)
        let server_first = self.read_auth_data(stream, 11).await?;
        let server_first_str = String::from_utf8_lossy(&server_first);

        // Compute and send client-final
        let (client_final, auth_message, salted_password) =
            scram.client_final(&self.cfg.password, &server_first_str)?;
        write_password_message(stream, client_final.as_bytes()).await?;

        // AuthenticationSASLFinal (code 12) — verify server signature
        let server_final = self.read_auth_data(stream, 12).await?;
        let server_final_str = String::from_utf8_lossy(&server_final);
        ScramClient::verify_server_final(&server_final_str, &salted_password, &auth_message)?;

        Ok(())
    }

    async fn read_auth_data<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        stream: &mut S,
        expected_code: i32,
    ) -> ReplResult<Vec<u8>> {
        loop {
            let msg = read_backend_message_into(stream, &mut BytesMut::new()).await?;
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

    async fn start_replication<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        stream: &mut S,
    ) -> ReplResult<()> {
        let slot_escaped = self.cfg.slot.replace('"', "\"\"");
        let pub_escaped = self.cfg.publication.replace('\'', "''");
        let sql = format!(
            "START_REPLICATION SLOT \"{slot_escaped}\" LOGICAL {} \
             (proto_version '1', publication_names '{pub_escaped}', messages 'true')",
            self.cfg.start_lsn
        );
        write_query(stream, &sql).await?;

        // Wait for CopyBothResponse ('W')
        loop {
            let msg = read_backend_message_into(stream, &mut BytesMut::new()).await?;
            match msg.tag {
                b'W' => return Ok(()), // CopyBothResponse — streaming begins
                b'E' => return Err(ReplError::Server(parse_error_response(&msg.payload))),
                b'N' | b'S' | b'K' => {} // Notice, ParameterStatus, BackendKeyData
                _ => {}
            }
        }
    }

    // ── Temporary slot creation ───────────────────────────────────────────────

    async fn create_temp_slot<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        stream: &mut S,
    ) -> ReplResult<()> {
        let escaped = self.cfg.slot.replace('"', "\"\"");
        let sql = format!(
            "CREATE_REPLICATION_SLOT \"{escaped}\" TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT"
        );
        write_query(stream, &sql).await?;
        loop {
            let msg = read_backend_message_into(stream, &mut BytesMut::new()).await?;
            match msg.tag {
                b'Z' => return Ok(()),
                b'E' => return Err(ReplError::Server(parse_error_response(&msg.payload))),
                _ => {}
            }
        }
    }

    // ── Main stream loop ──────────────────────────────────────────────────────

    async fn stream_loop<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        stream: &mut BufReader<S>,
    ) -> ReplResult<()> {
        let status_interval = std::time::Duration::from_secs(self.cfg.status_interval_secs);
        let idle_wakeup = std::time::Duration::from_secs(self.cfg.idle_wakeup_secs);

        let mut last_status = Instant::now() - status_interval;
        let mut last_applied = self.progress.load_applied();
        let mut read_buf = BytesMut::with_capacity(4096);

        const DRAIN_BATCH: usize = 256;

        loop {
            // Sync applied LSN from client
            let current_applied = self.progress.load_applied();
            if current_applied != last_applied {
                last_applied = current_applied;
            }

            // Periodic status feedback
            if last_status.elapsed() >= status_interval {
                self.send_feedback(stream, last_applied, false).await?;
                last_status = Instant::now();
            }

            // ── Drain phase: tight loop while BufReader has buffered data ─────
            let mut drained = 0;
            while stream.buffer().len() >= 5 && drained < DRAIN_BATCH {
                let msg = read_backend_message_into(stream, &mut read_buf).await?;
                drained += 1;
                match msg.tag {
                    b'd' => {
                        if self
                            .handle_copy_data(
                                stream,
                                msg.payload,
                                &mut last_applied,
                                &mut last_status,
                            )
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
                    let _ = write_copy_done(stream).await;
                    return Ok(());
                }
                continue;
            }

            // ── Wait phase: select on socket vs stop signal vs idle timeout ───
            let msg = tokio::select! {
                biased;

                _ = self.stop_rx.changed() => {
                    if *self.stop_rx.borrow() {
                        let _ = write_copy_done(stream).await;
                        return Ok(());
                    }
                    continue;
                }

                res = tokio::time::timeout(
                    idle_wakeup,
                    read_backend_message_into(stream, &mut read_buf),
                ) => {
                    match res {
                        Ok(inner) => inner?,
                        Err(_) => {
                            // Idle timeout — send status update and continue
                            let applied = self.progress.load_applied();
                            last_applied = applied;
                            self.send_feedback(stream, applied, false).await?;
                            last_status = Instant::now();
                            continue;
                        }
                    }
                }
            };

            match msg.tag {
                b'd' => {
                    if self
                        .handle_copy_data(stream, msg.payload, &mut last_applied, &mut last_status)
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
    async fn handle_copy_data<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        stream: &mut BufReader<S>,
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
                    self.send_feedback(stream, applied, true).await?;
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

    async fn emit(&self, ev: ReplResult<ReplicationEvent>) {
        if self.out.send(ev).await.is_err() {
            // Channel closed — client disconnected
        }
    }

    async fn send_feedback<S: AsyncWrite + Unpin>(
        &self,
        stream: &mut S,
        applied: Lsn,
        reply_requested: bool,
    ) -> ReplResult<()> {
        let ts = current_pg_timestamp();
        let payload = encode_standby_status_update(applied, ts, reply_requested);
        write_copy_data(stream, &payload).await
    }
}


