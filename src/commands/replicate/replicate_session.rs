//! The replicate session: owns the decode → filter → transform → deliver run,
//! the LSN-advance policy, and the exit outcome behind one `new` + `run` seam.
//! The source and the applier sit behind narrow traits so the whole loop is
//! testable without a live Postgres, mirroring the consume session.

use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use crate::replication::client::{ReplicationClient, ReplicationEvent};
use crate::replication::decoder::{decode_pgoutput, RelationCache};
use crate::replication::event::{qualified_name, WalEvent};
use crate::replication::lsn::Lsn;
use crate::utils::session_loop::{SessionExit, Shutdown};

use super::applier::PostgresApplier;
use super::filter::RowFilter;
use super::sinks::WalSink;
use super::transforms::ColumnTransforms;
use super::OpFilter;

// ─────────────────────────────────────────────────────────────────────────────
// Seams
// ─────────────────────────────────────────────────────────────────────────────

/// The replication source: everything the session needs from the wire client.
#[async_trait]
pub(crate) trait EventSource: Send {
    async fn recv(&mut self) -> Result<Option<ReplicationEvent>>;
    fn update_applied_lsn(&self, lsn: Lsn);
    fn last_applied_lsn(&self) -> Lsn;
    fn stop(&self);
}

#[async_trait]
impl EventSource for ReplicationClient {
    async fn recv(&mut self) -> Result<Option<ReplicationEvent>> {
        ReplicationClient::recv(self).await
    }
    fn update_applied_lsn(&self, lsn: Lsn) {
        ReplicationClient::update_applied_lsn(self, lsn);
    }
    fn last_applied_lsn(&self) -> Lsn {
        ReplicationClient::last_applied_lsn(self)
    }
    fn stop(&self) {
        ReplicationClient::stop(self);
    }
}

/// The WAL applier (Postgres target). Gated by the same filter decision as the
/// fan-out sinks.
#[async_trait]
pub(crate) trait Applier: Send {
    fn handle_begin(&mut self);
    async fn handle_event(&mut self, event: &WalEvent) -> Result<()>;
    async fn handle_commit(&mut self) -> Result<()>;
}

#[async_trait]
impl Applier for PostgresApplier {
    fn handle_begin(&mut self) {
        PostgresApplier::handle_begin(self);
    }
    async fn handle_event(&mut self, event: &WalEvent) -> Result<()> {
        PostgresApplier::handle_event(self, event).await
    }
    async fn handle_commit(&mut self) -> Result<()> {
        PostgresApplier::handle_commit(self).await
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Session config (the slice of ReplicateArgs the loop needs)
// ─────────────────────────────────────────────────────────────────────────────

/// What the session needs from the CLI args to make its filtering and
/// boundary-emission decisions.
#[derive(Debug, Clone, Default)]
pub(crate) struct ReplicateSessionConfig {
    pub emit_txn_boundaries: bool,
    pub emit_schema: bool,
    pub tables: Vec<String>,
    pub ops: Vec<OpFilter>,
}

// ─────────────────────────────────────────────────────────────────────────────
// The session
// ─────────────────────────────────────────────────────────────────────────────

const RECV_TIMEOUT: Duration = Duration::from_secs(60);

pub(crate) struct ReplicateSession {
    config: ReplicateSessionConfig,
    row_filter: RowFilter,
    transforms: ColumnTransforms,
    sink: Arc<dyn WalSink>,
    applier: Arc<tokio::sync::Mutex<Option<Box<dyn Applier>>>>,
    resume_lsn: Arc<tokio::sync::Mutex<Lsn>>,
    rel_cache: RelationCache,
}

impl ReplicateSession {
    pub(crate) fn new(
        config: ReplicateSessionConfig,
        row_filter: RowFilter,
        transforms: ColumnTransforms,
        sink: Arc<dyn WalSink>,
        applier: Arc<tokio::sync::Mutex<Option<Box<dyn Applier>>>>,
        resume_lsn: Arc<tokio::sync::Mutex<Lsn>>,
    ) -> Self {
        Self {
            config,
            row_filter,
            transforms,
            sink,
            applier,
            resume_lsn,
            rel_cache: RelationCache::new(),
        }
    }

    /// Run one session against a freshly connected source. Returns the existing
    /// `SessionExit` outcomes so the session-loop factory is unchanged. Owns
    /// the resume-LSN: on exit, progress is persisted before the outcome is
    /// returned.
    pub(crate) async fn run(
        &mut self,
        source: &mut (impl EventSource + ?Sized),
        shutdown: &mut Shutdown,
    ) -> SessionExit {
        let start_lsn = *self.resume_lsn.lock().await;

        let mut clean_exit = false;
        let mut applier_guard = self.applier.lock().await;
        let mut applier = applier_guard.as_mut();

        loop {
            let ev = tokio::select! {
                biased;

                _ = shutdown.wait() => {
                    info!("Signal received, stopping replication");
                    source.stop();
                    clean_exit = true;
                    break;
                }

                _ = tokio::time::sleep(RECV_TIMEOUT) => {
                    warn!("Replication stream idle for 60s, reconnecting");
                    source.stop();
                    break;
                }

                result = source.recv() => match result {
                    Ok(None) => {
                        clean_exit = true;
                        break;
                    }
                    Err(e) => {
                        error!(error = %e, "Replication error");
                        break;
                    }
                    Ok(Some(ev)) => ev,
                },
            };

            match ev {
                ReplicationEvent::KeepAlive { wal_end } => {
                    source.update_applied_lsn(wal_end);
                }

                ReplicationEvent::Begin {
                    final_lsn,
                    xid,
                    commit_time,
                } => {
                    if let Some(applier) = applier.as_deref_mut() {
                        applier.handle_begin();
                    }

                    if self.config.emit_txn_boundaries {
                        let event = WalEvent::Begin {
                            lsn: final_lsn.to_string(),
                            commit_time,
                            xid,
                        };
                        log_event(&event, &final_lsn.to_string());
                        let env = event_env(&event, &final_lsn.to_string());
                        if let Err(e) = self.sink.send_wal(&event.to_json(), &env).await {
                            error!(error = %e, "Downstream send failed (Begin); LSN not advanced");
                            continue;
                        }
                    }
                    source.update_applied_lsn(final_lsn);
                }

                ReplicationEvent::Commit {
                    lsn,
                    end_lsn,
                    commit_time,
                } => {
                    if let Some(applier) = applier.as_deref_mut() {
                        if let Err(e) = applier.handle_commit().await {
                            error!(error = %e, "PG applier commit failed");
                            break;
                        }
                    }

                    if self.config.emit_txn_boundaries {
                        let event = WalEvent::Commit {
                            lsn: lsn.to_string(),
                            end_lsn: end_lsn.to_string(),
                            commit_time,
                        };
                        log_event(&event, &end_lsn.to_string());
                        let env = event_env(&event, &end_lsn.to_string());
                        if let Err(e) = self.sink.send_wal(&event.to_json(), &env).await {
                            error!(error = %e, "Downstream send failed (Commit); LSN not advanced");
                            continue;
                        }
                    }
                    source.update_applied_lsn(end_lsn);
                }

                ReplicationEvent::XLogData { data, wal_end, .. } => {
                    let lsn_str = wal_end.to_string();
                    let is_pg_active = applier.is_some();

                    match decode_pgoutput(&data, &mut self.rel_cache) {
                        Ok(Some(mut event)) => {
                            // One gate: computed on the pre-transform event,
                            // then applied to the applier AND the sinks.
                            // Transforms apply afterward.
                            let forward = should_forward(&event, &self.config, &self.row_filter);

                            self.transforms.apply(&mut event);

                            log_event(&event, &lsn_str);

                            if forward {
                                if let Some(applier) = applier.as_deref_mut() {
                                    if let Err(e) = applier.handle_event(&event).await {
                                        error!(error = %e, "PG applier event failed");
                                        break;
                                    }
                                }

                                let env = event_env(&event, &lsn_str);
                                if let Err(e) = self.sink.send_wal(&event.to_json(), &env).await {
                                    error!(sink = self.sink.name(), error = %e, "Downstream send failed; LSN not advanced");
                                    continue;
                                }
                            }

                            // LSN-advance policy: with an applier active the rows
                            // are buffered until commit, so progress advances at
                            // commit boundaries; without one, it advances here.
                            if !is_pg_active {
                                source.update_applied_lsn(wal_end);
                            }
                        }
                        Ok(None) => {
                            source.update_applied_lsn(wal_end);
                        }
                        Err(e) => {
                            error!(error = %e, "WAL decode error; LSN not advanced");
                        }
                    }
                }
            }
        }

        drop(applier_guard);

        let last_applied = source.last_applied_lsn();
        *self.resume_lsn.lock().await = last_applied;

        if clean_exit {
            SessionExit::Shutdown
        } else if last_applied != start_lsn {
            SessionExit::ReconnectAfterHealthy
        } else {
            SessionExit::Reconnect
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Filter helpers (table name, op)
// ─────────────────────────────────────────────────────────────────────────────

fn table_matches(schema: &str, table: &str, filter: &[String]) -> bool {
    if filter.is_empty() {
        return true;
    }
    let qualified = qualified_name(schema, table);
    filter.iter().any(|f| f == table || f == &qualified)
}

fn op_matches(op: &str, filter: &[OpFilter]) -> bool {
    if filter.is_empty() {
        return true;
    }
    filter.iter().any(|f| match f {
        OpFilter::Insert => op == "insert",
        OpFilter::Update => op == "update",
        OpFilter::Delete => op == "delete",
        OpFilter::Truncate => op == "truncate",
    })
}

fn should_forward(
    event: &WalEvent,
    config: &ReplicateSessionConfig,
    row_filter: &RowFilter,
) -> bool {
    match event {
        WalEvent::Insert { schema, table, .. }
        | WalEvent::Update { schema, table, .. }
        | WalEvent::Delete { schema, table, .. } => {
            let op = event.op_label().to_lowercase();
            table_matches(schema, table, &config.tables)
                && op_matches(&op, &config.ops)
                && row_filter.should_forward(event)
        }
        WalEvent::Truncate { tables, .. } => {
            op_matches("truncate", &config.ops)
                && (config.tables.is_empty()
                    || tables.iter().any(|t| config.tables.iter().any(|f| f == t)))
        }
        WalEvent::Begin { .. } | WalEvent::Commit { .. } => config.emit_txn_boundaries,
        WalEvent::Relation { .. } => config.emit_schema,
        WalEvent::Keepalive { .. } => false,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Event → env-var map (for shell sinks)
// ─────────────────────────────────────────────────────────────────────────────

/// Env-var keys used to pass WAL event fields to child processes (single
/// source of truth; the shell/AMQP sinks and parquet/iceberg sinks read these).
pub(crate) const PGX_OP: &str = "PGX_OP";
pub(crate) const PGX_SCHEMA: &str = "PGX_SCHEMA";
pub(crate) const PGX_TABLE: &str = "PGX_TABLE";
pub(crate) const PGX_LSN: &str = "PGX_LSN";
pub(crate) const PGX_NEW: &str = "PGX_NEW";
pub(crate) const PGX_OLD: &str = "PGX_OLD";
pub(crate) const PGX_XID: &str = "PGX_XID";
pub(crate) const PGX_TABLES: &str = "PGX_TABLES";
pub(crate) const PGX_PAYLOAD: &str = "PGX_PAYLOAD";

fn json_or_dash(v: &impl serde::Serialize) -> String {
    match serde_json::to_string(v) {
        Ok(s) => s,
        Err(e) => {
            error!(error = %e, "Failed to serialize row for event_env");
            String::new()
        }
    }
}

fn event_env(event: &WalEvent, lsn_str: &str) -> HashMap<String, String> {
    let mut env = HashMap::new();
    env.insert(PGX_OP.to_string(), event.op_label().to_lowercase());
    env.insert(PGX_LSN.to_string(), lsn_str.to_string());

    match event {
        WalEvent::Insert {
            schema, table, new, ..
        } => {
            env.insert(PGX_SCHEMA.to_string(), schema.clone());
            env.insert(PGX_TABLE.to_string(), table.clone());
            env.insert(PGX_NEW.to_string(), json_or_dash(new));
        }
        WalEvent::Update {
            schema,
            table,
            new,
            old,
            ..
        } => {
            env.insert(PGX_SCHEMA.to_string(), schema.clone());
            env.insert(PGX_TABLE.to_string(), table.clone());
            env.insert(PGX_NEW.to_string(), json_or_dash(new));
            if let Some(o) = old {
                env.insert(PGX_OLD.to_string(), json_or_dash(o));
            }
        }
        WalEvent::Delete {
            schema, table, old, ..
        } => {
            env.insert(PGX_SCHEMA.to_string(), schema.clone());
            env.insert(PGX_TABLE.to_string(), table.clone());
            env.insert(PGX_OLD.to_string(), json_or_dash(old));
        }
        WalEvent::Truncate { tables, .. } => {
            env.insert(PGX_TABLES.to_string(), tables.join(","));
        }
        WalEvent::Begin { xid, .. } => {
            env.insert(PGX_XID.to_string(), xid.to_string());
        }
        _ => {}
    }
    env
}

// ─────────────────────────────────────────────────────────────────────────────
// Console log helper
// ─────────────────────────────────────────────────────────────────────────────

fn log_event(event: &WalEvent, lsn_str: &str) {
    match event {
        WalEvent::Insert { schema, table, .. } => debug!(
            op = "insert", schema = %schema, table = %table, lsn = %lsn_str, "WAL event"
        ),
        WalEvent::Update { schema, table, .. } => debug!(
            op = "update", schema = %schema, table = %table, lsn = %lsn_str, "WAL event"
        ),
        WalEvent::Delete { schema, table, .. } => debug!(
            op = "delete", schema = %schema, table = %table, lsn = %lsn_str, "WAL event"
        ),
        WalEvent::Truncate { tables, .. } => debug!(
            op = "truncate", tables = %tables.join(", "), lsn = %lsn_str, "WAL event"
        ),
        WalEvent::Begin { xid, .. } => debug!(op = "begin", xid, "WAL event"),
        WalEvent::Commit { .. } => debug!(op = "commit", lsn = %lsn_str, "WAL event"),
        WalEvent::Relation {
            schema,
            table,
            columns,
            ..
        } => debug!(
            op = "relation", schema = %schema, table = %table,
            col_count = columns.len(), "WAL schema event"
        ),
        WalEvent::Keepalive { .. } => {}
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests: the session is driven through `run()` with a scripted source and
// in-memory fakes, asserting the gate, the LSN policy, and session outcomes.
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::replicate::filter::parse_where_arg;
    use std::cell::Cell;
    use std::collections::VecDeque;
    use std::sync::Mutex as StdMutex;

    // ── pgoutput test encoders ──────────────────────────────────────────────

    fn u16(v: u16) -> Vec<u8> {
        v.to_be_bytes().to_vec()
    }
    fn u32(v: u32) -> Vec<u8> {
        v.to_be_bytes().to_vec()
    }
    fn cstring(s: &str) -> Vec<u8> {
        let mut b = s.as_bytes().to_vec();
        b.push(0);
        b
    }

    fn rel_msg(rel_id: u32, schema: &str, table: &str, cols: &[(&str, u32)]) -> Vec<u8> {
        let mut b = vec![b'R'];
        b.extend(u32(rel_id));
        b.extend(cstring(schema));
        b.extend(cstring(table));
        b.push(0); // replica identity
        b.extend(u16(cols.len() as u16));
        for (name, type_id) in cols {
            b.push(0); // flags
            b.extend(cstring(name));
            b.extend(u32(*type_id));
            b.extend((-1i32).to_be_bytes()); // type modifier
        }
        b
    }

    fn text_col(v: &str) -> Vec<u8> {
        let mut b = vec![b't'];
        b.extend(u32(v.len() as u32));
        b.extend(v.as_bytes());
        b
    }

    fn insert_msg(rel_id: u32, vals: &[&str]) -> Vec<u8> {
        let mut b = vec![b'I'];
        b.extend(u32(rel_id));
        b.push(b'N');
        b.extend(u16(vals.len() as u16));
        for v in vals {
            b.extend(text_col(v));
        }
        b
    }

    fn xlog(lsn: u64, data: Vec<u8>) -> ReplicationEvent {
        ReplicationEvent::XLogData {
            wal_start: Lsn::from_u64(lsn),
            wal_end: Lsn::from_u64(lsn),
            data: bytes::Bytes::from(data),
        }
    }

    fn begin_event(final_lsn: u64) -> ReplicationEvent {
        ReplicationEvent::Begin {
            final_lsn: Lsn::from_u64(final_lsn),
            xid: 1,
            commit_time: 0,
        }
    }

    fn commit_event(end_lsn: u64) -> ReplicationEvent {
        ReplicationEvent::Commit {
            lsn: Lsn::from_u64(end_lsn),
            end_lsn: Lsn::from_u64(end_lsn),
            commit_time: 0,
        }
    }

    // ── Fakes ───────────────────────────────────────────────────────────────

    #[derive(Default)]
    struct ApplierLog {
        began: usize,
        commits: usize,
        events: Vec<WalEvent>,
    }

    struct FakeApplier {
        log: Arc<StdMutex<ApplierLog>>,
        fail_on: Option<usize>,
    }

    #[async_trait]
    impl Applier for FakeApplier {
        fn handle_begin(&mut self) {
            self.log.lock().unwrap().began += 1;
        }
        async fn handle_event(&mut self, event: &WalEvent) -> Result<()> {
            let mut log = self.log.lock().unwrap();
            if Some(log.events.len()) == self.fail_on {
                drop(log);
                anyhow::bail!("applier failed");
            }
            log.events.push(event.clone());
            Ok(())
        }
        async fn handle_commit(&mut self) -> Result<()> {
            self.log.lock().unwrap().commits += 1;
            Ok(())
        }
    }

    struct FakeSink {
        sent: Arc<StdMutex<Vec<String>>>,
    }

    #[async_trait]
    impl WalSink for FakeSink {
        fn name(&self) -> &str {
            "fake"
        }
        async fn send_wal(&self, event_json: &str, _env: &HashMap<String, String>) -> Result<()> {
            self.sent.lock().unwrap().push(event_json.to_string());
            Ok(())
        }
        async fn flush(&self) -> Result<()> {
            Ok(())
        }
    }

    struct ScriptedSource {
        events: VecDeque<ReplicationEvent>,
        applied: Cell<Lsn>,
    }

    impl ScriptedSource {
        fn new(events: Vec<ReplicationEvent>) -> Self {
            Self {
                events: events.into(),
                applied: Cell::new(Lsn::ZERO),
            }
        }
    }

    #[async_trait]
    impl EventSource for ScriptedSource {
        async fn recv(&mut self) -> Result<Option<ReplicationEvent>> {
            Ok(self.events.pop_front())
        }
        fn update_applied_lsn(&self, lsn: Lsn) {
            self.applied.set(lsn);
        }
        fn last_applied_lsn(&self) -> Lsn {
            self.applied.get()
        }
        fn stop(&self) {}
    }

    // ── Harness ─────────────────────────────────────────────────────────────

    enum ApplierMode {
        None,
        Ok,
        FailOn(usize),
    }

    struct Harness {
        session: ReplicateSession,
        applier_log: Arc<StdMutex<ApplierLog>>,
        sink_log: Arc<StdMutex<Vec<String>>>,
        resume: Arc<tokio::sync::Mutex<Lsn>>,
    }

    impl Harness {
        fn new(config: ReplicateSessionConfig, row_filter: RowFilter, mode: ApplierMode) -> Self {
            let applier_log = Arc::new(StdMutex::new(ApplierLog::default()));
            let sink_log = Arc::new(StdMutex::new(Vec::new()));
            let resume = Arc::new(tokio::sync::Mutex::new(Lsn::ZERO));

            let applier = match mode {
                ApplierMode::None => None,
                ApplierMode::Ok | ApplierMode::FailOn(_) => Some(Box::new(FakeApplier {
                    log: applier_log.clone(),
                    fail_on: match mode {
                        ApplierMode::FailOn(n) => Some(n),
                        _ => None,
                    },
                })
                    as Box<dyn Applier>),
            };

            let sink: Arc<dyn WalSink> = Arc::new(FakeSink {
                sent: sink_log.clone(),
            });

            let session = ReplicateSession::new(
                config,
                row_filter,
                ColumnTransforms::new(),
                sink,
                Arc::new(tokio::sync::Mutex::new(applier)),
                resume.clone(),
            );

            Self {
                session,
                applier_log,
                sink_log,
                resume,
            }
        }

        async fn run(&mut self, events: Vec<ReplicationEvent>) -> SessionExit {
            let mut source = ScriptedSource::new(events);
            let (_tx, rx) = tokio::sync::watch::channel(false);
            let mut shutdown = Shutdown::from_receiver(rx);
            self.session.run(&mut source, &mut shutdown).await
        }

        async fn applied(&self) -> Lsn {
            *self.resume.lock().await
        }

        fn applier_events(&self) -> Vec<WalEvent> {
            self.applier_log.lock().unwrap().events.clone()
        }

        fn sink_sent(&self) -> Vec<String> {
            self.sink_log.lock().unwrap().clone()
        }
    }

    fn orders_relation() -> Vec<u8> {
        rel_msg(1, "public", "orders", &[("id", 23), ("status", 25)])
    }

    // ── Tests ───────────────────────────────────────────────────────────────

    /// Regression for the live bug: `--where` filters must gate the applier as
    /// well as the sinks, so filtered-out rows never reach the target.
    #[tokio::test]
    async fn where_filter_gates_the_applier_and_the_sink() {
        let mut rf = RowFilter::new();
        let (key, expr) = parse_where_arg("public.orders:status = 'active'").unwrap();
        rf.add(key, expr);

        let mut h = Harness::new(ReplicateSessionConfig::default(), rf, ApplierMode::Ok);
        let exit = h
            .run(vec![
                xlog(10, orders_relation()),
                xlog(20, insert_msg(1, &["42", "active"])),
                xlog(30, insert_msg(1, &["43", "inactive"])),
            ])
            .await;

        assert!(matches!(exit, SessionExit::Shutdown));
        let events = h.applier_events();
        assert_eq!(
            events.len(),
            1,
            "filtered-out row must not reach the applier"
        );
        match &events[0] {
            WalEvent::Insert {
                schema, table, new, ..
            } => {
                assert_eq!(schema, "public");
                assert_eq!(table, "orders");
                assert_eq!(new.get("status").and_then(|c| c.as_str()), Some("active"));
            }
            other => panic!("expected insert, got {other:?}"),
        }
        let sent = h.sink_sent();
        assert_eq!(sent.len(), 1, "filtered-out row must not reach the sink");
        assert!(sent[0].contains("\"active\""));
    }

    /// The table filter gates both the applier and the sink identically.
    #[tokio::test]
    async fn table_filter_gates_the_applier_and_the_sink() {
        let config = ReplicateSessionConfig {
            tables: vec!["public.orders".to_string()],
            ..ReplicateSessionConfig::default()
        };
        let mut h = Harness::new(config, RowFilter::new(), ApplierMode::Ok);
        let exit = h
            .run(vec![
                xlog(10, rel_msg(1, "public", "orders", &[("id", 23)])),
                xlog(20, rel_msg(2, "public", "inventory", &[("id", 23)])),
                xlog(30, insert_msg(1, &["42"])),
                xlog(40, insert_msg(2, &["7"])),
            ])
            .await;

        assert!(matches!(exit, SessionExit::Shutdown));
        assert_eq!(
            h.applier_events().len(),
            1,
            "only orders reaches the applier"
        );
        assert_eq!(h.sink_sent().len(), 1, "only orders reaches the sink");
    }

    /// Without an applier, LSN advances per XLogData and every event is sunk.
    #[tokio::test]
    async fn without_applier_lsn_advances_per_xlogdata() {
        let mut h = Harness::new(
            ReplicateSessionConfig::default(),
            RowFilter::new(),
            ApplierMode::None,
        );
        let exit = h
            .run(vec![
                xlog(20, insert_msg(1, &["42"])),
                xlog(30, insert_msg(1, &["43"])),
            ])
            .await;

        assert!(matches!(exit, SessionExit::Shutdown));
        assert_eq!(h.applied().await, Lsn::from_u64(30));
        assert_eq!(h.sink_sent().len(), 2);
    }

    /// With an applier active, per-row LSNs are not advanced; progress moves at
    /// commit boundaries, and the buffered rows reach the applier.
    #[tokio::test]
    async fn with_applier_lsn_advances_at_commit() {
        let mut h = Harness::new(
            ReplicateSessionConfig::default(),
            RowFilter::new(),
            ApplierMode::Ok,
        );
        let exit = h
            .run(vec![
                begin_event(10),
                xlog(20, insert_msg(1, &["42"])),
                xlog(30, insert_msg(1, &["43"])),
                commit_event(40),
            ])
            .await;

        assert!(matches!(exit, SessionExit::Shutdown));
        assert_eq!(h.applied().await, Lsn::from_u64(40));
        let log = h.applier_log.lock().unwrap();
        assert_eq!(log.began, 1);
        assert_eq!(log.commits, 1);
        assert_eq!(log.events.len(), 2);
        assert_eq!(h.sink_sent().len(), 2);
    }

    /// With boundary emission on, BEGIN and COMMIT are forwarded to the sink.
    #[tokio::test]
    async fn emit_txn_boundaries_forwards_begin_and_commit() {
        let config = ReplicateSessionConfig {
            emit_txn_boundaries: true,
            ..ReplicateSessionConfig::default()
        };
        let mut h = Harness::new(config, RowFilter::new(), ApplierMode::None);
        let exit = h
            .run(vec![
                begin_event(10),
                xlog(20, insert_msg(1, &["42"])),
                commit_event(30),
            ])
            .await;

        assert!(matches!(exit, SessionExit::Shutdown));
        let sent = h.sink_sent();
        assert_eq!(sent.len(), 3);
        assert!(sent[0].contains("\"op\":\"begin\""));
        assert!(sent[2].contains("\"op\":\"commit\""));
        assert_eq!(h.applied().await, Lsn::from_u64(30));
    }

    /// An applier failure before any progress is a plain reconnect: the loop
    /// must not `continue` forever with the LSN stuck.
    #[tokio::test]
    async fn applier_failure_without_progress_is_reconnect() {
        let mut h = Harness::new(
            ReplicateSessionConfig::default(),
            RowFilter::new(),
            ApplierMode::FailOn(0),
        );
        let exit = h.run(vec![xlog(20, insert_msg(1, &["42"]))]).await;

        assert!(matches!(exit, SessionExit::Reconnect));
        assert_eq!(h.applied().await, Lsn::ZERO);
    }

    /// An applier failure after a committed transaction counts as healthy
    /// progress: reconnect-after-healthy, and the committed LSN is persisted.
    #[tokio::test]
    async fn applier_failure_after_progress_is_reconnect_after_healthy() {
        let mut h = Harness::new(
            ReplicateSessionConfig::default(),
            RowFilter::new(),
            ApplierMode::FailOn(1),
        );
        let exit = h
            .run(vec![
                begin_event(10),
                xlog(20, insert_msg(1, &["42"])),
                commit_event(40),
                xlog(50, insert_msg(1, &["43"])),
            ])
            .await;

        assert!(matches!(exit, SessionExit::ReconnectAfterHealthy));
        assert_eq!(h.applied().await, Lsn::from_u64(40));
    }

    /// A keepalive acknowledges the server's latest LSN.
    #[tokio::test]
    async fn keepalive_advances_applied_lsn() {
        let mut h = Harness::new(
            ReplicateSessionConfig::default(),
            RowFilter::new(),
            ApplierMode::None,
        );
        let exit = h
            .run(vec![ReplicationEvent::KeepAlive {
                wal_end: Lsn::from_u64(50),
            }])
            .await;

        assert!(matches!(exit, SessionExit::Shutdown));
        assert_eq!(h.applied().await, Lsn::from_u64(50));
    }
}
