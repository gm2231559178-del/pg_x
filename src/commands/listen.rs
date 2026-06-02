use anyhow::Result;
use clap::{Args, Subcommand, ValueEnum};

use std::collections::HashMap;
use std::sync::Arc;
use tokio_postgres::NoTls;
use tracing::{debug, error, info, warn};

use crate::downstream::{contract::NotifyEvent, sink::Downstream};
use crate::utils::config::Connection;
use crate::utils::signal::{parse_key_val, shutdown_signal};

#[derive(Args)]
pub struct ListenArgs {
    /// NOTIFY channel(s) to subscribe to (repeatable: -C orders -C inventory)
    #[arg(short = 'C', long = "channel", required = true)]
    pub channels: Vec<String>,

    /// Maximum consecutive reconnect attempts before giving up (0 = infinite).
    /// In containerized environments, set to a small number (e.g. 5) and rely
    /// on your restart policy, OR set to 0 to retry forever inside the process.
    #[arg(long, env = "PGX_MAX_RECONNECT_ATTEMPTS", default_value_t = 0)]
    pub max_reconnect_attempts: u32,

    /// Base reconnect delay in milliseconds (doubles each attempt).
    #[arg(long, env = "PGX_RECONNECT_BASE_MS", default_value_t = 1_000)]
    pub reconnect_base_ms: u64,

    /// Maximum reconnect delay cap in milliseconds.
    #[arg(long, env = "PGX_RECONNECT_MAX_MS", default_value_t = 60_000)]
    pub reconnect_max_ms: u64,

    #[command(subcommand)]
    pub downstream: DownstreamCommand,
}

#[derive(Subcommand)]
pub enum DownstreamCommand {
    /// Forward events to RabbitMQ (AMQP)
    #[cfg(feature = "rabbitmq")]
    Rabbitmq(RabbitmqArgs),

    /// Forward events to Apache Kafka
    #[cfg(feature = "kafka")]
    Kafka(KafkaArgs),

    /// Forward events via HTTP webhook (POST)
    #[cfg(feature = "webhook")]
    Webhook(WebhookArgs),

    /// Forward events to a shell command
    Shell(ShellArgs),
}

#[cfg(feature = "rabbitmq")]
#[derive(Args)]
pub struct RabbitmqArgs {
    #[arg(
        long,
        env = "AMQP_URL",
        default_value = "amqp://guest:guest@localhost:5672/%2F"
    )]
    pub amqp_url: String,
    #[arg(long, default_value = "pgx")]
    pub exchange: String,
    #[arg(long, default_value = "pgx.notify")]
    pub routing_key: String,
    #[arg(long, value_enum, default_value_t = ForwardMode::Simple)]
    pub mode: ForwardMode,
}

#[cfg(feature = "kafka")]
#[derive(Args)]
pub struct KafkaArgs {
    #[arg(long, env = "KAFKA_BROKERS", default_value = "localhost:9092")]
    pub brokers: String,
    #[arg(long, default_value = "pgx-notify")]
    pub topic: String,
    #[arg(long, value_enum, default_value_t = ForwardMode::Simple)]
    pub mode: ForwardMode,
}

#[cfg(feature = "webhook")]
#[derive(Args)]
pub struct WebhookArgs {
    #[arg(long, env = "WEBHOOK_URL")]
    pub url: String,
    #[arg(long = "header", value_parser = parse_key_val)]
    pub headers: Vec<(String, String)>,
    #[arg(long, value_enum, default_value_t = ForwardMode::Simple)]
    pub mode: ForwardMode,
}

#[derive(Args)]
pub struct ShellArgs {
    #[arg(long)]
    pub command: String,
    #[arg(long = "env", value_parser = parse_key_val)]
    pub envs: Vec<(String, String)>,
    #[arg(long, value_enum, default_value_t = ForwardMode::Simple)]
    pub mode: ForwardMode,
}

#[derive(Clone, ValueEnum)]
pub enum ForwardMode {
    Simple,
    Contract,
}

/// Exponential backoff with ±20% jitter. Shift is clamped to avoid u64 overflow.
fn backoff_delay(attempt: u32, base_ms: u64, max_ms: u64) -> std::time::Duration {
    let shift = attempt.saturating_sub(1).min(62);
    let base = (base_ms.saturating_mul(1u64 << shift)).min(max_ms);
    let jitter = base / 5;
    let delay_ms = base - jitter + (rand::random::<u64>() % (jitter * 2 + 1));
    std::time::Duration::from_millis(delay_ms)
}

/// Escape a channel name so it can be safely used in LISTEN "..."
fn escape_channel(ch: &str) -> String {
    ch.replace('"', "\"\"")
}

pub async fn run(url: String, mut args: ListenArgs, conn: Option<&Connection>) -> Result<()> {
    // Merge connection-level defaults into CLI args (CLI wins).
    if let Some(cfg) = conn.and_then(|c| c.listen.as_ref()) {
        if args.channels.is_empty() && !cfg.channels.is_empty() {
            args.channels = cfg.channels.clone();
        }
        if args.max_reconnect_attempts == 0 && cfg.max_reconnect_attempts.unwrap_or(0) > 0 {
            args.max_reconnect_attempts = cfg.max_reconnect_attempts.unwrap();
        }
        if args.reconnect_base_ms == 1000 && cfg.reconnect_base_ms.unwrap_or(0) > 0 {
            args.reconnect_base_ms = cfg.reconnect_base_ms.unwrap();
        }
        if args.reconnect_max_ms == 60000 && cfg.reconnect_max_ms.unwrap_or(0) > 0 {
            args.reconnect_max_ms = cfg.reconnect_max_ms.unwrap();
        }
    }

    let sink: Arc<dyn Downstream> = build_downstream(&args.downstream).await?;

    tokio::pin!(let shutdown = shutdown_signal(););

    // Counts *consecutive* failures only — resets to 0 after each successful
    // session so a brief drop after hours of healthy operation doesn't exhaust
    // a stale retry budget.
    let mut consecutive_failures: u32 = 0;

    loop {
        // ── Backoff (skipped on first attempt) ────────────────────────────────
        if consecutive_failures > 0 {
            let infinite = args.max_reconnect_attempts == 0;

            if !infinite && consecutive_failures >= args.max_reconnect_attempts {
                error!(
                    consecutive_failures,
                    max = args.max_reconnect_attempts,
                    "Max reconnect attempts reached"
                );
                return Err(anyhow::anyhow!(
                    "Max reconnect attempts ({}) reached",
                    args.max_reconnect_attempts
                ));
            }

            let delay = backoff_delay(
                consecutive_failures,
                args.reconnect_base_ms,
                args.reconnect_max_ms,
            );

            warn!(
                consecutive_failures,
                delay_secs = delay.as_secs_f32(),
                max_attempts = if infinite {
                    "∞".to_string()
                } else {
                    args.max_reconnect_attempts.to_string()
                },
                "Connection lost, reconnecting…"
            );

            tokio::select! {
                biased;
                _ = &mut shutdown => {
                    info!("Signal received during backoff, shutting down cleanly");
                    return Ok(());
                }
                _ = tokio::time::sleep(delay) => {}
            }
        }

        // ── Connect ───────────────────────────────────────────────────────────
        info!("Connecting to PostgreSQL…");

        let (client, connection) = match tokio_postgres::connect(&url, NoTls).await {
            Ok(pair) => pair,
            Err(e) => {
                error!(error = %e, "Connection failed");
                consecutive_failures += 1;
                continue;
            }
        };

        // ── Drainer task ──────────────────────────────────────────────────────
        // THE BUG FIX: do NOT clone tx before spawning. Move the only sender
        // into the Drainer. When the Drainer exits (connection dies), the sole
        // tx is dropped, rx.recv() returns None, and the event loop breaks.
        //
        // The original code did `let tx = tx.clone()` inside spawn while keeping
        // the original tx alive in this scope. That left a live sender dangling
        // so rx.recv() would hang forever after the Drainer exited.
        // Using a bounded channel to prevent unbounded memory growth.
        let (tx, mut rx) = tokio::sync::mpsc::channel::<tokio_postgres::Notification>(1024);

        let drain_handle = tokio::spawn(async move {
            use std::future::Future;
            use std::pin::Pin;
            use std::task::{Context as Cx, Poll};

            struct Drainer {
                conn: tokio_postgres::Connection<
                    tokio_postgres::Socket,
                    tokio_postgres::tls::NoTlsStream,
                >,
                // Sole owner of tx — when Drainer is dropped, tx is dropped,
                // closing the channel and unblocking rx.recv() with None.
                tx: tokio::sync::mpsc::Sender<tokio_postgres::Notification>,
            }

            impl Future for Drainer {
                type Output = ();
                fn poll(mut self: Pin<&mut Self>, cx: &mut Cx<'_>) -> Poll<()> {
                    loop {
                        match self.conn.poll_message(cx) {
                            Poll::Pending => return Poll::Pending,
                            Poll::Ready(None) => return Poll::Ready(()),
                            Poll::Ready(Some(Ok(tokio_postgres::AsyncMessage::Notification(
                                n,
                            )))) => {
                                if self.tx.try_send(n).is_err() {
                                    // Channel full — downstream is slow, drop oldest to
                                    // apply backpressure rather than blocking.
                                }
                            }
                            Poll::Ready(Some(Ok(_))) => {}
                            Poll::Ready(Some(Err(e))) => {
                                error!(error = %e, "PostgreSQL connection error");
                                return Poll::Ready(());
                                // Drainer drops here -> tx dropped -> rx.recv() = None
                            }
                        }
                    }
                }
            }

            Drainer {
                conn: connection,
                tx,
            }
            .await
        });
        // tx is fully moved into spawn — no copy remains in this scope.

        // ── LISTEN ────────────────────────────────────────────────────────────
        let mut listen_ok = true;
        for ch in &args.channels {
            let escaped = escape_channel(ch);
            match client.execute(&format!("LISTEN \"{escaped}\""), &[]).await {
                Ok(_) => info!(channel = %ch, "Listening on channel"),
                Err(e) => {
                    error!(channel = %ch, error = %e, "LISTEN failed");
                    listen_ok = false;
                    break;
                }
            }
        }
        if !listen_ok {
            consecutive_failures += 1;
            continue;
        }

        // ── Successful session — reset failure counter ────────────────────────
        consecutive_failures = 0;
        info!(
            sink = sink.name(),
            "Forwarding events — Ctrl-C / SIGTERM to stop"
        );

        // ── Event loop ────────────────────────────────────────────────────────
        let session_dropped = loop {
            tokio::select! {
                biased;

                _ = &mut shutdown => {
                    info!("Signal received, shutting down cleanly");
                    return Ok(());
                }

                maybe_n = rx.recv() => {
                    match maybe_n {
                        None => {
                            // tx was dropped (Drainer exited) — connection is dead.
                            warn!("Drainer channel closed — connection lost");
                            break true;
                        }
                        Some(n) => {
                            let event = NotifyEvent {
                                channel: n.channel().to_string(),
                                payload: n.payload().to_string(),
                                pid: n.process_id(),
                            };

                            debug!(
                                channel = %event.channel,
                                pid = event.pid,
                                payload = %event.payload,
                                "NOTIFY received"
                            );

                            if let Err(e) = sink.send(&event).await {
                                error!(sink = sink.name(), error = %e, "Downstream send failed");
                            }
                        }
                    }
                }
            }
        };

        if session_dropped {
            consecutive_failures += 1;
        }

        // Drainer task completed — check for panics
        if drain_handle.is_finished() {
            if let Err(e) = drain_handle.await {
                error!("Drainer task panicked: {e}");
            }
        }
    }
}

async fn build_downstream(cmd: &DownstreamCommand) -> Result<Arc<dyn Downstream>> {
    match cmd {
        #[cfg(feature = "rabbitmq")]
        DownstreamCommand::Rabbitmq(a) => {
            use crate::downstream::rabbitmq::rabbitmq::{
                ContractRabbitMqDownstream, SimpleRabbitMqDownstream,
            };
            match a.mode {
                ForwardMode::Simple => Ok(Arc::new(
                    SimpleRabbitMqDownstream::connect(&a.amqp_url, &a.exchange, &a.routing_key)
                        .await?,
                )),
                ForwardMode::Contract => Ok(Arc::new(
                    ContractRabbitMqDownstream::connect(&a.amqp_url, &a.exchange, &a.routing_key)
                        .await?,
                )),
            }
        }

        #[cfg(feature = "kafka")]
        DownstreamCommand::Kafka(a) => {
            use crate::downstream::kafka::kafka::{ContractKafkaDownstream, SimpleKafkaDownstream};
            match a.mode {
                ForwardMode::Simple => Ok(Arc::new(SimpleKafkaDownstream::connect(
                    &a.brokers, &a.topic,
                )?)),
                ForwardMode::Contract => Ok(Arc::new(ContractKafkaDownstream::connect(
                    &a.brokers, &a.topic,
                )?)),
            }
        }

        #[cfg(feature = "webhook")]
        DownstreamCommand::Webhook(a) => {
            use crate::downstream::webhook::webhook::{
                ContractWebhookDownstream, SimpleWebhookDownstream,
            };
            let headers: HashMap<String, String> = a.headers.iter().cloned().collect();
            match a.mode {
                ForwardMode::Simple => Ok(Arc::new(SimpleWebhookDownstream::new(&a.url))),
                ForwardMode::Contract => {
                    Ok(Arc::new(ContractWebhookDownstream::new(&a.url, headers)))
                }
            }
        }

        DownstreamCommand::Shell(a) => {
            use crate::downstream::shell::shell::ShellDownstream;
            let base_env: HashMap<String, String> = a.envs.iter().cloned().collect();
            let contract_mode = matches!(a.mode, ForwardMode::Contract);
            Ok(Arc::new(ShellDownstream::new(
                &a.command,
                base_env,
                contract_mode,
            )))
        }
    }
}
