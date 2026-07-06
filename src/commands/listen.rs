use anyhow::Result;
use clap::{Args, Subcommand, ValueEnum};

use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::downstream::{contract::NotifyEvent, sink::Downstream};
use crate::utils::config::{
    merge_opt, merge_vec, ChannelFullBehavior, Connection, DownstreamSinkKind, ResolverConfig,
};
use crate::utils::signal::{parse_key_val, shutdown_signal};
use crate::utils::tls;

#[derive(Args)]
pub struct ListenArgs {
    /// NOTIFY channel(s) to subscribe to (repeatable: -C orders -C inventory)
    #[arg(short = 'C', long = "channel", required = true)]
    pub channels: Vec<String>,

    /// Maximum consecutive reconnect attempts before giving up (0 = infinite, default).
    /// In containerized environments, set to a small number (e.g. 5) and rely
    /// on your restart policy, OR set to 0 to retry forever inside the process.
    #[arg(long, env = "PGX_MAX_RECONNECT_ATTEMPTS")]
    pub max_reconnect_attempts: Option<u32>,

    /// Base reconnect delay in milliseconds (doubles each attempt, default 1000).
    #[arg(long, env = "PGX_RECONNECT_BASE_MS")]
    pub reconnect_base_ms: Option<u64>,

    /// Maximum reconnect delay cap in milliseconds (default 60000).
    #[arg(long, env = "PGX_RECONNECT_MAX_MS")]
    pub reconnect_max_ms: Option<u64>,

    /// Behavior when the internal notification channel is full.
    /// Options: block (waits for space), drop_oldest (drops oldest), grow (unbounded).
    #[arg(long, default_value = "drop_oldest")]
    pub channel_full_behavior: ChannelFullBehavior,

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

    /// Execute GraphQL query and index result into Elasticsearch
    #[cfg(feature = "elasticsearch")]
    Elasticsearch(ElasticsearchArgs),
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
    /// Webhook URL. Required unless provided via config or WEBHOOK_URL env.
    #[arg(long, env = "WEBHOOK_URL")]
    pub url: Option<String>,
    #[arg(long = "header", value_parser = parse_key_val)]
    pub headers: Vec<(String, String)>,
    #[arg(long, value_enum, default_value_t = ForwardMode::Simple)]
    pub mode: ForwardMode,
}

#[derive(Args)]
pub struct ShellArgs {
    /// Shell command to execute. Required unless provided via config.
    #[arg(long)]
    pub command: Option<String>,
    #[arg(long = "env", value_parser = parse_key_val)]
    pub envs: Vec<(String, String)>,
    #[arg(long, value_enum, default_value_t = ForwardMode::Simple)]
    pub mode: ForwardMode,
}

#[cfg(feature = "elasticsearch")]
#[derive(Args)]
pub struct ElasticsearchArgs {
    /// Elasticsearch URL (default: http://localhost:9200).
    #[arg(long, env = "ES_URL")]
    pub es_url: Option<String>,
    /// Elasticsearch index name (default: pgx).
    #[arg(long)]
    pub index: Option<String>,
    /// Field to use as document _id.
    #[arg(long)]
    pub id_field: Option<String>,
    /// Schema directory (defaults to ~/.pgx/schema).
    #[arg(long)]
    pub schema_dir: Option<String>,
}

#[derive(Clone, ValueEnum)]
pub enum ForwardMode {
    Simple,
    Contract,
}

impl std::str::FromStr for ForwardMode {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "simple" => Ok(Self::Simple),
            "contract" => Ok(Self::Contract),
            other => Err(format!(
                "unknown forward mode '{other}'; expected simple|contract"
            )),
        }
    }
}

/// Escape a channel name so it can be safely used in LISTEN "..."
fn escape_channel(ch: &str) -> String {
    ch.replace('"', "\"\"")
}

pub async fn run(
    url: String,
    mut args: ListenArgs,
    conn: Option<&Connection>,
    use_tls: bool,
    resolvers: &HashMap<String, ResolverConfig>,
) -> Result<()> {
    // Merge connection-level defaults into CLI args (CLI wins).
    if let Some(cfg) = conn.and_then(|c| c.listen.as_ref()) {
        merge_vec(&mut args.channels, &cfg.channels);
        merge_opt(
            &mut args.max_reconnect_attempts,
            &cfg.max_reconnect_attempts,
        );
        merge_opt(&mut args.reconnect_base_ms, &cfg.reconnect_base_ms);
        merge_opt(&mut args.reconnect_max_ms, &cfg.reconnect_max_ms);

        // Merge channel_full_behavior from config (CLI default is "drop_oldest",
        // but config file may override it).
        if let Some(behavior) = &cfg.channel_full_behavior {
            // CLI arg has a default, so it's always Some. Only override if the
            // config value differs from the implicit default.
            if args.channel_full_behavior == ChannelFullBehavior::default() {
                args.channel_full_behavior = behavior.clone();
            }
        }

        // Merge downstream sink defaults from config into CLI subcommand args.
        if let Some(sink_cfg) = &cfg.sink {
            match (&mut args.downstream, sink_cfg) {
                (DownstreamCommand::Shell(a), DownstreamSinkKind::Shell { command, mode, .. }) => {
                    merge_opt(&mut a.command, &Some(command.clone()));
                    if let Some(m) = mode {
                        if let Ok(fm) = m.parse::<ForwardMode>() {
                            a.mode = fm;
                        }
                    }
                }
                #[cfg(feature = "webhook")]
                (DownstreamCommand::Webhook(a), DownstreamSinkKind::Webhook { url, mode, .. }) => {
                    merge_opt(&mut a.url, &Some(url.clone()));
                    if let Some(m) = mode {
                        if let Ok(fm) = m.parse::<ForwardMode>() {
                            a.mode = fm;
                        }
                    }
                }
                #[cfg(feature = "rabbitmq")]
                (
                    DownstreamCommand::Rabbitmq(a),
                    DownstreamSinkKind::Rabbitmq {
                        amqp_url,
                        exchange,
                        routing_key,
                        mode,
                    },
                ) => {
                    if let Some(u) = amqp_url {
                        a.amqp_url = u.clone();
                    }
                    if let Some(e) = exchange {
                        a.exchange = e.clone();
                    }
                    if let Some(r) = routing_key {
                        a.routing_key = r.clone();
                    }
                    if let Some(m) = mode {
                        if let Ok(fm) = m.parse::<ForwardMode>() {
                            a.mode = fm;
                        }
                    }
                }
                #[cfg(feature = "kafka")]
                (
                    DownstreamCommand::Kafka(a),
                    DownstreamSinkKind::Kafka {
                        brokers,
                        topic,
                        mode,
                    },
                ) => {
                    if let Some(b) = brokers {
                        a.brokers = b.clone();
                    }
                    if let Some(t) = topic {
                        a.topic = t.clone();
                    }
                    if let Some(m) = mode {
                        if let Ok(fm) = m.parse::<ForwardMode>() {
                            a.mode = fm;
                        }
                    }
                }
                #[cfg(feature = "elasticsearch")]
                (
                    DownstreamCommand::Elasticsearch(a),
                    DownstreamSinkKind::Elasticsearch {
                        url,
                        index,
                        id_field,
                        schema_dir,
                    },
                ) => {
                    merge_opt(&mut a.es_url, &Some(url.clone()));
                    merge_opt(&mut a.index, &Some(index.clone()));
                    merge_opt(&mut a.id_field, id_field);
                    merge_opt(&mut a.schema_dir, schema_dir);
                }
                // Mismatch — CLI subcommand doesn't match config sink type; CLI wins.
                _ => {}
            }
        }
    }

    // Resolve optional reconnect parameters to final defaults.
    let max_reconnect_attempts = args.max_reconnect_attempts.unwrap_or(0);
    let reconnect_base_ms = args.reconnect_base_ms.unwrap_or(1000);
    let reconnect_max_ms = args.reconnect_max_ms.unwrap_or(60000);

    let sink: Arc<dyn Downstream> =
        build_downstream(&args.downstream, &url, use_tls, resolvers).await?;

    tokio::pin!(let shutdown = shutdown_signal(););

    // Counts *consecutive* failures only — resets to 0 after each successful
    // session so a brief drop after hours of healthy operation doesn't exhaust
    // a stale retry budget.
    let mut consecutive_failures: u32 = 0;

    loop {
        // ── Backoff (skipped on first attempt) ────────────────────────────────
        if consecutive_failures > 0 {
            let infinite = max_reconnect_attempts == 0;

            if !infinite && consecutive_failures >= max_reconnect_attempts {
                error!(
                    consecutive_failures,
                    max = max_reconnect_attempts,
                    "Max reconnect attempts reached"
                );
                return Err(anyhow::anyhow!(
                    "Max reconnect attempts ({}) reached",
                    max_reconnect_attempts
                ));
            }

            let delay = crate::utils::backoff::delay(
                consecutive_failures,
                reconnect_base_ms,
                reconnect_max_ms,
            );

            warn!(
                consecutive_failures,
                delay_secs = delay.as_secs_f32(),
                max_attempts = if infinite {
                    "∞".to_string()
                } else {
                    max_reconnect_attempts.to_string()
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

        let connector = tls::build_tls(use_tls)?;
        let (client, connection) = match tokio_postgres::connect(&url, connector).await {
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
        let behavior = args.channel_full_behavior.clone();
        let channel_capacity = match behavior {
            ChannelFullBehavior::Grow => 100_000,
            _ => 1024,
        };
        let (tx, mut rx) =
            tokio::sync::mpsc::channel::<tokio_postgres::Notification>(channel_capacity);

        let drain_handle = tokio::spawn(async move {
            use std::future::Future;
            use std::pin::Pin;
            use std::task::{Context as Cx, Poll};

            struct Drainer<S> {
                conn: tokio_postgres::Connection<tokio_postgres::Socket, S>,
                tx: tokio::sync::mpsc::Sender<tokio_postgres::Notification>,
                behavior: ChannelFullBehavior,
            }

            impl<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin> Future for Drainer<S> {
                type Output = ();
                fn poll(mut self: Pin<&mut Self>, cx: &mut Cx<'_>) -> Poll<()> {
                    loop {
                        match self.conn.poll_message(cx) {
                            Poll::Pending => return Poll::Pending,
                            Poll::Ready(None) => return Poll::Ready(()),
                            Poll::Ready(Some(Ok(tokio_postgres::AsyncMessage::Notification(
                                n,
                            )))) => match self.behavior {
                                ChannelFullBehavior::DropOldest => {
                                    if self.tx.try_send(n).is_err() {
                                        // Channel full — downstream is slow, drop
                                        // oldest to apply backpressure.
                                    }
                                }
                                ChannelFullBehavior::Block => {
                                    if self.tx.try_send(n).is_err() {
                                        // Channel full — wait for space by returning
                                        // Pending. The waker will be notified when
                                        // the downstream consumes.
                                        return Poll::Pending;
                                    }
                                }
                                ChannelFullBehavior::Grow => {
                                    let _ = self.tx.try_send(n);
                                }
                            },
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
                behavior,
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

#[allow(unused_variables)]
async fn build_downstream(
    cmd: &DownstreamCommand,
    url: &str,
    use_tls: bool,
    resolvers: &HashMap<String, ResolverConfig>,
) -> Result<Arc<dyn Downstream>> {
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
            let url = a.url.as_deref().unwrap_or_default();
            if url.is_empty() {
                anyhow::bail!("Webhook URL is required — provide --url, set WEBHOOK_URL env, or add sink.url in config");
            }
            let headers: HashMap<String, String> = a.headers.iter().cloned().collect();
            match a.mode {
                ForwardMode::Simple => Ok(Arc::new(SimpleWebhookDownstream::new(url))),
                ForwardMode::Contract => Ok(Arc::new(ContractWebhookDownstream::new(url, headers))),
            }
        }

        DownstreamCommand::Shell(a) => {
            use crate::downstream::shell::shell::ShellDownstream;
            let command = a.command.as_deref().unwrap_or_default();
            if command.is_empty() {
                anyhow::bail!(
                    "Shell command is required — provide --command or add sink.command in config"
                );
            }
            let base_env: HashMap<String, String> = a.envs.iter().cloned().collect();
            let contract_mode = matches!(a.mode, ForwardMode::Contract);
            Ok(Arc::new(ShellDownstream::new(
                command,
                base_env,
                contract_mode,
            )))
        }

        #[cfg(feature = "elasticsearch")]
        DownstreamCommand::Elasticsearch(a) => {
            use crate::downstream::elasticsearch::ElasticsearchDownstream;
            use crate::graphql::pool::QueryConn;
            use std::path::PathBuf;

            let pool = QueryConn::connect(url, use_tls).await?;
            let es_url = a.es_url.as_deref().unwrap_or("http://localhost:9200");
            let index = a.index.as_deref().unwrap_or("pgx");
            let schema_dir = a.schema_dir.as_ref().map(PathBuf::from);
            let ds = ElasticsearchDownstream::new(
                es_url,
                index,
                a.id_field.clone(),
                8,
                pool,
                resolvers.clone(),
                schema_dir,
            )?;
            Ok(Arc::new(ds))
        }
    }
}
