use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;

use super::{ReplicateArgs, ReplicateDownstreamCommand};
use crate::utils::config::DownstreamSinkKind;

#[async_trait::async_trait]
pub(crate) trait WalSink: Send + Sync {
    fn name(&self) -> &str;
    async fn send_wal(&self, event_json: &str, env: &HashMap<String, String>) -> Result<()>;
    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}

struct StdoutSink {
    pretty: bool,
}

#[async_trait::async_trait]
impl WalSink for StdoutSink {
    fn name(&self) -> &str {
        "stdout"
    }

    async fn send_wal(&self, event_json: &str, _env: &HashMap<String, String>) -> Result<()> {
        if self.pretty {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(event_json) {
                if let Ok(s) = serde_json::to_string_pretty(&v) {
                    println!("{s}");
                    return Ok(());
                }
                return Ok(());
            }
        }
        println!("{event_json}");
        Ok(())
    }
}

struct ShellWalSink {
    command: String,
    base_env: HashMap<String, String>,
}

#[async_trait::async_trait]
impl WalSink for ShellWalSink {
    fn name(&self) -> &str {
        "shell"
    }

    async fn send_wal(&self, event_json: &str, extra_env: &HashMap<String, String>) -> Result<()> {
        let mut env = self.base_env.clone();
        env.extend(extra_env.clone());
        env.insert("PGX_PAYLOAD".to_string(), event_json.to_string());

        let status = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(&self.command)
            .envs(&env)
            .status()
            .await
            .context("Failed to spawn shell command")?;

        if !status.success() {
            anyhow::bail!(
                "Shell command exited with status: {}",
                status.code().unwrap_or(-1)
            );
        }
        Ok(())
    }
}

#[cfg(feature = "webhook")]
struct WebhookWalSink {
    client: reqwest::Client,
    url: String,
    default_headers: HashMap<String, String>,
}

#[cfg(feature = "webhook")]
#[async_trait::async_trait]
impl WalSink for WebhookWalSink {
    fn name(&self) -> &str {
        "webhook"
    }

    async fn send_wal(&self, event_json: &str, _env: &HashMap<String, String>) -> Result<()> {
        use reqwest::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE};
        use std::str::FromStr;

        let mut hmap = HeaderMap::new();
        hmap.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        for (k, v) in &self.default_headers {
            if let (Ok(name), Ok(val)) = (HeaderName::from_str(k), HeaderValue::from_str(v)) {
                hmap.insert(name, val);
            }
        }
        self.client
            .post(&self.url)
            .headers(hmap)
            .body(event_json.to_string())
            .send()
            .await
            .context("Webhook POST failed")?
            .error_for_status()
            .context("Webhook returned error status")?;
        Ok(())
    }
}

#[cfg(feature = "rabbitmq")]
struct RabbitmqWalSink {
    channel: lapin::Channel,
    exchange: String,
    routing_key: String,
}

#[cfg(feature = "rabbitmq")]
#[async_trait::async_trait]
impl WalSink for RabbitmqWalSink {
    fn name(&self) -> &str {
        "rabbitmq"
    }

    async fn send_wal(&self, event_json: &str, env: &HashMap<String, String>) -> Result<()> {
        use lapin::{
            options::BasicPublishOptions,
            types::{AMQPValue, FieldTable, ShortString},
            BasicProperties,
        };
        use std::collections::BTreeMap;

        let mut headers: BTreeMap<ShortString, AMQPValue> = BTreeMap::new();
        for key in ["PGX_OP", "PGX_SCHEMA", "PGX_TABLE", "PGX_LSN"] {
            if let Some(val) = env.get(key) {
                let header_key = key.to_lowercase().replace('_', "-");
                headers.insert(
                    ShortString::from(header_key.as_str()),
                    AMQPValue::LongString(val.as_str().into()),
                );
            }
        }
        let props = BasicProperties::default().with_headers(FieldTable::from(headers));
        self.channel
            .basic_publish(
                &self.exchange,
                &self.routing_key,
                BasicPublishOptions::default(),
                event_json.as_bytes(),
                props,
            )
            .await
            .context("RabbitMQ publish failed")?
            .await
            .context("RabbitMQ confirm failed")?;
        Ok(())
    }
}

#[cfg(feature = "kafka")]
struct KafkaWalSink {
    producer: rdkafka::producer::FutureProducer,
    topic: String,
}

#[cfg(feature = "kafka")]
#[async_trait::async_trait]
impl WalSink for KafkaWalSink {
    fn name(&self) -> &str {
        "kafka"
    }

    async fn send_wal(&self, event_json: &str, env: &HashMap<String, String>) -> Result<()> {
        use rdkafka::producer::FutureRecord;

        let key = env
            .get("PGX_TABLE")
            .map(|s| s.as_str())
            .unwrap_or("pgx-wal");
        self.producer
            .send(
                FutureRecord::to(&self.topic).key(key).payload(event_json),
                std::time::Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| anyhow::anyhow!("Kafka send failed: {e}"))?;
        Ok(())
    }
}

struct NoopSink;

#[async_trait::async_trait]
impl WalSink for NoopSink {
    fn name(&self) -> &str {
        "postgres"
    }
    async fn send_wal(&self, _event_json: &str, _env: &HashMap<String, String>) -> Result<()> {
        Ok(())
    }
}

struct FanOutSink {
    sinks: Vec<Arc<dyn WalSink>>,
}

#[async_trait::async_trait]
impl WalSink for FanOutSink {
    fn name(&self) -> &str {
        "fan-out"
    }

    async fn send_wal(&self, event_json: &str, env: &HashMap<String, String>) -> Result<()> {
        let mut first_err: Option<anyhow::Error> = None;
        for sink in &self.sinks {
            if let Err(e) = sink.send_wal(event_json, env).await {
                tracing::error!(sink = sink.name(), error = %e, "Fan-out sink failed");
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

pub(crate) async fn build_wal_sink(cmd: &ReplicateDownstreamCommand) -> Result<Arc<dyn WalSink>> {
    match cmd {
        ReplicateDownstreamCommand::Stdout(a) => Ok(Arc::new(StdoutSink { pretty: a.pretty })),

        ReplicateDownstreamCommand::Shell(a) => {
            let command = a.command.as_deref().unwrap_or_default();
            if command.is_empty() {
                anyhow::bail!(
                    "Shell command is required — provide --command or add sink.command in config"
                );
            }
            Ok(Arc::new(ShellWalSink {
                command: command.to_string(),
                base_env: a.envs.iter().cloned().collect(),
            }))
        }

        #[cfg(feature = "webhook")]
        ReplicateDownstreamCommand::Webhook(a) => {
            let url = a.url.as_deref().unwrap_or_default();
            if url.is_empty() {
                anyhow::bail!("Webhook URL is required — provide --url, set WEBHOOK_URL env, or add sink.url in config");
            }
            Ok(Arc::new(WebhookWalSink {
                client: reqwest::Client::new(),
                url: url.to_string(),
                default_headers: a.headers.iter().cloned().collect(),
            }))
        }

        #[cfg(feature = "rabbitmq")]
        ReplicateDownstreamCommand::Rabbitmq(a) => {
            use lapin::{
                options::ExchangeDeclareOptions, types::FieldTable, Connection,
                ConnectionProperties, ExchangeKind,
            };
            let conn = Connection::connect(&a.amqp_url, ConnectionProperties::default())
                .await
                .context("Failed to connect to RabbitMQ")?;
            let channel = conn
                .create_channel()
                .await
                .context("Failed to open AMQP channel")?;
            channel
                .exchange_declare(
                    &a.exchange,
                    ExchangeKind::Topic,
                    ExchangeDeclareOptions {
                        durable: true,
                        ..Default::default()
                    },
                    FieldTable::default(),
                )
                .await
                .context("Failed to declare exchange")?;
            Ok(Arc::new(RabbitmqWalSink {
                channel,
                exchange: a.exchange.clone(),
                routing_key: a.routing_key.clone(),
            }))
        }

        #[cfg(feature = "kafka")]
        ReplicateDownstreamCommand::Kafka(a) => {
            use rdkafka::config::ClientConfig;
            let producer = ClientConfig::new()
                .set("bootstrap.servers", &a.brokers)
                .set("message.timeout.ms", "5000")
                .create()
                .context("Failed to create Kafka producer")?;
            Ok(Arc::new(KafkaWalSink {
                producer,
                topic: a.topic.clone(),
            }))
        }

        ReplicateDownstreamCommand::Postgres(_) => Ok(Arc::new(NoopSink)),

        #[cfg(feature = "parquet")]
        ReplicateDownstreamCommand::Parquet(a) => Ok(Arc::new(super::parquet::ParquetSink::new(a))),

        #[cfg(feature = "iceberg")]
        ReplicateDownstreamCommand::Iceberg(a) => {
            Ok(Arc::new(super::iceberg::IcebergSink::new(a).await?))
        }
    }
}

pub(crate) async fn build_sink_from_kind(kind: &DownstreamSinkKind) -> Result<Arc<dyn WalSink>> {
    match kind {
        DownstreamSinkKind::Stdout { pretty } => Ok(Arc::new(StdoutSink {
            pretty: pretty.unwrap_or(false),
        })),
        DownstreamSinkKind::Shell { command, envs, .. } => {
            if command.is_empty() {
                anyhow::bail!("Shell sink requires command");
            }
            let mut base_env = HashMap::new();
            if let Some(env_list) = envs {
                for e in env_list {
                    if let Some((k, v)) = e.split_once('=') {
                        base_env.insert(k.to_string(), v.to_string());
                    }
                }
            }
            Ok(Arc::new(ShellWalSink {
                command: command.clone(),
                base_env,
            }))
        }
        DownstreamSinkKind::Webhook {
            url: _url, headers, ..
        } => {
            let mut h = Vec::new();
            if let Some(hdrs) = headers {
                for entry in hdrs {
                    if let Some((k, v)) = entry.split_once('=') {
                        h.push((k.to_string(), v.to_string()));
                    }
                }
            }
            #[cfg(feature = "webhook")]
            {
                let url = _url;
                Ok(Arc::new(WebhookWalSink {
                    client: reqwest::Client::new(),
                    url: url.clone(),
                    default_headers: h.into_iter().collect(),
                }))
            }
            #[cfg(not(feature = "webhook"))]
            {
                let _ = h;
                anyhow::bail!("Webhook sink requires 'webhook' feature (reqwest)");
            }
        }
        DownstreamSinkKind::Kafka { brokers, topic, .. } => {
            #[cfg(feature = "kafka")]
            {
                use rdkafka::config::ClientConfig;
                let producer = ClientConfig::new()
                    .set(
                        "bootstrap.servers",
                        brokers.as_deref().unwrap_or("localhost:9092"),
                    )
                    .set("message.timeout.ms", "5000")
                    .create()
                    .context("Failed to create Kafka producer")?;
                Ok(Arc::new(KafkaWalSink {
                    producer,
                    topic: topic.clone().unwrap_or_else(|| "pgx-wal".to_string()),
                }))
            }
            #[cfg(not(feature = "kafka"))]
            {
                let _ = (brokers, topic);
                anyhow::bail!("Kafka sink requires 'kafka' feature (rdkafka)");
            }
        }
        DownstreamSinkKind::Rabbitmq {
            amqp_url,
            exchange,
            routing_key,
            ..
        } => {
            #[cfg(feature = "rabbitmq")]
            {
                use lapin::{
                    options::ExchangeDeclareOptions, types::FieldTable, Connection,
                    ConnectionProperties, ExchangeKind,
                };
                let url = amqp_url
                    .clone()
                    .unwrap_or_else(|| "amqp://guest:guest@localhost:5672/%2F".to_string());
                let conn = Connection::connect(&url, ConnectionProperties::default())
                    .await
                    .context("Failed to connect to RabbitMQ")?;
                let channel = conn
                    .create_channel()
                    .await
                    .context("Failed to open AMQP channel")?;
                let exch = exchange.clone().unwrap_or_else(|| "pgx".to_string());
                channel
                    .exchange_declare(
                        &exch,
                        ExchangeKind::Topic,
                        ExchangeDeclareOptions {
                            durable: true,
                            ..Default::default()
                        },
                        FieldTable::default(),
                    )
                    .await
                    .context("Failed to declare exchange")?;
                Ok(Arc::new(RabbitmqWalSink {
                    channel,
                    exchange: exch,
                    routing_key: routing_key.clone().unwrap_or_else(|| "pgx.wal".to_string()),
                }))
            }
            #[cfg(not(feature = "rabbitmq"))]
            {
                let _ = (amqp_url, exchange, routing_key);
                anyhow::bail!("RabbitMQ sink requires 'rabbitmq' feature (lapin)");
            }
        }
        DownstreamSinkKind::Elasticsearch { .. } => {
            anyhow::bail!("Elasticsearch sink is not supported for replication; use 'listen elasticsearch' instead");
        }
        DownstreamSinkKind::Postgres { .. } => Ok(Arc::new(NoopSink)),
        DownstreamSinkKind::Parquet {
            output_dir,
            max_rows,
            flush_interval,
            compression,
        } => {
            #[cfg(feature = "parquet")]
            {
                let args = super::parquet::ParquetArgs {
                    output_dir: output_dir
                        .clone()
                        .unwrap_or_else(|| "./parquet_output".to_string()),
                    max_rows: max_rows.unwrap_or(100000),
                    flush_interval: flush_interval.unwrap_or(300),
                    compression: compression.clone().unwrap_or_else(|| "snappy".to_string()),
                };
                Ok(Arc::new(super::parquet::ParquetSink::new(&args)))
            }
            #[cfg(not(feature = "parquet"))]
            {
                let _ = (output_dir, max_rows, flush_interval, compression);
                anyhow::bail!("Parquet sink requires 'parquet' feature");
            }
        }
    }
}

pub(crate) async fn build_fan_out_sink(
    args: &ReplicateArgs,
    config_additional: &[DownstreamSinkKind],
) -> Result<Arc<dyn WalSink>> {
    let mut sinks: Vec<Arc<dyn WalSink>> = Vec::new();

    let primary = build_wal_sink(&args.downstream).await?;
    sinks.push(primary);

    for s in &args.additional_sinks {
        let kind = parse_sink_string(s)?;
        let sink = build_sink_from_kind(&kind).await?;
        sinks.push(sink);
    }

    for kind in config_additional {
        let sink = build_sink_from_kind(kind).await?;
        sinks.push(sink);
    }

    if sinks.len() == 1 {
        Ok(sinks.into_iter().next().unwrap())
    } else {
        Ok(Arc::new(FanOutSink { sinks }))
    }
}

fn parse_sink_string(s: &str) -> Result<DownstreamSinkKind> {
    let (sink_type, rest) = match s.split_once(':') {
        Some((t, r)) => (t.to_lowercase(), r),
        None => (s.to_lowercase(), ""),
    };

    let mut params: HashMap<String, String> = HashMap::new();
    if !rest.is_empty() {
        for pair in rest.split(',') {
            if let Some((k, v)) = pair.split_once('=') {
                params.insert(k.to_string(), v.to_string());
            }
        }
    }

    match sink_type.as_str() {
        "stdout" => Ok(DownstreamSinkKind::Stdout {
            pretty: params.get("pretty").map(|v| v == "true"),
        }),
        "shell" => {
            let command = params
                .get("command")
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("shell sink requires command=..."))?;
            Ok(DownstreamSinkKind::Shell {
                command,
                envs: params
                    .get("envs")
                    .map(|e| e.split(',').map(String::from).collect()),
                mode: params.get("mode").cloned(),
            })
        }
        "webhook" => {
            let url = params
                .get("url")
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("webhook sink requires url=..."))?;
            Ok(DownstreamSinkKind::Webhook {
                url,
                headers: params
                    .get("headers")
                    .map(|h| h.split(',').map(String::from).collect()),
                mode: params.get("mode").cloned(),
            })
        }
        "kafka" => Ok(DownstreamSinkKind::Kafka {
            brokers: params.get("brokers").cloned(),
            topic: params.get("topic").cloned(),
            mode: params.get("mode").cloned(),
        }),
        "rabbitmq" => Ok(DownstreamSinkKind::Rabbitmq {
            amqp_url: params.get("amqp_url").cloned(),
            exchange: params.get("exchange").cloned(),
            routing_key: params.get("routing_key").cloned(),
            mode: params.get("mode").cloned(),
        }),
        "parquet" => Ok(DownstreamSinkKind::Parquet {
            output_dir: params.get("output_dir").cloned(),
            max_rows: params.get("max_rows").and_then(|v| v.parse().ok()),
            flush_interval: params.get("flush_interval").and_then(|v| v.parse().ok()),
            compression: params.get("compression").cloned(),
        }),
        other => {
            anyhow::bail!(
                "Unknown sink type '{other}'. Supported: stdout, shell, webhook, kafka, rabbitmq, parquet"
            );
        }
    }
}
