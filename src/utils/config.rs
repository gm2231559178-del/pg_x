use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

/// Top-level config file: ~/.pgx/config.toml
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Config {
    /// The name of the default connection profile
    pub default: Option<String>,

    /// Named connection profiles
    #[serde(default)]
    pub connections: HashMap<String, Connection>,

    /// Resolver mappings for GraphQL composition
    #[serde(default)]
    pub resolvers: HashMap<String, ResolverConfig>,
}

/// Configuration for a single resolver — maps a GraphQL field to SQL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolverConfig {
    /// The SQL query string (or path to a .sql file)
    pub sql: String,
    /// Which variable/column to bind as $1
    pub param: Option<String>,
    /// Column name used for DataLoader batching (ANY($1))
    pub batch_by: Option<String>,
    /// Optional named connection override (e.g. read replica)
    pub connection: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Connection {
    pub url: String,
    pub description: Option<String>,

    /// Defaults for the `listen` sub-command when using this connection.
    #[serde(default)]
    pub listen: Option<ListenSinkConfig>,

    /// Defaults for the `replicate` sub-command when using this connection.
    #[serde(default)]
    pub replicate: Option<ReplicateSinkConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListenSinkConfig {
    /// NOTIFY channels to subscribe to.
    #[serde(default)]
    pub channels: Vec<String>,

    /// Maximum reconnect attempts (0 = infinite).
    pub max_reconnect_attempts: Option<u32>,

    /// Base reconnect delay in milliseconds.
    pub reconnect_base_ms: Option<u64>,

    /// Maximum reconnect delay cap in milliseconds.
    pub reconnect_max_ms: Option<u64>,

    /// Downstream sink kind and its options.
    pub sink: Option<DownstreamSinkKind>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReplicateSinkConfig {
    /// Replication slot name.
    pub slot: Option<String>,

    /// Publication name(s).
    #[serde(default)]
    pub publications: Vec<String>,

    /// Table filter(s).
    #[serde(default)]
    pub tables: Vec<String>,

    /// Operation filter(s).
    #[serde(default)]
    pub ops: Vec<String>,

    /// Use a temporary slot.
    pub temporary: Option<bool>,

    /// Emit BEGIN/COMMIT events.
    pub emit_txn_boundaries: Option<bool>,

    /// Emit RELATION events.
    pub emit_schema: Option<bool>,

    /// Downstream sink kind and its options.
    pub sink: Option<DownstreamSinkKind>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DownstreamSinkKind {
    /// Print events as JSON to stdout.
    Stdout {
        /// Pretty-print JSON output.
        pretty: Option<bool>,
    },
    /// Forward events to a shell command.
    Shell {
        /// Shell command to execute.
        command: String,
        /// Extra environment variables.
        envs: Option<Vec<String>>,
        /// Forward mode (simple or contract).
        mode: Option<String>,
    },
    /// Forward events via HTTP webhook.
    Webhook {
        /// Webhook URL.
        url: String,
        /// HTTP headers (key=value).
        headers: Option<Vec<String>>,
        /// Forward mode (simple or contract).
        mode: Option<String>,
    },
    /// Forward events to RabbitMQ.
    Rabbitmq {
        /// AMQP URL.
        amqp_url: Option<String>,
        /// Exchange name.
        exchange: Option<String>,
        /// Routing key.
        routing_key: Option<String>,
        /// Forward mode (simple or contract).
        mode: Option<String>,
    },
    /// Forward events to Apache Kafka.
    Kafka {
        /// Kafka brokers.
        brokers: Option<String>,
        /// Topic name.
        topic: Option<String>,
        /// Forward mode (simple or contract).
        mode: Option<String>,
    },
    /// Index documents into Elasticsearch.
    Elasticsearch {
        /// Elasticsearch URL (e.g. http://localhost:9200).
        url: String,
        /// Elasticsearch index name.
        index: String,
        /// Optional field to use as document _id.
        id_field: Option<String>,
        /// Schema directory override.
        schema_dir: Option<String>,
    },
}

impl Config {
    pub fn path() -> Result<PathBuf> {
        let home = dirs::home_dir().context("Cannot determine home directory")?;
        Ok(home.join(".pgx").join("config.toml"))
    }

    pub fn load() -> Result<Self> {
        let path = Self::path()?;
        if !path.exists() {
            tracing::debug!(path = %path.display(), "No config file found, using defaults");
            return Ok(Config::default());
        }
        let raw = std::fs::read_to_string(&path)
            .with_context(|| format!("Cannot read config: {}", path.display()))?;
        toml::from_str(&raw).context("Invalid config TOML")
    }

    /// Look up a named connection profile.
    pub fn get(&self, name: &str) -> Option<&Connection> {
        self.connections.get(name)
    }

    /// Look up a named connection URL.
    #[inline]
    pub fn connection(&self, name: &str) -> Option<String> {
        self.get(name).map(|c| c.url.clone())
    }

    /// Return the default connection name if configured.
    #[inline]
    pub fn default_name(&self) -> Option<&str> {
        self.default.as_deref()
    }

    /// Like [`resolve`] but uses an already-loaded config instead of loading
    /// from disk.  Used when the caller already has a `Config` handle.
    pub fn resolve_from(
        &self,
        url_flag: Option<String>,
        conn_flag: Option<String>,
    ) -> Result<(String, Option<String>)> {
        if let Some(u) = url_flag {
            return Ok((u, None));
        }

        if let Some(name) = conn_flag {
            let url = self
                .connection(&name)
                .ok_or_else(|| anyhow::anyhow!("No connection named '{}' in config", name))?;
            return Ok((url, Some(name)));
        }

        if let Some(name) = self.default_name() {
            if let Some(url) = self.connection(name) {
                return Ok((url, Some(name.to_string())));
            }
        }

        Err(anyhow::anyhow!(
            "No database URL supplied.\n\
             Use -U <url>, set DATABASE_URL, or add a default in ~/.pgx/config.toml"
        ))
    }
}
