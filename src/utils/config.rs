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
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Connection {
    pub url: String,
    pub description: Option<String>,
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

    /// Look up a named connection URL.
    pub fn connection(&self, name: &str) -> Option<String> {
        self.connections.get(name).map(|c| c.url.clone())
    }

    /// Return the default connection URL if configured.
    pub fn default_url(&self) -> Option<String> {
        let name = self.default.as_deref()?;
        self.connection(name)
    }
}
