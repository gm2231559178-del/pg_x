use anyhow::{Context, Result};
use async_memcached::AsciiProtocol;
use async_trait::async_trait;
use redis::AsyncCommands;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::r#trait::ConsumeSink;

enum KvConnection {
    Redis(redis::aio::MultiplexedConnection),
    Memcached(async_memcached::Client),
}

pub struct KvConsumeSink {
    conn: Arc<Mutex<KvConnection>>,
    key_prefix: String,
    key_field: Option<String>,
    ttl: u64,
}

impl KvConsumeSink {
    pub async fn connect(
        url: &str,
        key_prefix: &str,
        key_field: Option<String>,
        ttl: u64,
    ) -> Result<Self> {
        let parsed = url::Url::parse(url).with_context(|| format!("Invalid KV URL: {}", url))?;

        let conn = match parsed.scheme() {
            s if s.starts_with("redis") => {
                let client = redis::Client::open(url)
                    .with_context(|| format!("Failed to open Redis client: {}", url))?;
                let c = client
                    .get_multiplexed_async_connection()
                    .await
                    .with_context(|| "Failed to connect to Redis")?;
                KvConnection::Redis(c)
            }
            "memcached" => {
                let host = parsed.host_str().unwrap_or("127.0.0.1");
                let port = parsed.port().unwrap_or(11211);
                let addr = format!("{}:{}", host, port);
                let c = async_memcached::Client::new(&addr)
                    .await
                    .with_context(|| format!("Failed to connect to Memcached at {}", addr))?;
                KvConnection::Memcached(c)
            }
            other => {
                anyhow::bail!(
                    "Unsupported KV URL scheme '{}' (expected redis:// or memcached://)",
                    other
                )
            }
        };

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            key_prefix: key_prefix.to_string(),
            key_field,
            ttl,
        })
    }

    fn extract_key(&self, doc: &Value) -> String {
        extract_key_impl(&self.key_prefix, self.key_field.as_deref(), doc)
    }
}

fn extract_key_impl(key_prefix: &str, key_field: Option<&str>, doc: &Value) -> String {
    let suffix = key_field.and_then(|kf| match doc {
        Value::Object(m) => m.get(kf).map(|v| match v {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        }),
        _ => None,
    });
    let suffix = suffix.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    format!("{}{}", key_prefix, suffix)
}

#[async_trait]
impl ConsumeSink for KvConsumeSink {
    fn name(&self) -> &str {
        "kv"
    }

    async fn send(&self, doc: &Value) -> Result<()> {
        let key = self.extract_key(doc);
        let value = serde_json::to_string(doc)?;
        let mut conn = self.conn.lock().await;

        match &mut *conn {
            KvConnection::Redis(ref mut c) => {
                let _: () = c
                    .set(&key, &value)
                    .await
                    .with_context(|| format!("Redis SET failed for key '{}'", key))?;
                if self.ttl > 0 {
                    let _: () = c
                        .expire(&key, self.ttl as i64)
                        .await
                        .with_context(|| format!("Redis EXPIRE failed for key '{}'", key))?;
                }
            }
            KvConnection::Memcached(ref mut c) => {
                let ttl = if self.ttl > 0 {
                    Some(self.ttl as i64)
                } else {
                    None
                };
                c.set(&key, value.as_bytes(), ttl, None)
                    .await
                    .with_context(|| format!("Memcached SET failed for key '{}'", key))?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extract_key_with_field_string() {
        let doc = json!({"id": "abc123", "name": "test"});
        assert_eq!(extract_key_impl("pfx:", Some("id"), &doc), "pfx:abc123");
    }

    #[test]
    fn extract_key_with_field_number() {
        let doc = json!({"user_id": 42, "name": "test"});
        assert_eq!(extract_key_impl("", Some("user_id"), &doc), "42");
    }

    #[test]
    fn extract_key_without_field_falls_back_to_uuid() {
        let doc = json!({"name": "test"});
        let key = extract_key_impl("", None, &doc);
        assert_eq!(key.len(), 36);
        uuid::Uuid::parse_str(&key).unwrap();
    }

    #[test]
    fn extract_key_with_missing_field() {
        let doc = json!({"id": "abc123"});
        let key = extract_key_impl("pfx:", Some("missing_field"), &doc);
        assert!(key.starts_with("pfx:"));
        assert_eq!(key.len(), 40);
    }
}
