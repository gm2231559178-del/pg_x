use anyhow::{Context, Result};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;

/// Webhook transport handle shared by the sink seams.
///
/// Hides retry policy and idempotency keying behind one `post` method: an
/// `Idempotency-Key` header is added whenever a `msg_id` is supplied, and the
/// retry count is chosen at construction time.
pub struct Webhook {
    client: Client,
    /// Number of retries after the first attempt (0 = single attempt).
    retries: u32,
}

impl Webhook {
    /// Default policy: two retries after the first attempt (three total),
    /// matching the notify seam's historical behaviour.
    pub fn new() -> Self {
        Self::with_retries(2)
    }

    /// `retries` is the number of additional attempts after the first.
    pub fn with_retries(retries: u32) -> Self {
        Self {
            client: Client::new(),
            retries,
        }
    }

    /// POST `body` as JSON to `url`, merging `headers`. Retries with
    /// exponential backoff on transport errors and non-2xx responses.
    pub async fn post(
        &self,
        url: &str,
        headers: &HashMap<String, String>,
        body: &Value,
        msg_id: Option<&str>,
    ) -> Result<()> {
        let mut attempt = 0u32;
        loop {
            let mut hmap = build_header_map(headers);
            hmap.insert(
                HeaderName::from_static("content-type"),
                HeaderValue::from_static("application/json"),
            );
            if let Some(id) = msg_id {
                hmap.insert(
                    HeaderName::from_static("idempotency-key"),
                    HeaderValue::from_str(id)
                        .with_context(|| format!("Invalid Idempotency-Key value: {id}"))?,
                );
            }

            let result = self
                .client
                .post(url)
                .headers(hmap)
                .json(body)
                .send()
                .await
                .and_then(|r| r.error_for_status());

            match result {
                Ok(_) => return Ok(()),
                Err(_) if attempt < self.retries => {
                    attempt += 1;
                    let delay = std::time::Duration::from_millis(100 * 2u64.pow(attempt));
                    tokio::time::sleep(delay).await;
                }
                Err(e) => return Err(e).with_context(|| format!("Webhook POST failed to {url}")),
            }
        }
    }
}

impl Default for Webhook {
    fn default() -> Self {
        Self::new()
    }
}

/// Build a [`HeaderMap`] from `"Name: value"` pairs, skipping invalid entries.
pub fn build_header_map(pairs: &HashMap<String, String>) -> HeaderMap {
    let mut map = HeaderMap::new();
    for (k, v) in pairs {
        if let (Ok(name), Ok(value)) = (HeaderName::from_str(k), HeaderValue::from_str(v)) {
            map.insert(name, value);
        }
    }
    map
}

#[cfg(test)]
mod tests {
    use super::Webhook;
    use serde_json::json;
    use std::collections::HashMap;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    /// A scripted HTTP server. Accepts one connection per `statuses` entry and
    /// records the raw request head so tests can assert headers and bodies.
    fn spawn_responder(
        statuses: Vec<&'static str>,
    ) -> (String, Arc<AtomicUsize>, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let hits = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let hits_w = hits.clone();
        let requests_w = requests.clone();

        std::thread::spawn(move || {
            for status in statuses {
                let (mut sock, _) = listener.accept().unwrap();
                hits_w.fetch_add(1, Ordering::SeqCst);
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                loop {
                    match sock.read(&mut chunk) {
                        Ok(0) => break,
                        Ok(n) => {
                            buf.extend_from_slice(&chunk[..n]);
                            if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                requests_w
                    .lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&buf).to_string());
                let response = format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\n\r\n");
                let _ = sock.write_all(response.as_bytes());
            }
        });

        (format!("http://{addr}"), hits, requests)
    }

    #[tokio::test]
    async fn sends_idempotency_key_when_msg_id_given() {
        let (url, hits, requests) = spawn_responder(vec!["200 OK"]);
        let webhook = Webhook::with_retries(0);

        webhook
            .post(&url, &HashMap::new(), &json!({"a": 1}), Some("msg-42"))
            .await
            .unwrap();

        assert_eq!(hits.load(Ordering::SeqCst), 1);
        let raw = &requests.lock().unwrap()[0];
        assert!(
            raw.contains("idempotency-key: msg-42"),
            "raw request: {raw}"
        );
        assert!(
            raw.contains("content-type: application/json"),
            "raw request: {raw}"
        );
    }

    #[tokio::test]
    async fn no_key_without_msg_id() {
        let (url, hits, requests) = spawn_responder(vec!["200 OK"]);
        let webhook = Webhook::with_retries(0);

        webhook
            .post(&url, &HashMap::new(), &json!(1), None)
            .await
            .unwrap();

        let raw = &requests.lock().unwrap()[0];
        assert!(!raw.contains("idempotency-key"), "raw request: {raw}");
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retries_until_success() {
        let (url, hits, _) = spawn_responder(vec!["500 Internal Server Error", "200 OK"]);
        let webhook = Webhook::with_retries(2);

        webhook
            .post(&url, &HashMap::new(), &json!(1), None)
            .await
            .unwrap();

        assert_eq!(hits.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn no_retry_when_retries_zero() {
        let (url, hits, _) = spawn_responder(vec!["500 Internal Server Error"]);
        let webhook = Webhook::with_retries(0);

        assert!(webhook
            .post(&url, &HashMap::new(), &json!(1), None)
            .await
            .is_err());
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }
}
