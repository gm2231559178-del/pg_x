#[cfg(feature = "webhook")]
#[allow(clippy::module_inception)]
pub mod webhook {
    use anyhow::{Context, Result};
    use async_trait::async_trait;
    use reqwest::{
        header::{HeaderMap, HeaderName, HeaderValue},
        Client,
    };
    use std::str::FromStr;

    use crate::downstream::{
        contract::{ContractMessage, NotifyEvent, SimpleMessage},
        sink::Downstream,
    };

    fn build_header_map(pairs: &std::collections::HashMap<String, String>) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (k, v) in pairs {
            if let (Ok(name), Ok(value)) = (HeaderName::from_str(k), HeaderValue::from_str(v)) {
                map.insert(name, value);
            }
        }
        map
    }

    /// POSTs every NOTIFY payload as JSON to a fixed URL.
    pub struct SimpleWebhookDownstream {
        client: Client,
        url: String,
    }

    impl SimpleWebhookDownstream {
        pub fn new(url: impl Into<String>) -> Self {
            Self {
                client: Client::new(),
                url: url.into(),
            }
        }
    }

    #[async_trait]
    impl Downstream for SimpleWebhookDownstream {
        fn name(&self) -> &str {
            "webhook-simple"
        }

        async fn send(&self, event: &NotifyEvent) -> Result<()> {
            let msg = SimpleMessage::from(event);
            let req = self.client.post(&self.url).json(&msg);
            send_with_retry(&self.client, req).await
        }
    }

    async fn send_with_retry(_client: &Client, req: reqwest::RequestBuilder) -> Result<()> {
        let mut attempt = 0u32;
        loop {
            let result = req
                .try_clone()
                .ok_or_else(|| anyhow::anyhow!("Request body not clonable"))?
                .send()
                .await
                .and_then(|r| r.error_for_status());

            match result {
                Ok(_) => return Ok(()),
                Err(_) if attempt < 2 => {
                    attempt += 1;
                    let delay = std::time::Duration::from_millis(100 * 2u64.pow(attempt));
                    tokio::time::sleep(delay).await;
                }
                Err(e) => return Err(e).context("Webhook POST failed after 3 retries"),
            }
        }
    }

    /// Parses the NOTIFY payload as a [`ContractMessage`].
    /// The contract may override the target URL and inject extra headers.
    pub struct ContractWebhookDownstream {
        client: Client,
        default_url: String,
        default_headers: std::collections::HashMap<String, String>,
    }

    impl ContractWebhookDownstream {
        pub fn new(
            default_url: impl Into<String>,
            default_headers: std::collections::HashMap<String, String>,
        ) -> Self {
            Self {
                client: Client::new(),
                default_url: default_url.into(),
                default_headers,
            }
        }
    }

    #[async_trait]
    impl Downstream for ContractWebhookDownstream {
        fn name(&self) -> &str {
            "webhook-contract"
        }

        async fn send(&self, event: &NotifyEvent) -> Result<()> {
            if let Some(contract) = ContractMessage::try_parse(&event.payload) {
                let r = &contract.meta.routing;

                let url = r.webhook_url.as_deref().unwrap_or(&self.default_url);

                // Merge default headers, then overlay per-message headers.
                let mut merged = self.default_headers.clone();
                merged.extend(r.webhook_headers.clone());
                let headers = build_header_map(&merged);

                let req = self.client.post(url).headers(headers).json(&contract.data);
                send_with_retry(&self.client, req).await?;
            } else {
                let msg = SimpleMessage::from(event);
                let headers = build_header_map(&self.default_headers);
                let req = self
                    .client
                    .post(&self.default_url)
                    .headers(headers)
                    .json(&msg);
                send_with_retry(&self.client, req).await?;
            }

            Ok(())
        }
    }
}
