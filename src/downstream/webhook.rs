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
            self.client
                .post(&self.url)
                .json(&msg)
                .send()
                .await
                .context("Webhook POST failed")?
                .error_for_status()
                .context("Webhook returned error status")?;

            Ok(())
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

                self.client
                    .post(url)
                    .headers(headers)
                    .json(&contract.data)
                    .send()
                    .await
                    .context("Webhook POST failed")?
                    .error_for_status()
                    .context("Webhook returned error status")?;
            } else {
                let msg = SimpleMessage::from(event);
                let headers = build_header_map(&self.default_headers);
                self.client
                    .post(&self.default_url)
                    .headers(headers)
                    .json(&msg)
                    .send()
                    .await
                    .context("Webhook POST failed")?
                    .error_for_status()
                    .context("Webhook returned error status")?;
            }

            Ok(())
        }
    }
}
