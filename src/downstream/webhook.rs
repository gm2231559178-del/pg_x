#[cfg(feature = "webhook")]
#[allow(clippy::module_inception)]
pub mod webhook {
    use anyhow::Result;
    use async_trait::async_trait;
    use std::collections::HashMap;

    use crate::downstream::{
        contract::{ContractMessage, NotifyEvent, SimpleMessage},
        delivery::webhook::Webhook,
        sink::Downstream,
    };

    /// POSTs every NOTIFY payload as JSON to a fixed URL.
    pub struct SimpleWebhookDownstream {
        webhook: Webhook,
        url: String,
    }

    impl SimpleWebhookDownstream {
        pub fn new(url: impl Into<String>) -> Self {
            Self {
                webhook: Webhook::new(),
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
            let body = serde_json::to_value(&msg)?;
            self.webhook
                .post(&self.url, &HashMap::new(), &body, None)
                .await
        }
    }

    /// Parses the NOTIFY payload as a [`ContractMessage`].
    /// The contract may override the target URL and inject extra headers.
    pub struct ContractWebhookDownstream {
        webhook: Webhook,
        default_url: String,
        default_headers: HashMap<String, String>,
    }

    impl ContractWebhookDownstream {
        pub fn new(
            default_url: impl Into<String>,
            default_headers: HashMap<String, String>,
        ) -> Self {
            Self {
                webhook: Webhook::new(),
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

                self.webhook.post(url, &merged, &contract.data, None).await
            } else {
                let msg = SimpleMessage::from(event);
                let body = serde_json::to_value(&msg)?;
                self.webhook
                    .post(&self.default_url, &self.default_headers, &body, None)
                    .await
            }
        }
    }
}
