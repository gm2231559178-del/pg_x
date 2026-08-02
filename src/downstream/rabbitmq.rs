#[cfg(feature = "rabbitmq")]
#[allow(clippy::module_inception)]
pub mod rabbitmq {
    use anyhow::{Context, Result};
    use async_trait::async_trait;

    use crate::downstream::{
        contract::{ContractMessage, NotifyEvent, SimpleMessage},
        delivery::rabbitmq::Rabbitmq,
        sink::Downstream,
    };

    // ─────────────────────────────────────────────────────────────────────────
    // Simple mode
    // ─────────────────────────────────────────────────────────────────────────

    /// Publishes every NOTIFY payload verbatim as the AMQP body.
    /// Exchange and routing key are fixed at construction time.
    pub struct SimpleRabbitMqDownstream {
        rabbitmq: Rabbitmq,
        exchange: String,
        routing_key: String,
    }

    impl SimpleRabbitMqDownstream {
        pub async fn connect(
            amqp_url: &str,
            exchange: impl Into<String>,
            routing_key: impl Into<String>,
        ) -> Result<Self> {
            let exchange = exchange.into();
            let routing_key = routing_key.into();
            let rabbitmq = Rabbitmq::connect(amqp_url).await?;
            rabbitmq.declare_exchange(&exchange).await?;

            Ok(Self {
                rabbitmq,
                exchange,
                routing_key,
            })
        }
    }

    #[async_trait]
    impl Downstream for SimpleRabbitMqDownstream {
        fn name(&self) -> &str {
            "rabbitmq-simple"
        }

        async fn send(&self, event: &NotifyEvent) -> Result<()> {
            let msg = SimpleMessage::from(event);
            let body = serde_json::to_vec(&msg).context("Serialise SimpleMessage")?;
            self.rabbitmq
                .publish(&self.exchange, &self.routing_key, &[], &body)
                .await
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Contract mode
    // ─────────────────────────────────────────────────────────────────────────

    /// Parses the NOTIFY payload as a [`ContractMessage`] and uses the
    /// embedded `routing` hints for exchange, routing key, and AMQP headers.
    /// Falls back to the configured defaults when hints are absent.
    pub struct ContractRabbitMqDownstream {
        rabbitmq: Rabbitmq,
        default_exchange: String,
        default_routing_key: String,
    }

    impl ContractRabbitMqDownstream {
        pub async fn connect(
            amqp_url: &str,
            default_exchange: impl Into<String>,
            default_routing_key: impl Into<String>,
        ) -> Result<Self> {
            Ok(Self {
                rabbitmq: Rabbitmq::connect(amqp_url).await?,
                default_exchange: default_exchange.into(),
                default_routing_key: default_routing_key.into(),
            })
        }
    }

    #[async_trait]
    impl Downstream for ContractRabbitMqDownstream {
        fn name(&self) -> &str {
            "rabbitmq-contract"
        }

        async fn send(&self, event: &NotifyEvent) -> Result<()> {
            // Try to parse a ContractMessage; fall back to raw payload.
            let (exchange, routing_key, headers, body) =
                if let Some(contract) = ContractMessage::try_parse(&event.payload) {
                    let r = &contract.meta.routing;

                    let exchange = r
                        .rabbitmq_exchange
                        .clone()
                        .unwrap_or_else(|| self.default_exchange.clone());

                    let routing_key = r
                        .rabbitmq_routing_key
                        .clone()
                        .unwrap_or_else(|| self.default_routing_key.clone());

                    // Build the AMQP string headers from the contract.
                    let mut headers: Vec<(String, String)> = Vec::new();
                    for (k, v) in &r.rabbitmq_headers {
                        headers.push((k.clone(), v.clone()));
                    }
                    if let Some(et) = &contract.meta.event_type {
                        headers.push(("x-event-type".to_string(), et.clone()));
                    }
                    headers.push(("x-pg-channel".to_string(), event.channel.clone()));
                    headers.push((
                        "x-schema-version".to_string(),
                        contract.meta.schema_version.clone(),
                    ));

                    let body = event.payload.as_bytes().to_vec();

                    (exchange, routing_key, headers, body)
                } else {
                    // Plain payload — envelope it so consumers get consistent shape.
                    let msg = SimpleMessage::from(event);
                    let body = serde_json::to_vec(&msg).context("Serialise SimpleMessage")?;
                    (
                        self.default_exchange.clone(),
                        self.default_routing_key.clone(),
                        Vec::new(),
                        body,
                    )
                };

            self.rabbitmq
                .publish(&exchange, &routing_key, &headers, &body)
                .await
        }
    }
}
