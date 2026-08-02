//! Transport handles shared by the sink seams (`ConsumeSink`, `Downstream`).
//!
//! One handle per transport — webhook, elasticsearch, kafka, rabbitmq, shell —
//! each hiding its mechanics (retry policy, idempotency keying, `_id`
//! derivation, buffering) behind a small method. Contract parsing and routing
//! stay caller-side: the seam decides the destination and payload, the handle
//! only transports it.

#[cfg(feature = "elasticsearch")]
pub mod elasticsearch;
#[cfg(feature = "kafka")]
pub mod kafka;
#[cfg(feature = "rabbitmq")]
pub mod rabbitmq;
pub mod shell;
#[cfg(feature = "webhook")]
pub mod webhook;
