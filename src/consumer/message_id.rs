//! Derive a stable message identity for dedupe from the broker's native id.

/// The broker's native message identity, supplied by the consumer.
pub enum NativeId {
    /// A broker-provided key/property (Kafka record key, AMQP `message_id`).
    Provided(String),
    /// Kafka's stable record position — rendered as `<partition>:<offset>`.
    KafkaPosition(i32, i64),
    /// No native identity available (e.g. an AMQP delivery without a
    /// `message_id` property).
    None,
}

/// Derive the stable identity used for dedupe and idempotent sink keys.
///
/// Returns `None` when the broker has no stable native id. There is
/// deliberately no payload-hash fallback: two distinct messages with identical
/// bodies would otherwise collide in the dedupe cache and one would be falsely
/// dropped.
pub fn derive_message_id(source: NativeId) -> Option<String> {
    match source {
        NativeId::Provided(id) if !id.is_empty() => Some(id),
        NativeId::Provided(_) => None,
        NativeId::KafkaPosition(partition, offset) => Some(format!("{partition}:{offset}")),
        NativeId::None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{derive_message_id, NativeId};

    #[test]
    fn provided_key_is_stable_across_redelivery() {
        let id = derive_message_id(NativeId::Provided("order-42".to_string()));
        assert_eq!(id.as_deref(), Some("order-42"));
        // A redelivery carries the same key, so the identity is unchanged.
        assert_eq!(
            id,
            derive_message_id(NativeId::Provided("order-42".to_string()))
        );
    }

    #[test]
    fn kafka_position_is_stable_across_redelivery() {
        let id = derive_message_id(NativeId::KafkaPosition(3, 42));
        assert_eq!(id.as_deref(), Some("3:42"));
        // Same record redelivered at the same partition/offset — same identity.
        assert_eq!(id, derive_message_id(NativeId::KafkaPosition(3, 42)));
    }

    #[test]
    fn empty_provided_id_is_treated_as_absent() {
        assert_eq!(derive_message_id(NativeId::Provided(String::new())), None);
    }

    #[test]
    fn no_native_id_means_no_dedupe_not_a_hash() {
        // Two distinct messages with identical bodies and no native id must not
        // share an identity — a payload hash would falsely dedupe them.
        assert_eq!(derive_message_id(NativeId::None), None);
        assert_eq!(derive_message_id(NativeId::None), None);
    }
}
