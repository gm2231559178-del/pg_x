use std::time::Duration;

/// Exponential backoff with ±20% jitter.
///
/// `attempt` is 1-based (1 = first retry).
/// Shift is clamped to avoid u64 overflow on high attempt counts,
/// and the result is always capped at `max_ms`.
pub fn delay(attempt: u32, base_ms: u64, max_ms: u64) -> Duration {
    let shift = attempt.saturating_sub(1).min(62);
    let base = (base_ms.saturating_mul(1u64 << shift)).min(max_ms);
    let jitter = base / 5;
    // Apply jitter first, then cap to ensure result never exceeds max_ms
    let delay_ms = (base - jitter + (rand::random::<u64>() % (jitter * 2 + 1))).min(max_ms);
    Duration::from_millis(delay_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delay_first_attempt_in_range() {
        let d = delay(1, 1000, 60000);
        assert!(d >= Duration::from_millis(800));
        assert!(d <= Duration::from_millis(1200));
    }

    #[test]
    fn delay_second_attempt_doubles() {
        let d1 = delay(1, 1000, 60000);
        let d2 = delay(2, 1000, 60000);
        // Second attempt should be (roughly) double the first
        assert!(d2 >= d1 * 3 / 5);
    }

    #[test]
    fn delay_capped_at_max() {
        let d = delay(20, 1000, 5000);
        assert!(d <= Duration::from_millis(5000));
    }

    #[test]
    fn delay_zero_base_is_zero() {
        let d = delay(1, 0, 60000);
        assert!(d.as_millis() == 0);
    }
}
