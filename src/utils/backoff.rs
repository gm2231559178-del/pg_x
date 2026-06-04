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
    let delay_ms = base - jitter + (rand::random::<u64>() % (jitter * 2 + 1));
    Duration::from_millis(delay_ms)
}
