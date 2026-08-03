use anyhow::Result;
use std::future::Future;
use tokio::sync::watch;

/// Outcome of a single session run.
pub enum SessionExit {
    /// The connection could not be established (or the session died before any
    /// healthy run). Counts as one more consecutive failure.
    Reconnect,
    /// The session ran healthily and then the connection was lost. Resets the
    /// consecutive-failure counter before counting, so a stale retry budget
    /// from an earlier outage is not exhausted by a fresh drop.
    ReconnectAfterHealthy,
    /// Clean shutdown requested — end the whole run successfully.
    Shutdown,
    /// Fatal error — abort the whole run.
    Fatal(anyhow::Error),
}

/// Reconnection policy shared by the consume, listen, and replicate commands.
#[derive(Debug, Clone, Copy)]
pub struct ReconnectConfig {
    /// Max consecutive failures before giving up (0 = retry forever).
    pub max_attempts: u32,
    /// Backoff base in milliseconds.
    pub base_ms: u64,
    /// Backoff cap in milliseconds.
    pub max_ms: u64,
}

/// Shared shutdown signal. Each session receives a clone so it can stop
/// cooperatively (clean protocol close, final sink flush) before the
/// session-loop ends the run.
#[derive(Clone)]
pub struct Shutdown(watch::Receiver<bool>);

impl Shutdown {
    /// Build a shutdown handle from a watch receiver. `run()` constructs the
    /// shared handle internally; a constructor is exposed so tests can drive a
    /// session directly.
    #[cfg(test)]
    pub(crate) fn from_receiver(rx: watch::Receiver<bool>) -> Self {
        Self(rx)
    }

    /// Resolves when shutdown is signalled (or the signal task is dropped).
    pub async fn wait(&mut self) {
        if *self.0.borrow() {
            return;
        }
        let _ = self.0.changed().await;
    }
}

/// Run sessions until shutdown or a fatal error.
///
/// The session-loop owns the reconnection policy: after a [`SessionExit::Reconnect`]
/// it backs off (exponentially, via [`crate::utils::backoff::delay`]) and runs
/// the factory again; after [`SessionExit::ReconnectAfterHealthy`] it resets
/// the failure counter before counting; [`SessionExit::Shutdown`] ends the run
/// cleanly; [`SessionExit::Fatal`] aborts with the error.
///
/// The `shutdown` future is polled while backing off; an in-flight session
/// observes it through the [`Shutdown`] handle passed to the factory and ends
/// cooperatively. `max_attempts > 0` consecutive failures abort the run.
pub async fn run<F, Fut, S>(mut factory: F, shutdown: S, cfg: &ReconnectConfig) -> Result<()>
where
    F: FnMut(Shutdown) -> Fut,
    Fut: Future<Output = SessionExit>,
    S: Future<Output = ()> + Send + 'static,
{
    let (tx, rx) = watch::channel(false);
    let signal = Box::pin(shutdown);
    tokio::spawn(async move {
        signal.await;
        let _ = tx.send(true);
    });

    let mut consecutive_failures: u32 = 0;
    let mut shutdown = Shutdown(rx);

    loop {
        // ── Backoff (skipped on first attempt) ────────────────────────────────
        if consecutive_failures > 0 {
            let infinite = cfg.max_attempts == 0;

            if !infinite && consecutive_failures >= cfg.max_attempts {
                tracing::error!(
                    consecutive_failures,
                    max = cfg.max_attempts,
                    "Max reconnect attempts reached"
                );
                return Err(anyhow::anyhow!(
                    "Max reconnect attempts ({}) reached",
                    cfg.max_attempts
                ));
            }

            let delay = crate::utils::backoff::delay(consecutive_failures, cfg.base_ms, cfg.max_ms);

            tracing::warn!(
                consecutive_failures,
                delay_secs = delay.as_secs_f32(),
                max_attempts = if infinite {
                    "\u{221e}".to_string()
                } else {
                    cfg.max_attempts.to_string()
                },
                "Connection lost, reconnecting…"
            );

            tokio::select! {
                biased;
                _ = shutdown.wait() => {
                    tracing::info!("Signal received during backoff, shutting down cleanly");
                    return Ok(());
                }
                _ = tokio::time::sleep(delay) => {}
            }
        }

        // ── Run one session ──────────────────────────────────────────────────
        let outcome = factory(shutdown.clone()).await;

        match outcome {
            SessionExit::Reconnect => consecutive_failures += 1,
            SessionExit::ReconnectAfterHealthy => consecutive_failures = 1,
            SessionExit::Shutdown => return Ok(()),
            SessionExit::Fatal(e) => return Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{run, ReconnectConfig, SessionExit};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    fn cfg(max_attempts: u32) -> ReconnectConfig {
        ReconnectConfig {
            max_attempts,
            base_ms: 1,
            max_ms: 4,
        }
    }

    /// A shared call counter: the factory must be callable many times, and each
    /// invocation's future needs its own handle (closures can't return borrows).
    fn counter() -> Arc<AtomicU32> {
        Arc::new(AtomicU32::new(0))
    }

    fn count(calls: &Arc<AtomicU32>) -> u32 {
        calls.load(Ordering::Relaxed)
    }

    #[tokio::test(start_paused = true)]
    async fn ends_cleanly_on_shutdown() {
        let calls = counter();
        let calls_for_factory = calls.clone();
        let outcome = run(
            move |_| {
                let calls = calls_for_factory.clone();
                async move {
                    calls.fetch_add(1, Ordering::Relaxed);
                    SessionExit::Shutdown
                }
            },
            std::future::pending(),
            &cfg(0),
        )
        .await;
        assert!(outcome.is_ok());
        assert_eq!(count(&calls), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn reconnects_after_failure() {
        let calls = counter();
        let calls_for_factory = calls.clone();
        let outcome = run(
            move |_| {
                let calls = calls_for_factory.clone();
                async move {
                    let n = calls.fetch_add(1, Ordering::Relaxed) + 1;
                    if n == 1 {
                        SessionExit::Reconnect
                    } else {
                        SessionExit::Shutdown
                    }
                }
            },
            std::future::pending(),
            &cfg(0),
        )
        .await;
        assert!(outcome.is_ok());
        assert_eq!(count(&calls), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn gives_up_after_max_consecutive_failures() {
        let calls = counter();
        let calls_for_factory = calls.clone();
        let outcome = run(
            move |_| {
                let calls = calls_for_factory.clone();
                async move {
                    calls.fetch_add(1, Ordering::Relaxed);
                    SessionExit::Reconnect
                }
            },
            std::future::pending(),
            &cfg(2),
        )
        .await;
        assert!(outcome.is_err());
        assert_eq!(count(&calls), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn healthy_session_resets_the_failure_counter() {
        // max_attempts = 2 would exhaust after two plain Reconnects; a healthy
        // session in between resets the counter so a third attempt is allowed.
        let calls = counter();
        let calls_for_factory = calls.clone();
        let outcome = run(
            move |_| {
                let calls = calls_for_factory.clone();
                async move {
                    let n = calls.fetch_add(1, Ordering::Relaxed) + 1;
                    match n {
                        1 | 2 => SessionExit::ReconnectAfterHealthy,
                        _ => SessionExit::Shutdown,
                    }
                }
            },
            std::future::pending(),
            &cfg(2),
        )
        .await;
        assert!(outcome.is_ok());
        assert_eq!(count(&calls), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_during_backoff_ends_cleanly() {
        let calls = counter();
        let calls_for_factory = calls.clone();
        let shutdown = async {
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        let outcome = run(
            move |_| {
                let calls = calls_for_factory.clone();
                async move {
                    calls.fetch_add(1, Ordering::Relaxed);
                    SessionExit::Reconnect
                }
            },
            shutdown,
            &cfg(0),
        )
        .await;
        assert!(outcome.is_ok());
        assert!(count(&calls) > 1);
    }

    #[tokio::test(start_paused = true)]
    async fn fatal_error_propagates() {
        let outcome = run(
            |_| async move { SessionExit::Fatal(anyhow::anyhow!("boom")) },
            std::future::pending(),
            &cfg(0),
        )
        .await;
        assert_eq!(outcome.unwrap_err().to_string(), "boom");
    }

    #[tokio::test(start_paused = true)]
    async fn session_observes_shutdown_handle() {
        // Factory runs a session that only ends when its Shutdown handle fires.
        // The handle is wired to run()'s internal signal task, so firing the
        // shutdown future must let the session end cooperatively.
        let (tx, _rx) = tokio::sync::watch::channel(false);
        let signal_task = async move {
            let _ = tx.send(true);
        };
        let outcome = run(
            |sess_shutdown| async move {
                let mut sess_shutdown = sess_shutdown;
                sess_shutdown.wait().await;
                SessionExit::Shutdown
            },
            signal_task,
            &cfg(0),
        )
        .await;
        assert!(outcome.is_ok());
    }
}
