//! Cancellation of the running request, for work that waits inside an
//! expression (`SLEEP`).
//!
//! A protocol that can cancel a request runs its connection inside
//! [`scope`] with the flag its reader raises; code evaluated on behalf of
//! that connection asks [`requested`]. Without a scope nothing is ever
//! cancelled.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::runtime::{Handle, RuntimeFlavor};

tokio::task_local! {
    static CANCEL: Arc<AtomicBool>;
}

/// Run `future` with `flag` as its cancellation signal.
pub async fn scope<F: Future>(flag: Arc<AtomicBool>, future: F) -> F::Output {
    CANCEL.scope(flag, future).await
}

/// Whether the running request has been cancelled.
pub fn requested() -> bool {
    CANCEL
        .try_with(|flag| flag.load(Ordering::Relaxed))
        .unwrap_or(false)
}

/// How often a wait looks for a cancellation.
const CANCEL_POLL: Duration = Duration::from_millis(10);

/// Wait for `duration` (forever, if `None`) unless the request is
/// cancelled first; true if it was. On a multi-threaded runtime the thread
/// is handed over for the wait, so that other connections on it carry on.
pub fn wait(duration: Option<Duration>) -> bool {
    let deadline = duration.and_then(|d| Instant::now().checked_add(d));
    let wait = || loop {
        if requested() {
            return true;
        }
        let pause = match deadline {
            Some(deadline) => match deadline.checked_duration_since(Instant::now()) {
                Some(left) if !left.is_zero() => left.min(CANCEL_POLL),
                _ => return false,
            },
            None => CANCEL_POLL,
        };
        std::thread::sleep(pause);
    };
    match Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(wait)
        }
        _ => wait(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn waits_without_a_scope() {
        let started = Instant::now();
        assert!(!wait(Some(Duration::from_millis(30))));
        assert!(started.elapsed() >= Duration::from_millis(30));
        assert!(!wait(Some(Duration::ZERO)));
        assert!(!requested());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn a_cancelled_scope_ends_the_wait() {
        let flag = Arc::new(AtomicBool::new(false));
        let raised = flag.clone();
        let interrupted = scope(flag, async move {
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                raised.store(true, Ordering::Relaxed);
            });
            // Forever, unless the spawned task can run while this waits
            wait(None)
        })
        .await;
        assert!(interrupted);
    }
}
