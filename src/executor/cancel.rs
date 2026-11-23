//! Interrupting a connection's work: the client cancels the running request
//! (a TDS attention signal), `KILL QUERY` or `KILL CONNECTION` names the
//! connection, or the client goes away.
//!
//! Each connection has one [`Interrupt`]. Its engine checks it between
//! result rows; its protocol checks it between statements and while idle;
//! and a connection runs inside [`scope`], so that work which waits inside
//! an expression (`SLEEP`) can ask [`requested`]. Without a scope nothing
//! is ever interrupted.

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::runtime::{Handle, RuntimeFlavor};
use tokio::sync::Notify;

/// Why a connection's work was interrupted; several may be raised at once.
pub mod reason {
    /// The client cancelled the running request (TDS attention).
    pub const CLIENT_CANCEL: u8 = 1;
    /// `KILL QUERY`: stop the running statement; the session goes on.
    pub const KILL_QUERY: u8 = 2;
    /// `KILL [CONNECTION]`: stop, and end the connection.
    pub const KILL_CONNECTION: u8 = 4;
    /// The client closed the connection. Requests it sent before are
    /// still carried out; the one running is interrupted.
    pub const CLIENT_GONE: u8 = 8;
    /// A KILL of either kind.
    pub const KILLED: u8 = KILL_QUERY | KILL_CONNECTION;
}

/// The interruption state of one connection, shared with whoever may
/// interrupt it: its protocol's reader, and other sessions' `KILL`.
#[derive(Default)]
pub struct Interrupt {
    reasons: AtomicU8,
    /// A wait (`SLEEP`) was cut short by the interruption: as in MySQL, a
    /// statement whose interruption a `SLEEP` took returns its result.
    sleep_interrupted: AtomicBool,
    /// Wakes the connection when it is idle and is killed.
    wake: Notify,
}

impl Interrupt {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Interrupt for `reasons` (see [`reason`]).
    pub fn raise(&self, reasons: u8) {
        self.reasons.fetch_or(reasons, Ordering::Relaxed);
        // One waiter, the connection itself; a permit is kept if it is not
        // waiting yet.
        self.wake.notify_one();
    }

    /// Whether any of `reasons` is raised.
    pub fn has(&self, reasons: u8) -> bool {
        self.reasons.load(Ordering::Relaxed) & reasons != 0
    }

    /// Whether the connection's work is interrupted, for any reason.
    pub fn is_raised(&self) -> bool {
        self.reasons.load(Ordering::Relaxed) != 0
    }

    /// Clear `reasons`; whether any of them was raised.
    pub fn take(&self, reasons: u8) -> bool {
        self.reasons.fetch_and(!reasons, Ordering::Relaxed) & reasons != 0
    }

    /// Whether a wait was cut short by the interruption since this was last
    /// asked; asking clears it.
    pub fn take_sleep_interrupted(&self) -> bool {
        self.sleep_interrupted.swap(false, Ordering::Relaxed)
    }

    /// Wait until the connection is killed (`KILL CONNECTION`).
    pub async fn killed(&self) {
        while !self.has(reason::KILL_CONNECTION) {
            self.wake.notified().await;
        }
    }
}

tokio::task_local! {
    static CURRENT: Arc<Interrupt>;
}

/// Run `future` (a connection's work) with `interrupt` as its interruption
/// state.
pub async fn scope<F: Future>(interrupt: Arc<Interrupt>, future: F) -> F::Output {
    CURRENT.scope(interrupt, future).await
}

/// Whether the running connection's work has been interrupted.
pub fn requested() -> bool {
    CURRENT
        .try_with(|interrupt| interrupt.is_raised())
        .unwrap_or(false)
}

/// Record that a wait was cut short by the interruption.
fn note_sleep_interrupted() {
    let _ =
        CURRENT.try_with(|interrupt| interrupt.sleep_interrupted.store(true, Ordering::Relaxed));
}

/// How often a wait looks for an interruption.
const POLL: Duration = Duration::from_millis(10);

/// Wait for `duration` (forever, if `None`) unless the connection's work is
/// interrupted first; true if it was. On a multi-threaded runtime the
/// thread is handed over for the wait, so that other connections on it
/// carry on.
pub fn wait(duration: Option<Duration>) -> bool {
    let deadline = duration.and_then(|d| Instant::now().checked_add(d));
    let wait = || loop {
        if requested() {
            note_sleep_interrupted();
            return true;
        }
        let pause = match deadline {
            Some(deadline) => match deadline.checked_duration_since(Instant::now()) {
                Some(left) if !left.is_zero() => left.min(POLL),
                _ => return false,
            },
            None => POLL,
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
    async fn an_interrupt_ends_the_wait() {
        let interrupt = Interrupt::new();
        let raiser = interrupt.clone();
        let interrupted = scope(interrupt, async move {
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                raiser.raise(reason::KILL_QUERY);
            });
            // Forever, unless the spawned task can run while this waits
            let interrupted = wait(None);
            assert!(CURRENT.with(|i| i.take_sleep_interrupted()));
            interrupted
        })
        .await;
        assert!(interrupted);
    }

    #[test]
    fn reasons_are_raised_and_taken_separately() {
        let interrupt = Interrupt::new();
        assert!(!interrupt.is_raised());
        interrupt.raise(reason::CLIENT_CANCEL);
        interrupt.raise(reason::KILL_QUERY);
        assert!(interrupt.has(reason::KILLED));
        assert!(!interrupt.has(reason::KILL_CONNECTION));
        assert!(interrupt.take(reason::KILL_QUERY));
        assert!(!interrupt.take(reason::KILL_QUERY));
        assert!(interrupt.is_raised(), "the client's cancel stays");
        assert!(interrupt.take(reason::CLIENT_CANCEL));
        assert!(!interrupt.is_raised());
    }

    #[tokio::test]
    async fn killed_returns_once_the_connection_is_killed() {
        let interrupt = Interrupt::new();
        // Neither ends the connection by itself
        interrupt.raise(reason::KILL_QUERY);
        interrupt.raise(reason::CLIENT_GONE);
        let waited = tokio::time::timeout(Duration::from_millis(50), interrupt.killed()).await;
        assert!(waited.is_err());
        let waiter = interrupt.clone();
        let ended = tokio::spawn(async move { waiter.killed().await });
        interrupt.raise(reason::KILL_CONNECTION);
        tokio::time::timeout(Duration::from_secs(5), ended)
            .await
            .expect("killed() returned")
            .unwrap();
    }
}
