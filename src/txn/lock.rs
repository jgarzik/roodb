//! Row locks.
//!
//! Writers wait for each other here, as they do in InnoDB, instead of
//! finding out at commit that they collided.
//!
//! Three kinds of lock, all held by a transaction until it ends:
//!
//! - A **record lock** on one row key, shared or exclusive. Shared locks are
//!   compatible with each other; an exclusive lock is compatible with
//!   nothing held by another transaction.
//! - A **gap lock** on an open interval of the key space, `(low, high)`. It
//!   stops other transactions inserting into the interval and nothing else:
//!   gap locks never conflict with each other, and never wait.
//! - An **insert intention** at a key: the wish to insert there. It waits
//!   while another transaction holds a gap lock covering the key, and once
//!   granted leaves nothing behind (the inserter takes a record lock on the
//!   new row next).
//!
//! A range that a statement has read is locked by a gap lock over the range
//! plus a record lock on every row in it; together they are what InnoDB
//! calls next-key locks.
//!
//! Requests on a record are granted in the order they arrived: a request is
//! not let past an earlier one it conflicts with, so a stream of readers
//! cannot starve a writer.
//!
//! The locks exist only in the memory of the node that holds them. They
//! protect changes that have not been committed, which exist only on that
//! node too; nothing here is replicated.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::oneshot;

/// How a record is locked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    Shared,
    Exclusive,
}

impl LockMode {
    fn compatible(self, other: LockMode) -> bool {
        self == LockMode::Shared && other == LockMode::Shared
    }

    fn covers(self, wanted: LockMode) -> bool {
        self == LockMode::Exclusive || wanted == LockMode::Shared
    }
}

/// Why a lock was not granted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockError {
    /// Granting the lock would have left a cycle of transactions waiting
    /// for each other. The requester is the one refused.
    Deadlock,
    /// The lock was not granted within the time allowed.
    Timeout,
}

#[derive(Debug)]
enum Wanted {
    Record(LockMode),
    Insert,
}

struct Waiter {
    id: u64,
    txn: u64,
    key: Vec<u8>,
    wanted: Wanted,
    granted: oneshot::Sender<()>,
}

#[derive(Default)]
struct State {
    /// Record locks: who holds each key, and how.
    records: HashMap<Vec<u8>, Vec<(u64, LockMode)>>,
    /// Gap locks: `(low, high, transaction)`, both ends exclusive.
    gaps: Vec<(Vec<u8>, Vec<u8>, u64)>,
    /// Requests that could not be granted at once, oldest first.
    waiters: Vec<Waiter>,
    /// Keys each transaction holds record locks on, for release.
    held: HashMap<u64, HashSet<Vec<u8>>>,
    next_waiter: u64,
}

impl State {
    /// Transactions whose locks, or whose earlier requests, stand in the way
    /// of this request. Empty means it can be granted now. `before` limits
    /// the earlier requests considered to those ahead of that waiter.
    fn blockers(&self, txn: u64, key: &[u8], wanted: &Wanted, before: Option<u64>) -> Vec<u64> {
        let mut blockers = Vec::new();
        match wanted {
            Wanted::Record(mode) => {
                if let Some(holders) = self.records.get(key) {
                    for &(holder, held) in holders {
                        if holder != txn && !mode.compatible(held) {
                            blockers.push(holder);
                        }
                    }
                }
                // Not past an earlier request for the same record that this
                // one conflicts with.
                for waiter in &self.waiters {
                    if Some(waiter.id) == before {
                        break;
                    }
                    if waiter.txn == txn || waiter.key != key {
                        continue;
                    }
                    if let Wanted::Record(theirs) = waiter.wanted {
                        if !mode.compatible(theirs) {
                            blockers.push(waiter.txn);
                        }
                    }
                }
            }
            Wanted::Insert => {
                for (low, high, holder) in &self.gaps {
                    if *holder != txn && low.as_slice() < key && key < high.as_slice() {
                        blockers.push(*holder);
                    }
                }
            }
        }
        blockers.sort_unstable();
        blockers.dedup();
        blockers
    }

    fn grant_record(&mut self, txn: u64, key: &[u8], mode: LockMode) {
        let holders = self.records.entry(key.to_vec()).or_default();
        match holders.iter_mut().find(|(holder, _)| *holder == txn) {
            Some((_, held)) => {
                if mode == LockMode::Exclusive {
                    *held = LockMode::Exclusive;
                }
            }
            None => holders.push((txn, mode)),
        }
        self.held.entry(txn).or_default().insert(key.to_vec());
    }

    /// Whether `from`, by waiting for `blockers`, would close a cycle: some
    /// chain of waiting transactions leads from a blocker back to `from`.
    fn closes_cycle(&self, from: u64, blockers: &[u64]) -> bool {
        let mut seen = HashSet::new();
        let mut stack: Vec<u64> = blockers.to_vec();
        while let Some(txn) = stack.pop() {
            if txn == from {
                return true;
            }
            if !seen.insert(txn) {
                continue;
            }
            // A transaction waits for at most one thing at a time.
            if let Some(waiter) = self.waiters.iter().find(|w| w.txn == txn) {
                stack.extend(self.blockers(txn, &waiter.key, &waiter.wanted, Some(waiter.id)));
            }
        }
        false
    }

    /// Grant every waiting request that nothing stands in the way of any
    /// more, oldest first.
    fn wake(&mut self) {
        let mut index = 0;
        while index < self.waiters.len() {
            let waiter = &self.waiters[index];
            if !self
                .blockers(waiter.txn, &waiter.key, &waiter.wanted, Some(waiter.id))
                .is_empty()
            {
                index += 1;
                continue;
            }
            let waiter = self.waiters.remove(index);
            if let Wanted::Record(mode) = waiter.wanted {
                self.grant_record(waiter.txn, &waiter.key, mode);
            }
            // The requester may have given up (timed out, disconnected)
            // between being granted the lock and hearing of it. It then
            // releases what it holds as any transaction does when it ends.
            let _ = waiter.granted.send(());
        }
    }

    fn remove_waiter(&mut self, id: u64) {
        if let Some(index) = self.waiters.iter().position(|w| w.id == id) {
            self.waiters.remove(index);
            // Requests behind it may have been waiting for it alone.
            self.wake();
        }
    }
}

/// The lock table of one node.
pub struct LockManager {
    state: Mutex<State>,
    /// How long a new session waits for a lock, in seconds
    /// (`innodb_lock_wait_timeout`, global).
    default_wait_secs: std::sync::atomic::AtomicU64,
}

/// MySQL's default for `innodb_lock_wait_timeout`.
pub const DEFAULT_LOCK_WAIT_SECS: u64 = 50;

impl Default for LockManager {
    fn default() -> Self {
        Self {
            state: Mutex::new(State::default()),
            default_wait_secs: std::sync::atomic::AtomicU64::new(DEFAULT_LOCK_WAIT_SECS),
        }
    }
}

impl std::fmt::Debug for LockManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (records, gaps, waiting) = self.counts();
        f.debug_struct("LockManager")
            .field("records", &records)
            .field("gaps", &gaps)
            .field("waiting", &waiting)
            .finish()
    }
}

/// Removes a request from the queue if the requester stops waiting for it.
struct WaitGuard<'a> {
    manager: &'a LockManager,
    id: u64,
    done: bool,
}

impl Drop for WaitGuard<'_> {
    fn drop(&mut self) {
        if !self.done {
            self.manager.state.lock().remove_waiter(self.id);
        }
    }
}

impl LockManager {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// How long sessions that have not chosen otherwise wait for a lock.
    pub fn default_wait(&self) -> Duration {
        Duration::from_secs(
            self.default_wait_secs
                .load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    /// Change the wait for sessions that start from now on.
    pub fn set_default_wait(&self, wait: Duration) {
        self.default_wait_secs
            .store(wait.as_secs(), std::sync::atomic::Ordering::Relaxed);
    }

    /// Lock one row for `txn`, waiting up to `timeout` for transactions
    /// that hold it in a conflicting mode. A lock the transaction already
    /// holds is kept, and strengthened if an exclusive lock is asked for.
    ///
    /// Returns whether the lock is new to the transaction: only such a lock
    /// may be given back with `unlock_record`.
    pub async fn lock_record(
        &self,
        txn: u64,
        key: &[u8],
        mode: LockMode,
        timeout: Duration,
    ) -> Result<bool, LockError> {
        self.acquire(txn, key, Wanted::Record(mode), timeout).await
    }

    /// Lock one row for `txn` if that can be done without waiting. `None`
    /// means another transaction stands in the way. Otherwise as
    /// `lock_record`.
    pub fn try_lock_record(&self, txn: u64, key: &[u8], mode: LockMode) -> Option<bool> {
        let mut state = self.state.lock();
        let held = state
            .records
            .get(key)
            .and_then(|holders| holders.iter().find(|(holder, _)| *holder == txn))
            .map(|(_, held)| *held);
        if held.is_some_and(|held| held.covers(mode)) {
            return Some(false);
        }
        if !state
            .blockers(txn, key, &Wanted::Record(mode), None)
            .is_empty()
        {
            return None;
        }
        state.grant_record(txn, key, mode);
        Some(held.is_none())
    }

    /// Give back a record lock before the transaction ends: for a row that
    /// was locked in order to look at it and turned out not to be wanted.
    pub fn unlock_record(&self, txn: u64, key: &[u8]) {
        let mut state = self.state.lock();
        let Some(holders) = state.records.get_mut(key) else {
            return;
        };
        holders.retain(|(holder, _)| *holder != txn);
        if holders.is_empty() {
            state.records.remove(key);
        }
        if let Some(held) = state.held.get_mut(&txn) {
            held.remove(key);
        }
        state.wake();
    }

    /// Wait until no other transaction holds a gap lock covering `key`, so
    /// that `txn` may insert a row there.
    pub async fn lock_insert(
        &self,
        txn: u64,
        key: &[u8],
        timeout: Duration,
    ) -> Result<(), LockError> {
        self.acquire(txn, key, Wanted::Insert, timeout)
            .await
            .map(|_| ())
    }

    /// Stop other transactions inserting any key strictly between `low` and
    /// `high`. Never waits: gap locks do not conflict with each other.
    pub fn lock_gap(&self, txn: u64, low: &[u8], high: &[u8]) {
        if low >= high {
            return;
        }
        let mut state = self.state.lock();
        let covered = state
            .gaps
            .iter()
            .any(|(l, h, holder)| *holder == txn && l.as_slice() <= low && high <= h.as_slice());
        if !covered {
            state.gaps.push((low.to_vec(), high.to_vec(), txn));
        }
    }

    async fn acquire(
        &self,
        txn: u64,
        key: &[u8],
        wanted: Wanted,
        timeout: Duration,
    ) -> Result<bool, LockError> {
        // Whether the transaction holds nothing on the key yet. That cannot
        // change while it waits: a transaction does one thing at a time.
        let new;
        let (id, granted) = {
            let mut state = self.state.lock();
            let held = state
                .records
                .get(key)
                .and_then(|holders| holders.iter().find(|(holder, _)| *holder == txn))
                .map(|(_, held)| *held);
            new = held.is_none();
            if let Wanted::Record(mode) = wanted {
                if held.is_some_and(|held| held.covers(mode)) {
                    return Ok(false);
                }
            }
            let blockers = state.blockers(txn, key, &wanted, None);
            if blockers.is_empty() {
                if let Wanted::Record(mode) = wanted {
                    state.grant_record(txn, key, mode);
                }
                return Ok(new);
            }
            if state.closes_cycle(txn, &blockers) {
                return Err(LockError::Deadlock);
            }
            let id = state.next_waiter;
            state.next_waiter += 1;
            let (tx, rx) = oneshot::channel();
            state.waiters.push(Waiter {
                id,
                txn,
                key: key.to_vec(),
                wanted,
                granted: tx,
            });
            (id, rx)
        };

        let mut guard = WaitGuard {
            manager: self,
            id,
            done: false,
        };
        match tokio::time::timeout(timeout, granted).await {
            Ok(Ok(())) => {
                guard.done = true;
                Ok(new)
            }
            // The sender is dropped only with the manager's state.
            Ok(Err(_)) => Err(LockError::Timeout),
            Err(_) => {
                // Granted at the very moment the time ran out: the lock is
                // held, and that is what the caller is told.
                let mut state = self.state.lock();
                if state.waiters.iter().any(|w| w.id == id) {
                    state.remove_waiter(id);
                    guard.done = true;
                    Err(LockError::Timeout)
                } else {
                    guard.done = true;
                    Ok(new)
                }
            }
        }
    }

    /// Release everything `txn` holds, and abandon anything it is waiting
    /// for. Called when the transaction commits, rolls back, or its session
    /// ends.
    pub fn release_all(&self, txn: u64) {
        let mut state = self.state.lock();
        let had_gaps = state.gaps.iter().any(|(_, _, holder)| *holder == txn);
        state.gaps.retain(|(_, _, holder)| *holder != txn);
        let keys = state.held.remove(&txn).unwrap_or_default();
        for key in &keys {
            if let Some(holders) = state.records.get_mut(key) {
                holders.retain(|(holder, _)| *holder != txn);
                if holders.is_empty() {
                    state.records.remove(key);
                }
            }
        }
        let waiting = state.waiters.len();
        state.waiters.retain(|w| w.txn != txn);
        if had_gaps || !keys.is_empty() || waiting != state.waiters.len() {
            state.wake();
        }
    }

    /// Number of record locks held, gap locks held, and requests waiting.
    pub fn counts(&self) -> (usize, usize, usize) {
        let state = self.state.lock();
        (
            state.records.values().map(Vec::len).sum(),
            state.gaps.len(),
            state.waiters.len(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LONG: Duration = Duration::from_secs(60);
    const SHORT: Duration = Duration::from_millis(20);

    /// Wait until `n` requests are queued, so a test knows a task is
    /// waiting rather than guessing how long that takes.
    async fn until_waiting(manager: &LockManager, n: usize) {
        while manager.counts().2 < n {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn shared_locks_coexist_and_exclusive_waits() {
        let m = LockManager::new();
        m.lock_record(1, b"k", LockMode::Shared, LONG)
            .await
            .unwrap();
        m.lock_record(2, b"k", LockMode::Shared, LONG)
            .await
            .unwrap();
        assert_eq!(
            m.lock_record(3, b"k", LockMode::Exclusive, SHORT).await,
            Err(LockError::Timeout)
        );
        // The timed-out request left nothing behind.
        assert_eq!(m.counts(), (2, 0, 0));

        let waiter = {
            let m = m.clone();
            tokio::spawn(async move { m.lock_record(3, b"k", LockMode::Exclusive, LONG).await })
        };
        until_waiting(&m, 1).await;
        m.release_all(1);
        assert_eq!(m.counts().2, 1, "granted while a reader still holds it");
        m.release_all(2);
        waiter.await.unwrap().unwrap();
        assert_eq!(m.counts(), (1, 0, 0));
    }

    #[tokio::test]
    async fn a_transaction_does_not_wait_for_itself() {
        let m = LockManager::new();
        m.lock_record(1, b"k", LockMode::Shared, LONG)
            .await
            .unwrap();
        m.lock_record(1, b"k", LockMode::Exclusive, LONG)
            .await
            .unwrap();
        m.lock_record(1, b"k", LockMode::Shared, LONG)
            .await
            .unwrap();
        assert_eq!(m.counts(), (1, 0, 0));
        // Still exclusive: asking for less did not weaken it.
        assert_eq!(
            m.lock_record(2, b"k", LockMode::Shared, SHORT).await,
            Err(LockError::Timeout)
        );
        m.lock_gap(1, b"a", b"z");
        m.lock_insert(1, b"m", LONG).await.unwrap();
    }

    #[tokio::test]
    async fn requests_are_granted_in_order() {
        let m = LockManager::new();
        m.lock_record(1, b"k", LockMode::Shared, LONG)
            .await
            .unwrap();
        let writer = {
            let m = m.clone();
            tokio::spawn(async move { m.lock_record(2, b"k", LockMode::Exclusive, LONG).await })
        };
        until_waiting(&m, 1).await;
        // A reader arriving after the writer queues behind it, although the
        // lock is held in a mode it is compatible with.
        let reader = {
            let m = m.clone();
            tokio::spawn(async move { m.lock_record(3, b"k", LockMode::Shared, LONG).await })
        };
        until_waiting(&m, 2).await;
        m.release_all(1);
        writer.await.unwrap().unwrap();
        assert!(!reader.is_finished());
        m.release_all(2);
        reader.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn a_gap_lock_blocks_inserts_into_the_gap_only() {
        let m = LockManager::new();
        m.lock_gap(1, b"b", b"f");
        // Gap locks do not conflict with each other.
        m.lock_gap(2, b"a", b"z");
        // Outside transaction 1's gap, but inside transaction 2's.
        m.lock_insert(2, b"g", LONG).await.unwrap();
        // The ends are not part of the gap.
        m.release_all(2);
        m.lock_insert(3, b"b", LONG).await.unwrap();
        m.lock_insert(3, b"f", LONG).await.unwrap();
        assert_eq!(m.lock_insert(3, b"c", SHORT).await, Err(LockError::Timeout));
        let insert = {
            let m = m.clone();
            tokio::spawn(async move { m.lock_insert(3, b"c", LONG).await })
        };
        until_waiting(&m, 1).await;
        m.release_all(1);
        insert.await.unwrap().unwrap();
        assert_eq!(m.counts(), (0, 0, 0));
    }

    #[tokio::test]
    async fn a_deadlock_is_refused_to_the_one_who_would_close_it() {
        let m = LockManager::new();
        m.lock_record(1, b"a", LockMode::Exclusive, LONG)
            .await
            .unwrap();
        m.lock_record(2, b"b", LockMode::Exclusive, LONG)
            .await
            .unwrap();
        let first = {
            let m = m.clone();
            tokio::spawn(async move { m.lock_record(1, b"b", LockMode::Exclusive, LONG).await })
        };
        until_waiting(&m, 1).await;
        assert_eq!(
            m.lock_record(2, b"a", LockMode::Exclusive, LONG).await,
            Err(LockError::Deadlock)
        );
        // The victim rolls back; the other goes on.
        m.release_all(2);
        first.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn a_deadlock_through_a_gap_is_found() {
        let m = LockManager::new();
        // Both read the same empty range, then both try to insert into it.
        m.lock_gap(1, b"a", b"z");
        m.lock_gap(2, b"a", b"z");
        let first = {
            let m = m.clone();
            tokio::spawn(async move { m.lock_insert(1, b"m", LONG).await })
        };
        until_waiting(&m, 1).await;
        assert_eq!(m.lock_insert(2, b"n", LONG).await, Err(LockError::Deadlock));
        m.release_all(2);
        first.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn two_readers_upgrading_deadlock() {
        let m = LockManager::new();
        m.lock_record(1, b"k", LockMode::Shared, LONG)
            .await
            .unwrap();
        m.lock_record(2, b"k", LockMode::Shared, LONG)
            .await
            .unwrap();
        let first = {
            let m = m.clone();
            tokio::spawn(async move { m.lock_record(1, b"k", LockMode::Exclusive, LONG).await })
        };
        until_waiting(&m, 1).await;
        assert_eq!(
            m.lock_record(2, b"k", LockMode::Exclusive, LONG).await,
            Err(LockError::Deadlock)
        );
        m.release_all(2);
        first.await.unwrap().unwrap();
        assert_eq!(m.counts(), (1, 0, 0));
    }

    #[tokio::test]
    async fn a_request_abandoned_while_waiting_lets_others_through() {
        let m = LockManager::new();
        m.lock_record(1, b"k", LockMode::Shared, LONG)
            .await
            .unwrap();
        let writer = {
            let m = m.clone();
            tokio::spawn(async move { m.lock_record(2, b"k", LockMode::Exclusive, LONG).await })
        };
        until_waiting(&m, 1).await;
        let reader = {
            let m = m.clone();
            tokio::spawn(async move { m.lock_record(3, b"k", LockMode::Shared, LONG).await })
        };
        until_waiting(&m, 2).await;
        // The writer's session goes away mid-wait.
        writer.abort();
        let _ = writer.await;
        reader.await.unwrap().unwrap();
        assert_eq!(m.counts(), (2, 0, 0));
    }

    #[tokio::test]
    async fn trying_for_a_lock_never_waits() {
        let m = LockManager::new();
        assert_eq!(m.try_lock_record(1, b"k", LockMode::Shared), Some(true));
        assert_eq!(m.try_lock_record(1, b"k", LockMode::Shared), Some(false));
        assert_eq!(m.try_lock_record(2, b"k", LockMode::Shared), Some(true));
        assert_eq!(m.try_lock_record(2, b"k", LockMode::Exclusive), None);
        assert_eq!(m.try_lock_record(3, b"k", LockMode::Exclusive), None);
        m.release_all(1);
        // The only holder left may strengthen its lock.
        assert_eq!(m.try_lock_record(2, b"k", LockMode::Exclusive), Some(false));
        assert_eq!(m.counts(), (1, 0, 0));
    }

    #[tokio::test]
    async fn a_new_lock_can_be_given_back() {
        let m = LockManager::new();
        assert!(m
            .lock_record(1, b"k", LockMode::Shared, LONG)
            .await
            .unwrap());
        // Asking again, or for more, is not a new lock.
        assert!(!m
            .lock_record(1, b"k", LockMode::Shared, LONG)
            .await
            .unwrap());
        assert!(!m
            .lock_record(1, b"k", LockMode::Exclusive, LONG)
            .await
            .unwrap());
        let waiter = {
            let m = m.clone();
            tokio::spawn(async move { m.lock_record(2, b"k", LockMode::Exclusive, LONG).await })
        };
        until_waiting(&m, 1).await;
        m.unlock_record(1, b"k");
        assert!(waiter.await.unwrap().unwrap(), "new to the waiter");
        m.unlock_record(2, b"k");
        assert_eq!(m.counts(), (0, 0, 0));
        // Giving back what is not held does nothing.
        m.unlock_record(2, b"k");
    }

    #[tokio::test]
    async fn releasing_frees_everything_a_transaction_held() {
        let m = LockManager::new();
        for key in [b"a", b"b", b"c"] {
            m.lock_record(1, key, LockMode::Exclusive, LONG)
                .await
                .unwrap();
        }
        m.lock_gap(1, b"a", b"c");
        assert_eq!(m.counts(), (3, 1, 0));
        m.release_all(1);
        assert_eq!(m.counts(), (0, 0, 0));
        // Releasing again, or for a transaction that held nothing, is fine.
        m.release_all(1);
        m.release_all(99);
    }
}
