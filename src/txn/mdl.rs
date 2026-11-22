//! Metadata locks.
//!
//! A statement must not find that a table it is using has changed shape, or
//! gone, under it. As in MySQL, every statement takes a **shared** metadata
//! lock on each table it names and keeps it until its transaction ends; a
//! schema change takes an **exclusive** one on each table it changes, which
//! waits for every shared holder to finish and keeps new ones out.
//!
//! What is locked is a name, `(database, table)`, not a table: a name that
//! nothing is stored under yet can be locked too, which is how `CREATE
//! TABLE` and the new name of a `RENAME` are protected.
//!
//! Requests on a name are granted in the order they arrived, and a request
//! is not let past an earlier one it conflicts with: a waiting schema change
//! holds back shared requests that arrive after it, so a stream of readers
//! cannot starve it. The one exception is an upgrade. A transaction that
//! holds a name shared and asks for it exclusive waits only for the other
//! holders, not for the requests queued before its own: those are waiting
//! for it.
//!
//! Like row locks (`lock.rs`), these exist only in the memory of the node
//! that granted them.
//!
//! # Deadlocks
//!
//! A transaction can hold a row lock and wait for a metadata lock while
//! another holds the metadata lock and waits for the row. So that such a
//! cycle is found, every wait for a metadata lock is also recorded with the
//! row lock manager, which then holds the whole picture of who waits for
//! whom and is the one place a cycle is looked for. The request that would
//! close a cycle is the one refused, in either manager.
//!
//! # Mutex order
//!
//! This table's mutex first, then the row lock manager's. The row lock
//! manager never calls back into this module, so the reverse order cannot
//! happen. Neither mutex is held while a request waits.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::oneshot;
use tokio::time::Instant;

use super::lock::LockManager;

/// The name a metadata lock is taken on. Names are compared exactly as
/// given: the caller settles letter case before locking.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TableName {
    pub database: String,
    pub table: String,
}

impl TableName {
    pub fn new(database: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            database: database.into(),
            table: table.into(),
        }
    }
}

/// How a name is locked. Ordered by strength.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MdlMode {
    /// The table is in use and must stay as it is.
    Shared,
    /// The table is being changed, created or removed.
    Exclusive,
}

impl MdlMode {
    fn compatible(self, other: MdlMode) -> bool {
        self == MdlMode::Shared && other == MdlMode::Shared
    }

    fn covers(self, wanted: MdlMode) -> bool {
        self >= wanted
    }
}

/// Why a metadata lock was not granted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MdlError {
    /// Waiting for the lock would have left a cycle of transactions waiting
    /// for each other, through metadata locks, row locks or both. The
    /// requester is the one refused.
    Deadlock,
    /// The lock was not granted within the time allowed.
    Timeout,
}

struct Waiter {
    id: u64,
    owner: u64,
    mode: MdlMode,
    granted: oneshot::Sender<()>,
}

/// The locks on one name.
#[derive(Default)]
struct Table {
    holders: Vec<(u64, MdlMode)>,
    /// Requests that could not be granted at once, oldest first.
    waiters: Vec<Waiter>,
}

impl Table {
    fn held_by(&self, owner: u64) -> Option<MdlMode> {
        self.holders
            .iter()
            .find(|(holder, _)| *holder == owner)
            .map(|(_, mode)| *mode)
    }

    /// Owners whose locks, or whose earlier requests, stand in the way of
    /// this request. Empty means it can be granted now. `before` limits the
    /// earlier requests considered to those ahead of that waiter.
    fn blockers(&self, owner: u64, mode: MdlMode, before: Option<u64>) -> Vec<u64> {
        let mut blockers = Vec::new();
        for &(holder, held) in &self.holders {
            if holder != owner && !mode.compatible(held) {
                blockers.push(holder);
            }
        }
        // A holder asking for more is upgrading, and waits for the other
        // holders only: the requests queued before it wait for it.
        if self.held_by(owner).is_none() {
            for waiter in &self.waiters {
                if Some(waiter.id) == before {
                    break;
                }
                if waiter.owner != owner && !mode.compatible(waiter.mode) {
                    blockers.push(waiter.owner);
                }
            }
        }
        blockers.sort_unstable();
        blockers.dedup();
        blockers
    }

    fn grant(&mut self, owner: u64, mode: MdlMode) {
        match self.holders.iter_mut().find(|(holder, _)| *holder == owner) {
            Some((_, held)) => *held = (*held).max(mode),
            None => self.holders.push((owner, mode)),
        }
    }
}

/// What a change to the table means for those waiting: gathered while the
/// change is made, and acted on once it is complete.
#[derive(Default)]
struct Changes {
    /// Owners that wait no longer.
    ended: Vec<u64>,
    /// Owners still waiting, and whom each now waits for.
    waits: Vec<(u64, Vec<u64>)>,
    /// Requests granted, to be told so.
    granted: Vec<oneshot::Sender<()>>,
}

#[derive(Default)]
struct State {
    tables: HashMap<TableName, Table>,
    /// Names each owner holds, for release.
    held: HashMap<u64, HashSet<TableName>>,
    next_waiter: u64,
}

impl State {
    fn grant(&mut self, owner: u64, name: &TableName, mode: MdlMode) {
        self.tables
            .entry(name.clone())
            .or_default()
            .grant(owner, mode);
        self.held.entry(owner).or_default().insert(name.clone());
    }

    /// After the locks on `name` have changed: grant every waiting request
    /// that nothing stands in the way of any more, oldest first, and note
    /// whom the rest now wait for.
    fn settle(&mut self, name: &TableName, changes: &mut Changes) {
        let Some(table) = self.tables.get_mut(name) else {
            return;
        };
        let mut index = 0;
        while index < table.waiters.len() {
            let waiter = &table.waiters[index];
            if !table
                .blockers(waiter.owner, waiter.mode, Some(waiter.id))
                .is_empty()
            {
                index += 1;
                continue;
            }
            let waiter = table.waiters.remove(index);
            table.grant(waiter.owner, waiter.mode);
            self.held
                .entry(waiter.owner)
                .or_default()
                .insert(name.clone());
            changes.ended.push(waiter.owner);
            changes.granted.push(waiter.granted);
        }
        for waiter in &table.waiters {
            changes.waits.push((
                waiter.owner,
                table.blockers(waiter.owner, waiter.mode, Some(waiter.id)),
            ));
        }
        if table.holders.is_empty() && table.waiters.is_empty() {
            self.tables.remove(name);
        }
    }

    /// Take back what `owner` holds on `name`, or with `keep` weaken it to
    /// that mode. The caller settles the name afterwards.
    fn withdraw(&mut self, owner: u64, name: &TableName, keep: Option<MdlMode>) {
        let Some(table) = self.tables.get_mut(name) else {
            return;
        };
        match keep {
            Some(mode) => {
                if let Some((_, held)) = table.holders.iter_mut().find(|(h, _)| *h == owner) {
                    *held = mode;
                }
            }
            None => {
                table.holders.retain(|(holder, _)| *holder != owner);
                if let Some(names) = self.held.get_mut(&owner) {
                    names.remove(name);
                    if names.is_empty() {
                        self.held.remove(&owner);
                    }
                }
            }
        }
    }
}

/// What a granted request changed for its owner.
#[derive(Clone, Copy)]
enum Taken {
    /// Nothing: the owner already held the name in a mode that covers it.
    Nothing,
    /// The owner did not hold the name before.
    New,
    /// The owner held the name shared and now holds it exclusive.
    Upgraded,
}

/// The metadata lock table of one node.
pub struct MetadataLocks {
    state: Mutex<State>,
    /// Where waits are recorded, so that deadlocks through both kinds of
    /// lock are found.
    row_locks: Arc<LockManager>,
}

impl std::fmt::Debug for MetadataLocks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (held, waiting) = self.counts();
        f.debug_struct("MetadataLocks")
            .field("held", &held)
            .field("waiting", &waiting)
            .finish()
    }
}

/// Removes a request from the queue if the requester stops waiting for it.
struct WaitGuard<'a> {
    locks: &'a MetadataLocks,
    name: &'a TableName,
    id: u64,
    done: bool,
}

impl Drop for WaitGuard<'_> {
    fn drop(&mut self) {
        if !self.done {
            let mut state = self.locks.state.lock();
            self.locks.abandon(&mut state, self.name, self.id);
        }
    }
}

impl MetadataLocks {
    /// `row_locks` is the row lock manager of the same node, whose
    /// transaction IDs are the owners here.
    pub fn new(row_locks: Arc<LockManager>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State::default()),
            row_locks,
        })
    }

    /// Lock `name` for `owner`, waiting up to `timeout` for owners that
    /// hold it in a conflicting mode and for conflicting requests made
    /// earlier. A lock the owner already holds is kept, and strengthened if
    /// an exclusive lock is asked for. Held until `release_all`.
    pub async fn lock(
        &self,
        owner: u64,
        name: &TableName,
        mode: MdlMode,
        timeout: Duration,
    ) -> Result<(), MdlError> {
        self.acquire(owner, name, mode, timeout).await.map(|_| ())
    }

    /// Lock several names for `owner`, each in the mode given with it, all
    /// within `timeout`. The names are taken in sorted order whatever order
    /// they are given in, so two calls that want the same names cannot
    /// each end up holding what the other waits for. A name given more than
    /// once is locked in the strongest mode asked for.
    ///
    /// If one of them cannot be had, those this call took are given back
    /// (and an upgrade it made is undone) before the error is returned;
    /// locks the owner held before the call are untouched.
    pub async fn lock_all(
        &self,
        owner: u64,
        names: &[(TableName, MdlMode)],
        timeout: Duration,
    ) -> Result<(), MdlError> {
        let mut wanted: Vec<(&TableName, MdlMode)> =
            names.iter().map(|(name, mode)| (name, *mode)).collect();
        wanted.sort_unstable();
        // Sorted, so of two requests for one name the later is the stronger.
        wanted.dedup_by(|later, earlier| {
            let same = later.0 == earlier.0;
            if same {
                earlier.1 = later.1;
            }
            same
        });

        let deadline = Instant::now().checked_add(timeout);
        let mut taken: Vec<(&TableName, Taken)> = Vec::new();
        for (name, mode) in wanted {
            let left = deadline.map_or(timeout, |deadline| {
                deadline.saturating_duration_since(Instant::now())
            });
            match self.acquire(owner, name, mode, left).await {
                Ok(Taken::Nothing) => {}
                Ok(what) => taken.push((name, what)),
                Err(error) => {
                    self.give_back(owner, &taken);
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    /// Release everything `owner` holds, and abandon anything it is
    /// waiting for (that request then fails as timed out). Called when the
    /// transaction commits, rolls back, or its session ends. Other owners'
    /// locks and requests are left alone, except that those the release
    /// makes room for are granted.
    pub fn release_all(&self, owner: u64) {
        let mut state = self.state.lock();
        let mut changes = Changes::default();
        let mut names = state.held.remove(&owner).unwrap_or_default();
        for name in &names {
            if let Some(table) = state.tables.get_mut(name) {
                table.holders.retain(|(holder, _)| *holder != owner);
            }
        }
        for (name, table) in &mut state.tables {
            let waiting = table.waiters.len();
            table.waiters.retain(|waiter| waiter.owner != owner);
            if waiting != table.waiters.len() {
                changes.ended.push(owner);
                names.insert(name.clone());
            }
        }
        for name in &names {
            state.settle(name, &mut changes);
        }
        self.apply(changes);
    }

    /// Number of locks held, and of requests waiting.
    pub fn counts(&self) -> (usize, usize) {
        let state = self.state.lock();
        state
            .tables
            .values()
            .fold((0, 0), |(held, waiting), table| {
                (held + table.holders.len(), waiting + table.waiters.len())
            })
    }

    async fn acquire(
        &self,
        owner: u64,
        name: &TableName,
        mode: MdlMode,
        timeout: Duration,
    ) -> Result<Taken, MdlError> {
        // What granting the request changes for the owner. That cannot
        // alter while it waits: a transaction does one thing at a time.
        let taken;
        let (id, granted) = {
            let mut state = self.state.lock();
            let (held, blockers) = match state.tables.get(name) {
                Some(table) => (table.held_by(owner), table.blockers(owner, mode, None)),
                None => (None, Vec::new()),
            };
            if held.is_some_and(|held| held.covers(mode)) {
                return Ok(Taken::Nothing);
            }
            taken = match held {
                Some(_) => Taken::Upgraded,
                None => Taken::New,
            };
            if blockers.is_empty() {
                state.grant(owner, name, mode);
                // An upgrade can be granted past requests that are waiting:
                // they now wait for an exclusive holder.
                let mut changes = Changes::default();
                state.settle(name, &mut changes);
                self.apply(changes);
                return Ok(taken);
            }
            // The wait is recorded with the row locks before the request
            // joins the queue, and refused there if it would close a cycle.
            if !self.row_locks.begin_outside_wait(owner, &blockers) {
                return Err(MdlError::Deadlock);
            }
            let id = state.next_waiter;
            state.next_waiter += 1;
            let (tx, rx) = oneshot::channel();
            state
                .tables
                .entry(name.clone())
                .or_default()
                .waiters
                .push(Waiter {
                    id,
                    owner,
                    mode,
                    granted: tx,
                });
            (id, rx)
        };

        let mut guard = WaitGuard {
            locks: self,
            name,
            id,
            done: false,
        };
        match tokio::time::timeout(timeout, granted).await {
            Ok(Ok(())) => {
                guard.done = true;
                Ok(taken)
            }
            // The request was withdrawn by `release_all`: the transaction
            // ended while it waited.
            Ok(Err(_)) => {
                guard.done = true;
                Err(MdlError::Timeout)
            }
            Err(_) => {
                guard.done = true;
                // Granted at the very moment the time ran out: the lock is
                // held, and that is what the caller is told.
                let mut state = self.state.lock();
                if self.abandon(&mut state, name, id) {
                    Err(MdlError::Timeout)
                } else {
                    Ok(taken)
                }
            }
        }
    }

    /// Take a waiting request out of the queue, letting through those that
    /// waited for it alone. False if it is not waiting (any more).
    fn abandon(&self, state: &mut State, name: &TableName, id: u64) -> bool {
        let Some(table) = state.tables.get_mut(name) else {
            return false;
        };
        let Some(index) = table.waiters.iter().position(|w| w.id == id) else {
            return false;
        };
        let waiter = table.waiters.remove(index);
        let mut changes = Changes {
            ended: vec![waiter.owner],
            ..Changes::default()
        };
        state.settle(name, &mut changes);
        self.apply(changes);
        true
    }

    /// Undo what a `lock_all` that failed had done so far.
    fn give_back(&self, owner: u64, taken: &[(&TableName, Taken)]) {
        let mut state = self.state.lock();
        let mut changes = Changes::default();
        for &(name, what) in taken {
            match what {
                Taken::Nothing => continue,
                Taken::New => state.withdraw(owner, name, None),
                Taken::Upgraded => state.withdraw(owner, name, Some(MdlMode::Shared)),
            }
            state.settle(name, &mut changes);
        }
        self.apply(changes);
    }

    /// Finish a change to the table. Called with this table's mutex held:
    /// the row lock manager hears of the new waits before any other change
    /// here can be made, and before a granted requester can go on to wait
    /// for something else.
    fn apply(&self, changes: Changes) {
        self.row_locks
            .update_outside_waits(&changes.ended, changes.waits);
        for granted in changes.granted {
            // The requester may have given up (timed out, disconnected)
            // between being granted the lock and hearing of it. It then
            // releases what it holds as any transaction does when it ends.
            let _ = granted.send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::lock::{LockError, LockMode};
    use super::*;
    use MdlMode::{Exclusive, Shared};

    const LONG: Duration = Duration::from_secs(60);
    const SHORT: Duration = Duration::from_millis(20);

    fn locks() -> (Arc<LockManager>, Arc<MetadataLocks>) {
        let rows = LockManager::new();
        let mdl = MetadataLocks::new(rows.clone());
        (rows, mdl)
    }

    fn name(table: &str) -> TableName {
        TableName::new("db", table)
    }

    /// Wait until `n` requests are queued, so a test knows a task is
    /// waiting rather than guessing how long that takes.
    async fn until_waiting(m: &MetadataLocks, n: usize) {
        while m.counts().1 < n {
            tokio::task::yield_now().await;
        }
    }

    fn spawn_lock(
        m: &Arc<MetadataLocks>,
        owner: u64,
        table: &str,
        mode: MdlMode,
    ) -> tokio::task::JoinHandle<Result<(), MdlError>> {
        let m = m.clone();
        let name = name(table);
        tokio::spawn(async move { m.lock(owner, &name, mode, LONG).await })
    }

    #[tokio::test]
    async fn shared_locks_coexist_and_exclusive_waits_for_all_of_them() {
        let (_, m) = locks();
        m.lock(1, &name("t"), Shared, LONG).await.unwrap();
        m.lock(2, &name("t"), Shared, LONG).await.unwrap();
        assert_eq!(m.counts(), (2, 0));

        let ddl = spawn_lock(&m, 3, "t", Exclusive);
        until_waiting(&m, 1).await;
        m.release_all(1);
        assert_eq!(m.counts(), (1, 1), "granted while a reader still holds it");
        m.release_all(2);
        ddl.await.unwrap().unwrap();
        assert_eq!(m.counts(), (1, 0));
        m.release_all(3);
        assert_eq!(m.counts(), (0, 0));
    }

    #[tokio::test]
    async fn a_waiting_exclusive_request_holds_back_new_shared_ones() {
        let (_, m) = locks();
        m.lock(1, &name("t"), Shared, LONG).await.unwrap();
        let ddl = spawn_lock(&m, 2, "t", Exclusive);
        until_waiting(&m, 1).await;
        // Compatible with what is held, but not let past the schema change.
        let reader = spawn_lock(&m, 3, "t", Shared);
        until_waiting(&m, 2).await;
        // The holder itself is not held back by a request that waits for it.
        m.lock(1, &name("t"), Shared, LONG).await.unwrap();

        m.release_all(1);
        ddl.await.unwrap().unwrap();
        assert_eq!(m.counts(), (1, 1), "the reader waits for the schema change");
        m.release_all(2);
        reader.await.unwrap().unwrap();
        assert_eq!(m.counts(), (1, 0));
    }

    #[tokio::test]
    async fn an_owner_does_not_wait_for_itself() {
        let (_, m) = locks();
        m.lock(1, &name("t"), Shared, LONG).await.unwrap();
        m.lock(1, &name("t"), Shared, LONG).await.unwrap();
        m.lock(1, &name("t"), Exclusive, LONG).await.unwrap();
        m.lock(1, &name("t"), Exclusive, LONG).await.unwrap();
        m.lock(1, &name("t"), Shared, LONG).await.unwrap();
        assert_eq!(m.counts(), (1, 0));
        // Still exclusive: asking for less did not weaken it.
        assert_eq!(
            m.lock(2, &name("t"), Shared, SHORT).await,
            Err(MdlError::Timeout)
        );
        m.release_all(1);
        assert_eq!(m.counts(), (0, 0));
    }

    #[tokio::test]
    async fn an_upgrade_waits_only_for_the_other_holders() {
        let (_, m) = locks();
        m.lock(1, &name("t"), Shared, LONG).await.unwrap();
        m.lock(2, &name("t"), Shared, LONG).await.unwrap();
        // A schema change by a third owner is queued first.
        let ddl = spawn_lock(&m, 3, "t", Exclusive);
        until_waiting(&m, 1).await;
        // The upgrade does not wait for it, only for the other reader.
        let upgrade = spawn_lock(&m, 1, "t", Exclusive);
        until_waiting(&m, 2).await;
        m.release_all(2);
        upgrade.await.unwrap().unwrap();
        assert_eq!(m.counts(), (1, 1));
        m.release_all(1);
        ddl.await.unwrap().unwrap();
        assert_eq!(m.counts(), (1, 0));
    }

    #[tokio::test]
    async fn a_sole_holder_upgrades_at_once_past_waiting_requests() {
        let (_, m) = locks();
        m.lock(1, &name("t"), Shared, LONG).await.unwrap();
        let ddl = spawn_lock(&m, 2, "t", Exclusive);
        until_waiting(&m, 1).await;
        m.lock(1, &name("t"), Exclusive, LONG).await.unwrap();
        assert_eq!(m.counts(), (1, 1));
        m.release_all(1);
        ddl.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn two_holders_upgrading_deadlock() {
        let (_, m) = locks();
        m.lock(1, &name("t"), Shared, LONG).await.unwrap();
        m.lock(2, &name("t"), Shared, LONG).await.unwrap();
        let first = spawn_lock(&m, 1, "t", Exclusive);
        until_waiting(&m, 1).await;
        // The second to ask is the one that would close the cycle.
        assert_eq!(
            m.lock(2, &name("t"), Exclusive, LONG).await,
            Err(MdlError::Deadlock)
        );
        assert_eq!(m.counts(), (2, 1), "the refused request left nothing");
        // The victim rolls back; the other goes on.
        m.release_all(2);
        first.await.unwrap().unwrap();
        assert_eq!(m.counts(), (1, 0));
    }

    #[tokio::test]
    async fn a_deadlock_over_two_names_is_refused_to_the_one_who_would_close_it() {
        let (_, m) = locks();
        m.lock(1, &name("a"), Exclusive, LONG).await.unwrap();
        m.lock(2, &name("b"), Exclusive, LONG).await.unwrap();
        let first = spawn_lock(&m, 1, "b", Shared);
        until_waiting(&m, 1).await;
        assert_eq!(
            m.lock(2, &name("a"), Shared, LONG).await,
            Err(MdlError::Deadlock)
        );
        m.release_all(2);
        first.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn a_timed_out_request_leaves_the_table_as_it_was() {
        let (rows, m) = locks();
        m.lock(1, &name("t"), Shared, LONG).await.unwrap();
        assert_eq!(
            m.lock(2, &name("t"), Exclusive, SHORT).await,
            Err(MdlError::Timeout)
        );
        assert_eq!(m.counts(), (1, 0));
        // Nothing is queued any more to hold a reader back.
        m.lock(3, &name("t"), Shared, LONG).await.unwrap();
        assert_eq!(m.counts(), (2, 0));

        // Nor is owner 2 still recorded as waiting for owner 1: were it,
        // owner 1 waiting for a row of owner 2's would look like a deadlock.
        rows.lock_record(2, b"k", LockMode::Exclusive, LONG)
            .await
            .unwrap();
        assert_eq!(
            rows.lock_record(1, b"k", LockMode::Exclusive, SHORT).await,
            Err(LockError::Timeout)
        );
    }

    #[tokio::test]
    async fn a_request_abandoned_while_waiting_lets_others_through() {
        let (_, m) = locks();
        m.lock(1, &name("t"), Shared, LONG).await.unwrap();
        let ddl = spawn_lock(&m, 2, "t", Exclusive);
        until_waiting(&m, 1).await;
        let reader = spawn_lock(&m, 3, "t", Shared);
        until_waiting(&m, 2).await;
        // The schema change's session goes away mid-wait.
        ddl.abort();
        let _ = ddl.await;
        reader.await.unwrap().unwrap();
        assert_eq!(m.counts(), (2, 0));
    }

    #[tokio::test]
    async fn releasing_abandons_what_the_owner_waits_for() {
        let (_, m) = locks();
        m.lock(1, &name("t"), Shared, LONG).await.unwrap();
        m.lock(2, &name("u"), Shared, LONG).await.unwrap();
        let ddl = spawn_lock(&m, 2, "t", Exclusive);
        until_waiting(&m, 1).await;
        let reader = spawn_lock(&m, 3, "t", Shared);
        until_waiting(&m, 2).await;
        // Owner 2's transaction is ended from outside while it waits.
        m.release_all(2);
        assert_eq!(ddl.await.unwrap(), Err(MdlError::Timeout));
        reader.await.unwrap().unwrap();
        assert_eq!(m.counts(), (2, 0));
        // Releasing again, or for an owner that held nothing, is fine.
        m.release_all(2);
        m.release_all(99);
    }

    #[tokio::test]
    async fn several_names_are_locked_in_one_order_whatever_order_is_asked() {
        let (_, m) = locks();
        m.lock(9, &name("a"), Shared, LONG).await.unwrap();

        let first = {
            let m = m.clone();
            tokio::spawn(async move {
                let names = [(name("b"), Exclusive), (name("a"), Exclusive)];
                m.lock_all(1, &names, LONG).await
            })
        };
        until_waiting(&m, 1).await;
        // It waits for `a` holding nothing: it did not take `b` first.
        assert_eq!(m.counts(), (1, 1));
        let second = {
            let m = m.clone();
            tokio::spawn(async move {
                let names = [(name("a"), Exclusive), (name("b"), Exclusive)];
                m.lock_all(2, &names, LONG).await
            })
        };
        until_waiting(&m, 2).await;
        assert_eq!(m.counts(), (1, 2));

        // Each completes in turn; neither is refused as a deadlock.
        m.release_all(9);
        first.await.unwrap().unwrap();
        assert_eq!(m.counts(), (2, 1));
        m.release_all(1);
        second.await.unwrap().unwrap();
        assert_eq!(m.counts(), (2, 0));
    }

    #[tokio::test]
    async fn a_name_asked_for_twice_is_locked_in_the_stronger_mode() {
        let (_, m) = locks();
        let names = [
            (name("t"), Exclusive),
            (name("t"), Shared),
            (name("u"), Shared),
        ];
        m.lock_all(1, &names, LONG).await.unwrap();
        assert_eq!(m.counts(), (2, 0));
        assert_eq!(
            m.lock(2, &name("t"), Shared, SHORT).await,
            Err(MdlError::Timeout)
        );
        m.lock(2, &name("u"), Shared, LONG).await.unwrap();
    }

    #[tokio::test]
    async fn locking_several_names_gives_back_only_what_it_took_when_it_fails() {
        let (_, m) = locks();
        // Held by the owner before the call: `a` shared, `b` exclusive.
        m.lock(1, &name("a"), Shared, LONG).await.unwrap();
        m.lock(1, &name("b"), Exclusive, LONG).await.unwrap();
        // In the way of the call.
        m.lock(9, &name("d"), Shared, LONG).await.unwrap();

        let names = [
            (name("d"), Exclusive),
            (name("c"), Exclusive),
            (name("b"), Shared),
            (name("a"), Exclusive),
        ];
        assert_eq!(m.lock_all(1, &names, SHORT).await, Err(MdlError::Timeout));
        assert_eq!(m.counts(), (3, 0));

        // `c` was taken by the call and is free again.
        m.lock(2, &name("c"), Exclusive, LONG).await.unwrap();
        // `a` was upgraded by the call and is shared again, not released.
        m.lock(2, &name("a"), Shared, LONG).await.unwrap();
        assert_eq!(
            m.lock(3, &name("a"), Exclusive, SHORT).await,
            Err(MdlError::Timeout)
        );
        // `b` is held as it was.
        assert_eq!(
            m.lock(2, &name("b"), Shared, SHORT).await,
            Err(MdlError::Timeout)
        );
    }

    #[tokio::test]
    async fn giving_back_lets_waiters_through() {
        let (_, m) = locks();
        m.lock(9, &name("b"), Shared, LONG).await.unwrap();
        let call = {
            let m = m.clone();
            tokio::spawn(async move {
                let names = [(name("a"), Exclusive), (name("b"), Exclusive)];
                m.lock_all(1, &names, SHORT).await
            })
        };
        // Queued behind the `a` that the call holds while it waits for `b`.
        until_waiting(&m, 1).await;
        let reader = spawn_lock(&m, 2, "a", Shared);
        assert_eq!(call.await.unwrap(), Err(MdlError::Timeout));
        reader.await.unwrap().unwrap();
        assert_eq!(m.counts(), (2, 0));
    }

    #[tokio::test]
    async fn different_names_do_not_interfere() {
        let (_, m) = locks();
        m.lock(1, &name("t"), Exclusive, LONG).await.unwrap();
        m.lock(2, &name("u"), Exclusive, LONG).await.unwrap();
        // The same table name in another database is another lock.
        let elsewhere = TableName::new("other", "t");
        m.lock(3, &elsewhere, Exclusive, LONG).await.unwrap();
        // A name can be locked whether or not a table exists under it.
        m.lock(3, &TableName::new("other", "not yet"), Exclusive, LONG)
            .await
            .unwrap();
        assert_eq!(m.counts(), (4, 0));
        assert_eq!(
            m.lock(2, &name("t"), Shared, SHORT).await,
            Err(MdlError::Timeout)
        );
        m.release_all(3);
        assert_eq!(m.counts(), (2, 0));
    }

    /// Owner 1 holds a row and wants the table exclusive; owner 2 holds the
    /// table shared and wants the row. Owner 2 waits first, so it is owner
    /// 1, asking for the metadata lock, that would close the cycle.
    #[tokio::test]
    async fn a_cycle_through_a_row_lock_is_refused_to_the_metadata_request_that_closes_it() {
        let (rows, m) = locks();
        rows.lock_record(1, b"k", LockMode::Exclusive, LONG)
            .await
            .unwrap();
        m.lock(2, &name("t"), Shared, LONG).await.unwrap();

        let row = {
            let rows = rows.clone();
            tokio::spawn(async move { rows.lock_record(2, b"k", LockMode::Exclusive, LONG).await })
        };
        while rows.counts().2 < 1 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            m.lock(1, &name("t"), Exclusive, LONG).await,
            Err(MdlError::Deadlock)
        );
        assert_eq!(m.counts(), (1, 0));
        // The victim rolls back; the other goes on.
        rows.release_all(1);
        m.release_all(1);
        row.await.unwrap().unwrap();
    }

    /// The same cycle, met from the other side: owner 1 waits for the
    /// metadata lock first, so it is owner 2, asking for the row, that
    /// would close the cycle, and the row lock manager that refuses.
    #[tokio::test]
    async fn a_cycle_through_a_metadata_lock_is_refused_to_the_row_request_that_closes_it() {
        let (rows, m) = locks();
        rows.lock_record(1, b"k", LockMode::Exclusive, LONG)
            .await
            .unwrap();
        m.lock(2, &name("t"), Shared, LONG).await.unwrap();

        let ddl = spawn_lock(&m, 1, "t", Exclusive);
        until_waiting(&m, 1).await;
        assert_eq!(
            rows.lock_record(2, b"k", LockMode::Exclusive, LONG).await,
            Err(LockError::Deadlock)
        );
        rows.release_all(2);
        m.release_all(2);
        ddl.await.unwrap().unwrap();
        assert_eq!(m.counts(), (1, 0));
    }

    /// A wait changes whom it is for as locks are granted. Owner 3's shared
    /// request waits for owner 2's schema change, queued before it; once
    /// that is granted it waits for owner 2 as the holder, and a row wait
    /// by owner 2 on owner 3 is still seen as the deadlock it is.
    #[tokio::test]
    async fn waits_are_followed_as_they_change_hands() {
        let (rows, m) = locks();
        rows.lock_record(3, b"k", LockMode::Exclusive, LONG)
            .await
            .unwrap();
        m.lock(1, &name("t"), Shared, LONG).await.unwrap();
        let ddl = spawn_lock(&m, 2, "t", Exclusive);
        until_waiting(&m, 1).await;
        let reader = spawn_lock(&m, 3, "t", Shared);
        until_waiting(&m, 2).await;
        m.release_all(1);
        ddl.await.unwrap().unwrap();
        assert_eq!(
            rows.lock_record(2, b"k", LockMode::Exclusive, LONG).await,
            Err(LockError::Deadlock)
        );
        // And once owner 1 is out of it, it is no part of any cycle.
        rows.lock_record(1, b"j", LockMode::Exclusive, LONG)
            .await
            .unwrap();
        rows.release_all(2);
        m.release_all(2);
        reader.await.unwrap().unwrap();
    }
}
