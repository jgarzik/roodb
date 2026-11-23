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
//! # The fast side
//!
//! Nearly every lock ever taken is a shared one on a table nobody is
//! changing, and every statement takes one. Those do not go through the
//! lock table and its mutex. Each name has a **gate** (`NameGate`): one
//! word holding a count of shared holders and a closed bit. While the gate
//! is open, a shared lock is that count going up by one and nothing else;
//! who holds it is known only to the holder's own session (`MdlSession`).
//!
//! A request for an exclusive lock closes the gate. From then on shared
//! requests for the name come to the lock table like any other and queue
//! behind it. The holders the count stood for are then found (every
//! session is asked what it holds on that gate) and entered in the lock
//! table under their own names, so that from the first moment a request
//! waits, the table says exactly whom it waits for, and deadlocks are
//! found as before. The gate opens again when the table has nothing left
//! on the name.
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
//!
//! Under this table's mutex, closing a gate takes the list of sessions and
//! then each session's own small mutex in turn. A session holds its mutex
//! only to change what it holds on the fast side, and takes nothing else
//! while it does: it never comes to this table with its mutex held.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use parking_lot::{Mutex, RwLock};
use tokio::sync::oneshot;
use tokio::time::Instant;

use super::lock::LockManager;

/// The name a metadata lock is taken on: a table's name as the catalog has
/// it. Names are compared exactly as given: the caller settles letter case
/// before locking.
pub use crate::catalog::TableName;

/// How long a request waits for a metadata lock unless its session says
/// otherwise: MySQL's default for `lock_wait_timeout`, a year.
pub const DEFAULT_METADATA_LOCK_WAIT_SECS: u64 = 31_536_000;

/// The closed bit of a gate's word; the rest of the word counts the shared
/// holders on the fast side.
const CLOSED: u64 = 1 << 63;

/// The fast side of the locks on one name: a count of shared holders that
/// have not been through the lock table, and whether new ones may still
/// join them.
#[derive(Debug, Default)]
pub struct NameGate {
    word: AtomicU64,
}

impl NameGate {
    /// Count one more shared holder, unless the gate is closed.
    fn enter(&self) -> bool {
        let mut word = self.word.load(Ordering::Relaxed);
        loop {
            if word & CLOSED != 0 {
                return false;
            }
            match self.word.compare_exchange_weak(
                word,
                word + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(now) => word = now,
            }
        }
    }

    /// Count one shared holder fewer.
    fn leave(&self) {
        self.word.fetch_sub(1, Ordering::AcqRel);
    }

    fn close(&self) {
        self.word.fetch_or(CLOSED, Ordering::AcqRel);
    }

    fn open(&self) {
        self.word.fetch_and(!CLOSED, Ordering::AcqRel);
    }

    /// The number of shared holders on the fast side.
    fn holders(&self) -> u64 {
        self.word.load(Ordering::Acquire) & !CLOSED
    }
}

/// A name to lock, with its gate: what a statement keeps for each table it
/// uses, so that locking the table again needs no lookup.
#[derive(Debug, Clone)]
pub struct MdlTarget {
    pub name: TableName,
    gate: Arc<NameGate>,
}

#[derive(Default)]
struct SessionLocks {
    /// The names this session has locked, with their gates: kept so that
    /// locking a table again touches nothing the sessions share but the
    /// gate's word itself.
    known: Vec<MdlTarget>,
    /// Shared locks held on the fast side, known nowhere else: the owner,
    /// and the name's place in `known`.
    fast: Vec<(u64, usize)>,
    /// Owners of this session that hold (or wait for) something in the
    /// lock table: locks taken there, and fast ones moved there when a
    /// gate was closed.
    in_table: Vec<u64>,
}

/// How many names a session remembers the gates of before it starts over
/// (when it holds none of them).
const KNOWN_NAMES: usize = 256;

/// What one session holds, where a schema change that closes a gate can
/// look at it. It is only a list: letting go of it runs nothing, so it
/// does not matter who lets go of it last, or under which lock.
#[derive(Default)]
struct SessionState {
    held: Mutex<SessionLocks>,
}

/// The locks of one session. A session takes its shared locks through
/// this, on the fast side whenever the name's gate is open. When it is
/// dropped, everything the session held is released.
pub struct MdlSession {
    locks: Arc<MetadataLocks>,
    state: Arc<SessionState>,
}

impl std::fmt::Debug for MdlSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let held = self.state.held.lock();
        f.debug_struct("MdlSession")
            .field("fast", &held.fast.len())
            .field("in_table", &held.in_table)
            .finish()
    }
}

impl MdlSession {
    /// Lock `target` shared for `owner` (a transaction of this session),
    /// waiting up to `timeout` if a schema change holds the name or waits
    /// for it. Held until `release(owner)`.
    ///
    /// A name the session already holds for another of its owners is
    /// given at once, even with a schema change waiting for it: the
    /// session is still using the table, and a statement it runs within
    /// another (or runs again) must not be made to wait for a schema
    /// change that is waiting for the session.
    pub async fn lock_shared(
        &self,
        owner: u64,
        name: &TableName,
        timeout: Duration,
    ) -> Result<(), MdlError> {
        let beside: Vec<u64>;
        {
            let mut held = self.state.held.lock();
            let slot = match held.known.iter().position(|known| known.name == *name) {
                Some(slot) => slot,
                None => {
                    if held.fast.is_empty() && held.known.len() >= KNOWN_NAMES {
                        held.known.clear();
                    }
                    held.known.push(self.locks.target(name));
                    held.known.len() - 1
                }
            };
            if held.fast.contains(&(owner, slot)) {
                return Ok(());
            }
            // Counted and noted under the session's mutex, so that whoever
            // closes the gate and then asks this session what it holds
            // sees either both or neither.
            if held.known[slot].gate.enter() {
                held.fast.push((owner, slot));
                return Ok(());
            }
            // The session's other owners that have locks in the table: if
            // one of them holds this name, this owner is given it beside
            // them, without waiting its turn.
            beside = held.in_table.clone();
            if !held.in_table.contains(&owner) {
                held.in_table.push(owner);
            }
        }
        self.locks
            .lock_shared_beside(owner, name, &beside, timeout)
            .await
    }

    /// Lock several names for `owner`, each in the mode given with it, as
    /// `MetadataLocks::lock_all` does. For the exclusive locks of a schema
    /// change, and the shared ones it takes with them.
    pub async fn lock_all(
        &self,
        owner: u64,
        names: &[(TableName, MdlMode)],
        timeout: Duration,
    ) -> Result<(), MdlError> {
        {
            let mut held = self.state.held.lock();
            if !held.in_table.contains(&owner) {
                held.in_table.push(owner);
            }
        }
        self.locks.lock_all(owner, names, timeout).await
    }

    /// Release everything `owner` holds, and abandon anything it waits
    /// for. Called when the transaction (or the statement, for one that
    /// runs in none) ends.
    pub fn release(&self, owner: u64) {
        self.release_where(|held| held == owner);
    }

    /// Release everything this session holds for every owner but `keep`:
    /// at the end of a statement, whatever was locked for the statement
    /// alone goes, and what the open transaction holds stays.
    pub fn release_all_but(&self, keep: Option<u64>) {
        self.release_where(|held| Some(held) != keep);
    }

    /// Release what the owners that `goes` picks hold.
    fn release_where(&self, goes: impl Fn(u64) -> bool) {
        let in_table: Vec<u64> = {
            let mut held = self.state.held.lock();
            let SessionLocks {
                known,
                fast,
                in_table,
            } = &mut *held;
            fast.retain(|&(owner, slot)| {
                if goes(owner) {
                    known[slot].gate.leave();
                }
                !goes(owner)
            });
            // Nearly always none: nothing is allocated then.
            if in_table.iter().any(|owner| goes(*owner)) {
                let gone = in_table.iter().copied().filter(|o| goes(*o)).collect();
                in_table.retain(|owner| !goes(*owner));
                gone
            } else {
                Vec::new()
            }
        };
        for owner in in_table {
            self.locks.release_all(owner);
        }
    }

    /// Whether this session holds nothing, for any owner.
    pub fn is_idle(&self) -> bool {
        let held = self.state.held.lock();
        held.fast.is_empty() && held.in_table.is_empty()
    }
}

impl Drop for MdlSession {
    fn drop(&mut self) {
        // A session that goes away holds nothing afterwards. (Only the
        // session's owner drops this, never whoever looks through the
        // sessions: they hold the list, not the session.)
        let held = std::mem::take(&mut *self.state.held.lock());
        for (_, slot) in held.fast {
            held.known[slot].gate.leave();
        }
        for owner in held.in_table {
            self.locks.release_all(owner);
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
    /// The name's gate, while this entry keeps it closed: from the first
    /// request for an exclusive lock until the entry goes.
    closed: Option<Arc<NameGate>>,
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
        self.forget_if_idle(name);
    }

    /// Drop the entry for `name` if nothing is held or waited for on it,
    /// and open the name's gate again if the entry kept it closed.
    fn forget_if_idle(&mut self, name: &TableName) {
        let idle = self
            .tables
            .get(name)
            .is_some_and(|table| table.holders.is_empty() && table.waiters.is_empty());
        if idle {
            if let Some(gate) = self.tables.remove(name).and_then(|table| table.closed) {
                gate.open();
            }
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
    /// Each name's gate. Read to find a gate, written to add one.
    gates: RwLock<HashMap<TableName, Arc<NameGate>>>,
    /// The sessions that may hold locks on the fast side.
    sessions: Mutex<Vec<Weak<SessionState>>>,
    /// How long sessions that have not chosen otherwise wait, in seconds.
    default_wait_secs: AtomicU64,
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
            gates: RwLock::new(HashMap::new()),
            sessions: Mutex::new(Vec::new()),
            default_wait_secs: AtomicU64::new(DEFAULT_METADATA_LOCK_WAIT_SECS),
            row_locks,
        })
    }

    /// How long sessions that have not chosen otherwise wait for a lock.
    pub fn default_wait(&self) -> Duration {
        Duration::from_secs(self.default_wait_secs.load(Ordering::Relaxed))
    }

    /// Change the wait for sessions that start from now on.
    pub fn set_default_wait(&self, wait: Duration) {
        self.default_wait_secs
            .store(wait.as_secs(), Ordering::Relaxed);
    }

    /// The locks of a new session.
    pub fn session(self: &Arc<Self>) -> Arc<MdlSession> {
        let state = Arc::new(SessionState::default());
        {
            let mut sessions = self.sessions.lock();
            // Sessions that have gone are dropped from the list as it
            // grows.
            if sessions.len() == sessions.capacity() {
                sessions.retain(|session| session.strong_count() > 0);
            }
            sessions.push(Arc::downgrade(&state));
        }
        Arc::new(MdlSession {
            locks: self.clone(),
            state,
        })
    }

    /// `name` with its gate, to lock it through a session.
    pub fn target(&self, name: &TableName) -> MdlTarget {
        if let Some(gate) = self.gates.read().get(name) {
            return MdlTarget {
                name: name.clone(),
                gate: gate.clone(),
            };
        }
        let gate = self.gates.write().entry(name.clone()).or_default().clone();
        MdlTarget {
            name: name.clone(),
            gate,
        }
    }

    /// The number of names that have a gate.
    pub fn gates(&self) -> usize {
        self.gates.read().len()
    }

    /// Before an exclusive lock on `name` is asked for: close the name's
    /// gate, so that no shared lock is taken on the fast side from here
    /// on, and enter those that were in the table under their owners. The
    /// entry for the name keeps the gate closed until it goes.
    fn close_gate(&self, state: &mut State, name: &TableName) {
        if state
            .tables
            .get(name)
            .is_some_and(|table| table.closed.is_some())
        {
            return;
        }
        let gate = self.target(name).gate;
        gate.close();
        // Whoever counted itself in before the gate closed did so under
        // its session's mutex, and noted the lock there: asking each
        // session in turn finds them all.
        let sessions: Vec<Arc<SessionState>> = {
            let mut sessions = self.sessions.lock();
            sessions.retain(|session| session.strong_count() > 0);
            sessions.iter().filter_map(Weak::upgrade).collect()
        };
        for session in sessions {
            let mut held = session.held.lock();
            let Some(slot) = held
                .known
                .iter()
                .position(|known| Arc::ptr_eq(&known.gate, &gate))
            else {
                continue;
            };
            let mut index = 0;
            while index < held.fast.len() {
                let (owner, at) = held.fast[index];
                if at != slot {
                    index += 1;
                    continue;
                }
                held.fast.swap_remove(index);
                gate.leave();
                state.grant(owner, name, MdlMode::Shared);
                if !held.in_table.contains(&owner) {
                    held.in_table.push(owner);
                }
            }
        }
        state.tables.entry(name.clone()).or_default().closed = Some(gate);
    }

    /// Drop the gates nobody uses: no holder, no entry in the table, and
    /// no statement keeping the gate to lock the name again.
    fn drop_idle_gates(&self, state: &State) {
        let mut gates = self.gates.write();
        gates.retain(|name, gate| {
            Arc::strong_count(gate) > 1 || gate.holders() > 0 || state.tables.contains_key(name)
        });
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

    /// Lock `name` shared for `owner`, one of several owners of one
    /// session, `beside` being the others: if one of them holds the name,
    /// `owner` is given it too without waiting behind requests made since.
    /// Otherwise as `lock`.
    async fn lock_shared_beside(
        &self,
        owner: u64,
        name: &TableName,
        beside: &[u64],
        timeout: Duration,
    ) -> Result<(), MdlError> {
        {
            let mut state = self.state.lock();
            let beside_one = state.tables.get(name).is_some_and(|table| {
                table.held_by(owner).is_none()
                    && table
                        .holders
                        .iter()
                        .any(|(holder, _)| *holder != owner && beside.contains(holder))
            });
            if beside_one {
                state.grant(owner, name, MdlMode::Shared);
                // Those who wait for the name now wait for this owner too.
                let mut changes = Changes::default();
                state.settle(name, &mut changes);
                self.apply(changes);
                return Ok(());
            }
        }
        self.lock(owner, name, MdlMode::Shared, timeout).await
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
        // Rarely anything: only names a schema change touched come here.
        if !names.is_empty() {
            self.drop_idle_gates(&state);
        }
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
            if mode == MdlMode::Exclusive {
                self.close_gate(&mut state, name);
            }
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
                // Nothing was taken or queued: if the name's gate was
                // closed for this request alone, it opens again.
                state.forget_if_idle(name);
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

    // ---- The fast side ----

    fn target(m: &MetadataLocks, table: &str) -> MdlTarget {
        m.target(&name(table))
    }

    /// Shared locks taken through a session, with nothing else going on,
    /// never reach the lock table; an exclusive request finds them all the
    /// same, waits for each, and is granted when the last has gone.
    #[tokio::test]
    async fn shared_locks_on_the_fast_side_are_found_by_an_exclusive_request() {
        let (_, m) = locks();
        let (a, b) = (m.session(), m.session());
        let t = target(&m, "t");
        a.lock_shared(1, &t.name, LONG).await.unwrap();
        b.lock_shared(2, &t.name, LONG).await.unwrap();
        // The same owner again: nothing more is counted.
        a.lock_shared(1, &t.name, LONG).await.unwrap();
        assert_eq!(m.counts(), (0, 0));
        assert_eq!(t.gate.holders(), 2);

        let writer = spawn_lock(&m, 3, "t", Exclusive);
        until_waiting(&m, 1).await;
        // Both holders are in the table now, under their own names.
        assert_eq!(m.counts(), (2, 1));
        assert_eq!(t.gate.holders(), 0);

        a.release(1);
        assert!(!writer.is_finished());
        b.release(2);
        writer.await.unwrap().unwrap();
        assert_eq!(m.counts(), (1, 0));
        assert!(a.is_idle() && b.is_idle());
    }

    /// While an exclusive lock is held or waited for, shared requests go
    /// through the table and wait their turn; once the name is free again,
    /// they are back on the fast side.
    #[tokio::test]
    async fn the_gate_closes_for_a_schema_change_and_opens_after_it() {
        let (_, m) = locks();
        let a = m.session();
        let t = target(&m, "t");
        m.lock(9, &name("t"), Exclusive, LONG).await.unwrap();

        let reader = {
            let (a, t) = (a.clone(), t.clone());
            tokio::spawn(async move { a.lock_shared(1, &t.name, LONG).await })
        };
        until_waiting(&m, 1).await;
        assert_eq!(t.gate.holders(), 0);
        m.release_all(9);
        reader.await.unwrap().unwrap();
        // Granted by the table, where it stays until it is released.
        assert_eq!(m.counts(), (1, 0));
        a.release(1);
        assert_eq!(m.counts(), (0, 0));

        // Nothing is left on the name: the next shared lock is fast again.
        a.lock_shared(2, &t.name, LONG).await.unwrap();
        assert_eq!(m.counts(), (0, 0));
        assert_eq!(t.gate.holders(), 1);
        a.release(2);
        assert_eq!(t.gate.holders(), 0);
    }

    /// A request that times out, or is refused, leaves the gate as it
    /// found it once nothing else is on the name.
    #[tokio::test]
    async fn a_failed_exclusive_request_does_not_leave_the_gate_closed() {
        let (_, m) = locks();
        let a = m.session();
        let t = target(&m, "t");
        a.lock_shared(1, &t.name, LONG).await.unwrap();
        assert_eq!(
            m.lock(2, &name("t"), Exclusive, SHORT).await,
            Err(MdlError::Timeout)
        );
        // The holder was moved to the table and is still there.
        assert_eq!(m.counts(), (1, 0));
        a.release(1);
        assert_eq!(m.counts(), (0, 0));
        a.lock_shared(3, &t.name, LONG).await.unwrap();
        assert_eq!(t.gate.holders(), 1);
        a.release(3);
    }

    /// A session that holds a name on the fast side and then asks for it
    /// exclusively is upgrading: it waits for the other holders only.
    #[tokio::test]
    async fn a_fast_holder_upgrades() {
        let (_, m) = locks();
        let (a, b) = (m.session(), m.session());
        let t = target(&m, "t");
        a.lock_shared(1, &t.name, LONG).await.unwrap();
        b.lock_shared(2, &t.name, LONG).await.unwrap();

        let upgrade = {
            let a = a.clone();
            tokio::spawn(async move { a.lock_all(1, &[(name("t"), Exclusive)], LONG).await })
        };
        until_waiting(&m, 1).await;
        b.release(2);
        upgrade.await.unwrap().unwrap();
        assert_eq!(m.counts(), (1, 0));
        a.release(1);
        assert_eq!(m.counts(), (0, 0));
        assert!(a.is_idle());
    }

    /// A cycle through a row lock and a metadata lock held on the fast
    /// side is found: the holder is named in the table before anyone
    /// waits for it.
    #[tokio::test]
    async fn a_cycle_through_a_fast_holder_is_found() {
        let (rows, m) = locks();
        let a = m.session();
        let t = target(&m, "t");
        // Owner 1: the table, on the fast side. Owner 2: the row.
        a.lock_shared(1, &t.name, LONG).await.unwrap();
        rows.lock_record(2, b"k", LockMode::Exclusive, LONG)
            .await
            .unwrap();
        // Owner 2 waits for the table...
        let writer = spawn_lock(&m, 2, "t", Exclusive);
        until_waiting(&m, 1).await;
        // ...and owner 1, asking for the row, would close the cycle.
        assert_eq!(
            rows.lock_record(1, b"k", LockMode::Exclusive, LONG).await,
            Err(LockError::Deadlock)
        );
        // The victim rolls back; the other goes on.
        rows.release_all(1);
        a.release(1);
        writer.await.unwrap().unwrap();
        m.release_all(2);
        rows.release_all(2);
    }

    /// A session that goes away holds nothing afterwards, on either side.
    #[tokio::test]
    async fn a_dropped_session_releases_what_it_held() {
        let (_, m) = locks();
        let t = target(&m, "t");
        let u = target(&m, "u");
        let a = m.session();
        a.lock_shared(1, &t.name, LONG).await.unwrap();
        a.lock_all(1, &[(name("u"), Exclusive)], LONG)
            .await
            .unwrap();
        assert_eq!(t.gate.holders(), 1);
        drop(a);
        assert_eq!(t.gate.holders(), 0);
        assert_eq!(m.counts(), (0, 0));
        // Both names can be had at once by another.
        m.lock_all(2, &[(name("t"), Exclusive), (name("u"), Exclusive)], LONG)
            .await
            .unwrap();
        m.release_all(2);
        drop((t, u));
    }

    /// A name's gate is kept only while something uses it.
    #[tokio::test]
    async fn gates_do_not_pile_up() {
        let (_, m) = locks();
        let a = m.session();
        for n in 0..50 {
            let table = format!("t{n}");
            // Created and dropped: an exclusive lock on a new name.
            m.lock(1, &name(&table), Exclusive, LONG).await.unwrap();
            m.release_all(1);
            // Read once.
            let t = target(&m, &table);
            a.lock_shared(2, &t.name, LONG).await.unwrap();
            a.release(2);
        }
        // A session keeps the gates of the names it has used (a bounded
        // number), to lock them again cheaply. Once it has gone, they are
        // dropped when a schema change next releases its locks.
        drop(a);
        m.lock(1, &name("last"), Exclusive, LONG).await.unwrap();
        m.release_all(1);
        assert_eq!(m.gates(), 0);
    }

    /// Readers and writers from several threads: a writer never holds a
    /// name while a reader does, nor two writers at once, and everyone
    /// gets through.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn readers_and_writers_exclude_each_other() {
        use std::sync::atomic::AtomicI64;

        let (_, m) = locks();
        // Above zero: that many readers inside. -1: a writer.
        let inside = Arc::new(AtomicI64::new(0));
        let mut tasks = Vec::new();
        for reader in 0..6u64 {
            let (m, inside) = (m.clone(), inside.clone());
            tasks.push(tokio::spawn(async move {
                let session = m.session();
                for round in 0..300u64 {
                    let owner = 1000 + reader * 1000 + round;
                    let t = m.target(&name("t"));
                    session.lock_shared(owner, &t.name, LONG).await.unwrap();
                    assert!(
                        inside.fetch_add(1, Ordering::SeqCst) >= 0,
                        "a writer is inside"
                    );
                    tokio::task::yield_now().await;
                    assert!(inside.fetch_sub(1, Ordering::SeqCst) > 0);
                    session.release(owner);
                }
            }));
        }
        for writer in 0..2u64 {
            let (m, inside) = (m.clone(), inside.clone());
            tasks.push(tokio::spawn(async move {
                let session = m.session();
                for round in 0..60u64 {
                    let owner = 100_000 + writer * 1000 + round;
                    session
                        .lock_all(owner, &[(name("t"), Exclusive)], LONG)
                        .await
                        .unwrap();
                    assert_eq!(
                        inside.compare_exchange(0, -1, Ordering::SeqCst, Ordering::SeqCst),
                        Ok(0),
                        "someone else is inside"
                    );
                    tokio::task::yield_now().await;
                    inside.store(0, Ordering::SeqCst);
                    session.release(owner);
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(m.counts(), (0, 0));
        assert_eq!(m.target(&name("t")).gate.holders(), 0);
    }

    /// Sessions that go away, holding locks, while schema changes close
    /// gates and look through the sessions: nobody is left waiting, and
    /// nothing is left held. (Whoever looks through the sessions holds
    /// their lists, never the sessions: a session's release is run by its
    /// owner alone, with no lock held.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sessions_can_go_away_while_gates_are_closed() {
        let (_, m) = locks();
        let mut tasks = Vec::new();
        for reader in 0..4u64 {
            let m = m.clone();
            tasks.push(tokio::spawn(async move {
                for round in 0..2000u64 {
                    let owner = 1000 + reader * 10_000 + round;
                    let session = m.session();
                    let t = m.target(&name("t"));
                    // Whether fast or through the table: dropped holding it.
                    session.lock_shared(owner, &t.name, SHORT).await.ok();
                    // Long enough alive to be found by someone looking
                    // through the sessions, then gone under their hands.
                    tokio::task::yield_now().await;
                    drop(session);
                }
            }));
        }
        for writer in 0..2u64 {
            let m = m.clone();
            tasks.push(tokio::spawn(async move {
                for round in 0..2000u64 {
                    let owner = 500_000 + writer * 10_000 + round;
                    m.lock(owner, &name("t"), Exclusive, SHORT).await.ok();
                    m.release_all(owner);
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(m.counts(), (0, 0));
        assert_eq!(m.target(&name("t")).gate.holders(), 0);
    }

    /// A session that holds a name for one of its owners is given it for
    /// another at once, though a schema change is waiting for the name:
    /// the schema change then waits for both.
    #[tokio::test]
    async fn a_session_is_given_again_what_it_holds_for_another_owner() {
        let (_, m) = locks();
        let (a, b) = (m.session(), m.session());
        let t = target(&m, "t");
        a.lock_shared(1, &t.name, LONG).await.unwrap();
        let writer = spawn_lock(&m, 9, "t", Exclusive);
        until_waiting(&m, 1).await;

        // Another session waits its turn behind the schema change...
        let other = {
            let (b, t) = (b.clone(), t.clone());
            tokio::spawn(async move { b.lock_shared(3, &t.name, LONG).await })
        };
        until_waiting(&m, 2).await;
        // ...the session that holds the name does not.
        a.lock_shared(2, &t.name, LONG).await.unwrap();
        assert_eq!(m.counts(), (2, 2));

        // The schema change waits until both owners have let go.
        a.release(1);
        assert!(!writer.is_finished());
        a.release(2);
        writer.await.unwrap().unwrap();
        m.release_all(9);
        other.await.unwrap().unwrap();
        b.release(3);
        assert_eq!(m.counts(), (0, 0));
    }
}
