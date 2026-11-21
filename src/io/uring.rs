//! The io_uring backend.
//!
//! A reactor is a thread that owns one ring. Tasks hand it requests over a
//! channel; the reactor turns everything that has arrived into submission
//! entries and enters the kernel once for the lot, so the cost of the system
//! call is shared by all requests queued at that moment. Completions are
//! delivered straight to the waiting tasks.
//!
//! Each open file is pinned to one reactor and registered in that ring's fixed
//! file table, which spares the kernel a descriptor lookup per request.
//!
//! Requests carry an urgency. More urgent requests are submitted first, and
//! background work (flush, compaction) is capped so it cannot fill the ring.

use std::collections::VecDeque;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::Arc;
use std::thread::JoinHandle;

use io_uring::{opcode, squeue, types, IoUring};
use tokio::sync::oneshot;

use super::engine::{Counters, Op, OpKind, Urgency};
use super::error::IoResult;

/// Submission queue size of each ring.
const RING_ENTRIES: u32 = 1024;
/// Completion queue size of each ring; also the cap on requests in the kernel.
const MAX_INFLIGHT: usize = 4096;
/// Background requests allowed in the kernel at once, per reactor. Kept low:
/// each can be a megabyte or more, and once requests are in the device's
/// queue the order they were submitted in no longer protects anyone.
const MAX_BACKGROUND: usize = 16;
/// Size of each ring's fixed file table.
const FIXED_FILES: u32 = 4096;
/// Largest single transfer handed to the kernel; longer requests continue
/// where the previous transfer stopped.
const MAX_TRANSFER: usize = 1 << 30;
/// Upper bound on reactors chosen automatically.
const MAX_AUTO_REACTORS: usize = 4;

/// `user_data` of the read that watches the wake-up eventfd.
const WAKE: u64 = u64::MAX;
/// Low bit of `user_data`: 0 = the transfer itself, 1 = its attached sync.
const TAG_SYNC: u64 = 1;

enum Msg {
    Op(Op),
    Register {
        fd: RawFd,
        reply: oneshot::Sender<Option<u32>>,
    },
    Unregister(u32),
    Shutdown,
}

/// The part of a reactor other threads can touch.
pub(crate) struct ReactorShared {
    tx: Sender<Msg>,
    eventfd: OwnedFd,
    /// True while the reactor is, or is about to be, blocked in the kernel.
    sleeping: AtomicBool,
}

impl ReactorShared {
    fn send(&self, msg: Msg) -> bool {
        if self.tx.send(msg).is_err() {
            return false;
        }
        // The reactor sets `sleeping` and then looks at the channel once more
        // before blocking; we queue and then look at `sleeping`. One of the two
        // always sees the other, so a request is never left unnoticed.
        if self.sleeping.swap(false, Ordering::SeqCst) {
            let one: u64 = 1;
            // SAFETY: writes 8 bytes from a live local to a descriptor we own.
            // A failed write can only mean the counter is already non-zero,
            // which wakes the reactor just the same.
            unsafe {
                libc::write(
                    self.eventfd.as_raw_fd(),
                    (&one as *const u64).cast(),
                    std::mem::size_of::<u64>(),
                );
            }
        }
        true
    }
}

/// How one file reaches its reactor.
pub(crate) struct FileRoute {
    shared: Arc<ReactorShared>,
    fixed: Option<u32>,
}

impl FileRoute {
    /// Queue a request. Returns false if the reactor has shut down.
    pub(crate) fn submit(&self, op: Op) -> bool {
        self.shared.send(Msg::Op(op))
    }
}

impl Drop for FileRoute {
    fn drop(&mut self) {
        // Runs when the last request on the file has finished, so the slot is
        // idle by now.
        if let Some(slot) = self.fixed {
            self.shared.send(Msg::Unregister(slot));
        }
    }
}

/// The set of reactors.
pub(crate) struct Uring {
    reactors: Vec<Arc<ReactorShared>>,
    threads: Vec<JoinHandle<()>>,
    next: AtomicUsize,
}

impl Uring {
    /// Start `reactors` threads (0 = a quarter of the cores, capped). Fails if
    /// the kernel will not create a ring.
    pub(crate) fn start(reactors: usize, counters: Arc<Counters>) -> IoResult<Self> {
        let count = if reactors == 0 {
            let cores = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4);
            (cores / 4).clamp(1, MAX_AUTO_REACTORS)
        } else {
            reactors
        };
        let mut this = Self {
            reactors: Vec::with_capacity(count),
            threads: Vec::with_capacity(count),
            next: AtomicUsize::new(0),
        };
        for i in 0..count {
            // SAFETY: eventfd() returns a fresh descriptor or -1.
            let raw = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC) };
            if raw < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            // SAFETY: `raw` is a valid descriptor that nothing else owns.
            let eventfd = unsafe { OwnedFd::from_raw_fd(raw) };
            let (tx, rx) = mpsc::channel();
            let shared = Arc::new(ReactorShared {
                tx,
                eventfd,
                sleeping: AtomicBool::new(false),
            });
            let (ready_tx, ready_rx) = mpsc::channel();
            let thread_shared = shared.clone();
            let thread_counters = counters.clone();
            let thread = std::thread::Builder::new()
                .name(format!("roodb-uring-{i}"))
                .spawn(move || {
                    // The ring is created on the thread that will use it: the
                    // kernel ties a single-issuer ring to its creating thread.
                    match Reactor::new(rx, thread_shared, thread_counters) {
                        Ok(mut reactor) => {
                            let _ = ready_tx.send(Ok(()));
                            reactor.run();
                        }
                        Err(e) => {
                            let _ = ready_tx.send(Err(e));
                        }
                    }
                })?;
            this.reactors.push(shared);
            this.threads.push(thread);
            match ready_rx.recv() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(e.into()),
                Err(_) => {
                    return Err(std::io::Error::other("io_uring reactor failed to start").into())
                }
            }
        }
        Ok(this)
    }

    /// Pin a newly opened file to a reactor and register it there.
    pub(crate) async fn route_for(&self, file: &std::fs::File) -> FileRoute {
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.reactors.len();
        let shared = self.reactors[index].clone();
        let (reply, wait) = oneshot::channel();
        let fixed = if shared.send(Msg::Register {
            fd: file.as_raw_fd(),
            reply,
        }) {
            wait.await.ok().flatten()
        } else {
            None
        };
        FileRoute { shared, fixed }
    }
}

impl Drop for Uring {
    fn drop(&mut self) {
        for shared in &self.reactors {
            shared.send(Msg::Shutdown);
        }
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}

/// A request that has been, or is about to be, handed to the kernel.
struct InFlight {
    op: Op,
    /// Bytes transferred so far.
    done: usize,
    /// Completions still expected for the entries currently in the kernel.
    cqes_left: u8,
    /// First error seen (positive errno).
    error: Option<i32>,
    /// A read hit end of file.
    eof: bool,
    /// The attached sync has succeeded.
    synced: bool,
    /// The last attempt should simply be repeated (interrupted).
    again: bool,
}

struct Reactor {
    ring: IoUring,
    rx: Receiver<Msg>,
    shared: Arc<ReactorShared>,
    counters: Arc<Counters>,
    pending: [VecDeque<Op>; Urgency::COUNT],
    retry: VecDeque<usize>,
    slots: Vec<Option<InFlight>>,
    free_slots: Vec<usize>,
    inflight: usize,
    background_inflight: usize,
    fixed_enabled: bool,
    free_fixed: Vec<u32>,
    next_fixed: u32,
    wake_buf: Box<u64>,
    wake_armed: bool,
    shutdown: bool,
}

impl Reactor {
    fn new(
        rx: Receiver<Msg>,
        shared: Arc<ReactorShared>,
        counters: Arc<Counters>,
    ) -> std::io::Result<Self> {
        // Single issuer + deferred task work: completions are processed only
        // when this thread asks for them, which avoids interrupting it. Older
        // kernels do not know these flags, so fall back to a plain ring.
        let ring = IoUring::builder()
            .setup_cqsize(MAX_INFLIGHT as u32)
            .setup_single_issuer()
            .setup_defer_taskrun()
            .build(RING_ENTRIES)
            .or_else(|_| {
                IoUring::builder()
                    .setup_cqsize(MAX_INFLIGHT as u32)
                    .build(RING_ENTRIES)
            })?;
        let fixed_enabled = ring.submitter().register_files_sparse(FIXED_FILES).is_ok();
        Ok(Self {
            ring,
            rx,
            shared,
            counters,
            pending: Default::default(),
            retry: VecDeque::new(),
            slots: Vec::new(),
            free_slots: Vec::new(),
            inflight: 0,
            background_inflight: 0,
            fixed_enabled,
            free_fixed: Vec::new(),
            next_fixed: 0,
            wake_buf: Box::new(0),
            wake_armed: false,
            shutdown: false,
        })
    }

    fn run(&mut self) {
        loop {
            self.drain_channel();
            let pushed = self.fill_submission_queue();
            if self.shutdown && self.inflight == 0 && self.nothing_queued() {
                return;
            }
            self.arm_wake();

            // Announce that we are about to block, then look at the channel one
            // last time (see `ReactorShared::send`).
            self.shared.sleeping.store(true, Ordering::SeqCst);
            match self.rx.try_recv() {
                Ok(msg) => {
                    self.shared.sleeping.store(false, Ordering::SeqCst);
                    self.handle(msg);
                    // Hand over what is already queued without waiting, then
                    // go round again for the new arrivals.
                    self.enter(0, pushed);
                    self.reap();
                    continue;
                }
                Err(TryRecvError::Empty) => {}
                Err(TryRecvError::Disconnected) => self.shutdown = true,
            }
            let ok = self.enter(1, pushed);
            self.shared.sleeping.store(false, Ordering::SeqCst);
            self.reap();
            if !ok {
                self.fail_everything();
                return;
            }
        }
    }

    fn nothing_queued(&self) -> bool {
        self.retry.is_empty() && self.pending.iter().all(VecDeque::is_empty)
    }

    /// Enter the kernel: submit what is queued and wait for `want` completions.
    /// Returns false on an error that makes the ring unusable.
    fn enter(&mut self, want: usize, pushed: usize) -> bool {
        if pushed > 0 {
            self.counters.submits.fetch_add(1, Ordering::Relaxed);
        }
        loop {
            match self.ring.submit_and_wait(want) {
                Ok(_) => return true,
                Err(e) => match e.raw_os_error() {
                    Some(libc::EINTR) => {}
                    // The completion queue is full or the kernel is short of
                    // memory for now: make room and let the loop come back.
                    Some(libc::EBUSY) | Some(libc::EAGAIN) => return true,
                    _ => {
                        tracing::error!(error = %e, "io_uring submit failed");
                        return false;
                    }
                },
            }
        }
    }

    fn drain_channel(&mut self) {
        loop {
            match self.rx.try_recv() {
                Ok(msg) => self.handle(msg),
                Err(TryRecvError::Empty) => return,
                Err(TryRecvError::Disconnected) => {
                    self.shutdown = true;
                    return;
                }
            }
        }
    }

    fn handle(&mut self, msg: Msg) {
        match msg {
            Msg::Op(op) => self.pending[op.urgency as usize].push_back(op),
            Msg::Register { fd, reply } => {
                let slot = self.register(fd);
                // The opener may have gone away while we registered its file.
                // Nobody would ever release the slot, so release it now.
                if reply.send(slot).is_err() {
                    if let Some(slot) = slot {
                        self.unregister(slot);
                    }
                }
            }
            Msg::Unregister(slot) => self.unregister(slot),
            Msg::Shutdown => self.shutdown = true,
        }
    }

    fn unregister(&mut self, slot: u32) {
        let _ = self.ring.submitter().register_files_update(slot, &[-1]);
        self.free_fixed.push(slot);
        self.counters
            .registered_files
            .fetch_sub(1, Ordering::Relaxed);
    }

    fn register(&mut self, fd: RawFd) -> Option<u32> {
        if !self.fixed_enabled {
            return None;
        }
        let (slot, fresh) = match self.free_fixed.pop() {
            Some(slot) => (slot, false),
            None if self.next_fixed < FIXED_FILES => (self.next_fixed, true),
            None => return None,
        };
        match self.ring.submitter().register_files_update(slot, &[fd]) {
            Ok(_) => {
                if fresh {
                    self.next_fixed += 1;
                }
                self.counters
                    .registered_files
                    .fetch_add(1, Ordering::Relaxed);
                Some(slot)
            }
            Err(_) => {
                if !fresh {
                    self.free_fixed.push(slot);
                }
                None
            }
        }
    }

    /// Keep one read on the eventfd in the kernel so that a write to it
    /// produces a completion and ends a wait.
    fn arm_wake(&mut self) {
        if self.wake_armed {
            return;
        }
        let entry = opcode::Read::new(
            types::Fd(self.shared.eventfd.as_raw_fd()),
            (&mut *self.wake_buf as *mut u64).cast(),
            std::mem::size_of::<u64>() as u32,
        )
        .build()
        .user_data(WAKE);
        // SAFETY: the entry points at `wake_buf`, a heap allocation owned by
        // the reactor that outlives the ring's use of it.
        if unsafe { self.ring.submission().push(&entry) }.is_ok() {
            self.wake_armed = true;
        }
    }

    /// Move queued requests into the submission queue, most urgent first.
    /// Returns how many requests were pushed.
    fn fill_submission_queue(&mut self) -> usize {
        let mut pushed = 0;
        while let Some(&slot) = self.retry.front() {
            if !self.push_entries(slot) {
                return pushed;
            }
            self.retry.pop_front();
            pushed += 1;
        }
        for urgency in 0..Urgency::COUNT {
            let background = urgency == Urgency::Background as usize;
            loop {
                if self.inflight >= MAX_INFLIGHT / 2 {
                    return pushed;
                }
                if background && self.background_inflight >= MAX_BACKGROUND {
                    break;
                }
                if self.submission_space() < 3 {
                    return pushed;
                }
                let Some(op) = self.pending[urgency].pop_front() else {
                    break;
                };
                let slot = self.allocate_slot(op);
                self.inflight += 1;
                if background {
                    self.background_inflight += 1;
                }
                let ok = self.push_entries(slot);
                debug_assert!(ok, "space was checked before taking the request");
                pushed += 1;
            }
        }
        pushed
    }

    fn submission_space(&mut self) -> usize {
        let sq = self.ring.submission();
        sq.capacity() - sq.len()
    }

    fn allocate_slot(&mut self, op: Op) -> usize {
        let state = InFlight {
            op,
            done: 0,
            cqes_left: 0,
            error: None,
            eof: false,
            synced: false,
            again: false,
        };
        match self.free_slots.pop() {
            Some(slot) => {
                self.slots[slot] = Some(state);
                slot
            }
            None => {
                self.slots.push(Some(state));
                self.slots.len() - 1
            }
        }
    }

    /// Build and queue the entries for the next step of the request in `slot`.
    /// Returns false, leaving nothing queued, if the queue lacks room.
    fn push_entries(&mut self, slot: usize) -> bool {
        // Leave one entry spare for the wake-up read.
        if self.submission_space() < 3 {
            return false;
        }
        let state = self.slots[slot]
            .as_mut()
            .expect("slot holds a request while it is queued");
        state.again = false;
        let user_data = (slot as u64) << 1;
        let fixed = match &state.op.file.route {
            super::engine::Route::Uring(route) => route.fixed,
            super::engine::Route::Pool(_) => None,
        };
        let raw = state.op.file.file.as_raw_fd();
        let offset = state.op.offset + state.done as u64;

        // Each opcode takes the descriptor as either a fixed-table index or a
        // raw descriptor; the two are different types.
        macro_rules! with_fd {
            (|$fd:ident| $build:expr) => {
                match fixed {
                    Some(index) => {
                        let $fd = types::Fixed(index);
                        $build
                    }
                    None => {
                        let $fd = types::Fd(raw);
                        $build
                    }
                }
            };
        }

        let mut entries: [Option<squeue::Entry>; 2] = [None, None];
        match &mut state.op.kind {
            OpKind::Read { buf } => {
                let len = (buf.capacity() - state.done).min(MAX_TRANSFER) as u32;
                // SAFETY: `done` is below the capacity, so the pointer stays
                // inside the buffer.
                let ptr = unsafe { buf.as_mut_ptr().add(state.done) };
                entries[0] = Some(
                    with_fd!(|fd| opcode::Read::new(fd, ptr, len).offset(offset).build())
                        .user_data(user_data),
                );
            }
            OpKind::Write { buf, len, sync } => {
                let transfer_done = state.done >= *len;
                if !transfer_done {
                    let chunk = (*len - state.done).min(MAX_TRANSFER) as u32;
                    // SAFETY: `done` is below `len`, which is within the buffer.
                    let ptr = unsafe { buf.as_mut_ptr().add(state.done) as *const u8 };
                    let mut entry =
                        with_fd!(|fd| opcode::Write::new(fd, ptr, chunk).offset(offset).build())
                            .user_data(user_data);
                    if sync.is_some() {
                        // Run the sync only after this write has fully
                        // succeeded; a short write cancels it.
                        entry = entry.flags(squeue::Flags::IO_LINK);
                    }
                    entries[0] = Some(entry);
                }
                if let Some(data_only) = *sync {
                    let flags = if data_only {
                        types::FsyncFlags::DATASYNC
                    } else {
                        types::FsyncFlags::empty()
                    };
                    let entry = with_fd!(|fd| opcode::Fsync::new(fd).flags(flags).build())
                        .user_data(user_data | TAG_SYNC);
                    let index = if transfer_done { 0 } else { 1 };
                    entries[index] = Some(entry);
                }
            }
            OpKind::Sync { data_only } => {
                let flags = if *data_only {
                    types::FsyncFlags::DATASYNC
                } else {
                    types::FsyncFlags::empty()
                };
                entries[0] = Some(
                    with_fd!(|fd| opcode::Fsync::new(fd).flags(flags).build()).user_data(user_data),
                );
            }
            OpKind::Allocate { len } => {
                let len = *len;
                entries[0] = Some(
                    with_fd!(|fd| opcode::Fallocate::new(fd, len).build()).user_data(user_data),
                );
            }
        }

        let mut sq = self.ring.submission();
        for entry in entries.iter().flatten() {
            // SAFETY: the buffers the entry points into are owned by the
            // request in `self.slots`, which is kept until every completion
            // for it has been seen; the file is kept open the same way.
            unsafe { sq.push(entry) }.expect("submission queue space was checked");
            state.cqes_left += 1;
        }
        true
    }

    fn reap(&mut self) {
        let completed: Vec<(u64, i32)> = self
            .ring
            .completion()
            .map(|cqe| (cqe.user_data(), cqe.result()))
            .collect();
        for (user_data, result) in completed {
            if user_data == WAKE {
                self.wake_armed = false;
                continue;
            }
            self.on_completion((user_data >> 1) as usize, user_data & TAG_SYNC != 0, result);
        }
    }

    fn on_completion(&mut self, slot: usize, is_sync: bool, result: i32) {
        let Some(state) = self.slots.get_mut(slot).and_then(Option::as_mut) else {
            return;
        };
        state.cqes_left -= 1;
        if result < 0 {
            let errno = -result;
            match errno {
                libc::EINTR | libc::EAGAIN => state.again = true,
                // The write this sync was linked to did not complete in full;
                // the request continues with the rest of the write.
                libc::ECANCELED if is_sync => {}
                _ => {
                    state.error.get_or_insert(errno);
                }
            }
        } else if is_sync {
            state.synced = true;
        } else {
            match &state.op.kind {
                OpKind::Read { .. } => {
                    if result == 0 {
                        state.eof = true;
                    }
                    state.done += result as usize;
                }
                // A write that makes no progress would be retried forever.
                OpKind::Write { .. } if result == 0 => {
                    state.error.get_or_insert(libc::EIO);
                }
                OpKind::Write { .. } => state.done += result as usize,
                OpKind::Sync { .. } => state.synced = true,
                OpKind::Allocate { .. } => {}
            }
        }
        if state.cqes_left > 0 {
            return;
        }

        let finished = if state.error.is_some() {
            true
        } else if state.again {
            false
        } else {
            match &state.op.kind {
                OpKind::Read { buf } => state.eof || state.done >= buf.capacity(),
                OpKind::Write { len, sync, .. } => {
                    state.done >= *len && (sync.is_none() || state.synced)
                }
                OpKind::Sync { .. } | OpKind::Allocate { .. } => true,
            }
        };
        if !finished {
            self.retry.push_back(slot);
            return;
        }

        let state = self.slots[slot].take().expect("checked above");
        self.free_slots.push(slot);
        self.inflight -= 1;
        if state.op.urgency == Urgency::Background {
            self.background_inflight -= 1;
        }
        let result = match state.error {
            Some(errno) => Err(std::io::Error::from_raw_os_error(errno)),
            None => Ok(state.done),
        };
        state.op.complete(result);
    }

    /// The ring is unusable: fail every request we know about.
    fn fail_everything(&mut self) {
        let error = || std::io::Error::other("io_uring reactor stopped after a submit error");
        // Requests in the kernel may still be using their buffers, so those
        // are leaked rather than freed; the callers are told the request
        // failed.
        for state in self.slots.drain(..).flatten() {
            let InFlight { op, .. } = state;
            let Op {
                file, kind, done, ..
            } = op;
            std::mem::forget(kind);
            std::mem::forget(file);
            drop(done);
        }
        for queue in &mut self.pending {
            for op in queue.drain(..) {
                op.complete(Err(error()));
            }
        }
    }
}
