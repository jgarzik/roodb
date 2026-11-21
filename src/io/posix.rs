//! The portable I/O backend: worker threads doing positional reads and writes.
//!
//! Each worker takes the most urgent queued request and runs it to completion
//! with ordinary blocking system calls. Several workers can be inside the
//! kernel for the same file at once; nothing here serialises per file.

use std::collections::VecDeque;
use std::sync::Arc;
use std::thread::JoinHandle;

use parking_lot::{Condvar, Mutex};

use super::engine::{Op, OpKind, Urgency};
use super::error::IoResult;

/// Upper bound on worker threads chosen automatically.
const MAX_AUTO_WORKERS: usize = 16;

struct Queues {
    by_urgency: [VecDeque<Op>; Urgency::COUNT],
    /// Workers currently running a background request.
    background_running: usize,
    shutdown: bool,
}

/// State shared by the workers and by every file routed to the pool.
pub(crate) struct Shared {
    queues: Mutex<Queues>,
    wake: Condvar,
    /// Most workers allowed to run background requests at once, so that some
    /// stay free for work a client or a commit is waiting on.
    max_background: usize,
}

impl Shared {
    /// Queue a request. Returns false if the pool has shut down.
    pub(crate) fn submit(&self, op: Op) -> bool {
        let mut queues = self.queues.lock();
        if queues.shutdown {
            return false;
        }
        queues.by_urgency[op.urgency as usize].push_back(op);
        drop(queues);
        self.wake.notify_one();
        true
    }

    fn next(&self) -> Option<Op> {
        let mut queues = self.queues.lock();
        loop {
            let background = Urgency::Background as usize;
            for urgency in 0..Urgency::COUNT {
                if urgency == background && queues.background_running >= self.max_background {
                    continue;
                }
                if let Some(op) = queues.by_urgency[urgency].pop_front() {
                    if urgency == background {
                        queues.background_running += 1;
                    }
                    return Some(op);
                }
            }
            if queues.shutdown {
                return None;
            }
            self.wake.wait(&mut queues);
        }
    }

    /// A worker finished a request of the given urgency.
    fn finished(&self, urgency: Urgency) {
        if urgency == Urgency::Background {
            self.queues.lock().background_running -= 1;
            // A background request may have been held back by the cap.
            self.wake.notify_one();
        }
    }
}

/// The worker pool.
pub(crate) struct Pool {
    shared: Arc<Shared>,
    workers: Vec<JoinHandle<()>>,
}

impl Pool {
    /// Start `workers` threads (0 = one per core, capped).
    pub(crate) fn start(workers: usize) -> IoResult<Self> {
        let workers = if workers == 0 {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
                .min(MAX_AUTO_WORKERS)
        } else {
            workers
        };
        // With more than one worker, keep at least a quarter of them (and at
        // least one) away from background work.
        let max_background = if workers > 1 {
            workers - (workers / 4).max(1)
        } else {
            1
        };
        let shared = Arc::new(Shared {
            queues: Mutex::new(Queues {
                by_urgency: Default::default(),
                background_running: 0,
                shutdown: false,
            }),
            wake: Condvar::new(),
            max_background,
        });
        // Built before any thread is spawned: if a spawn fails, dropping the
        // pool stops and joins the workers already running.
        let mut pool = Self {
            shared,
            workers: Vec::with_capacity(workers),
        };
        for i in 0..workers {
            let shared = pool.shared.clone();
            let handle = std::thread::Builder::new()
                .name(format!("roodb-io-{i}"))
                .spawn(move || {
                    while let Some(mut op) = shared.next() {
                        let urgency = op.urgency;
                        let result = execute(&mut op);
                        op.file
                            .counters
                            .submits
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        shared.finished(urgency);
                        op.complete(result);
                    }
                })?;
            pool.workers.push(handle);
        }
        Ok(pool)
    }

    pub(crate) fn shared(&self) -> Arc<Shared> {
        self.shared.clone()
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        self.shared.queues.lock().shutdown = true;
        self.wake_all();
        for handle in self.workers.drain(..) {
            let _ = handle.join();
        }
    }
}

impl Pool {
    fn wake_all(&self) {
        self.shared.wake.notify_all();
    }
}

#[cfg(unix)]
fn pread(file: &std::fs::File, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(file, buf, offset)
}

#[cfg(unix)]
fn pwrite(file: &std::fs::File, buf: &[u8], offset: u64) -> std::io::Result<usize> {
    std::os::unix::fs::FileExt::write_at(file, buf, offset)
}

#[cfg(windows)]
fn pread(file: &std::fs::File, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
    std::os::windows::fs::FileExt::seek_read(file, buf, offset)
}

#[cfg(windows)]
fn pwrite(file: &std::fs::File, buf: &[u8], offset: u64) -> std::io::Result<usize> {
    std::os::windows::fs::FileExt::seek_write(file, buf, offset)
}

/// Run one request with blocking calls. Returns bytes transferred.
fn execute(op: &mut Op) -> std::io::Result<usize> {
    let file = &op.file.file;
    match &mut op.kind {
        OpKind::Read { buf } => {
            let dst = buf.as_mut_slice();
            let mut done = 0;
            while done < dst.len() {
                match pread(file, &mut dst[done..], op.offset + done as u64) {
                    Ok(0) => break,
                    Ok(n) => done += n,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(e) => return Err(e),
                }
            }
            Ok(done)
        }
        OpKind::Write { buf, len, sync } => {
            let src = &buf.as_mut_slice()[..*len];
            let mut done = 0;
            while done < src.len() {
                match pwrite(file, &src[done..], op.offset + done as u64) {
                    Ok(0) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::WriteZero,
                            "write returned zero bytes",
                        ))
                    }
                    Ok(n) => done += n,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(e) => return Err(e),
                }
            }
            match sync {
                Some(true) => file.sync_data()?,
                Some(false) => file.sync_all()?,
                None => {}
            }
            Ok(done)
        }
        OpKind::Sync { data_only } => {
            if *data_only {
                file.sync_data()?;
            } else {
                file.sync_all()?;
            }
            Ok(0)
        }
        OpKind::Allocate { len } => {
            allocate(file, *len)?;
            Ok(0)
        }
    }
}

/// Reserve space for the first `len` bytes and grow the file to that size.
#[cfg(target_os = "linux")]
pub(crate) fn allocate(file: &std::fs::File, len: u64) -> std::io::Result<()> {
    use std::os::unix::io::AsRawFd;
    loop {
        // SAFETY: plain system call on a descriptor we hold open.
        let rc = unsafe { libc::posix_fallocate(file.as_raw_fd(), 0, len as libc::off_t) };
        match rc {
            0 => return Ok(()),
            libc::EINTR => {}
            err => return Err(std::io::Error::from_raw_os_error(err)),
        }
    }
}

/// Grow the file to `len`. Platforms without a portable reservation call get
/// the size only; the space is claimed as it is written.
#[cfg(not(target_os = "linux"))]
pub(crate) fn allocate(file: &std::fs::File, len: u64) -> std::io::Result<()> {
    if file.metadata()?.len() < len {
        file.set_len(len)?;
    }
    Ok(())
}
