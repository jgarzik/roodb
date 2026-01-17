//! Background I/O batcher
//!
//! Collects background I/O operations and triggers batch submission
//! when count or time threshold is reached.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::{oneshot, Notify};

use super::config::SchedulerConfig;
use super::IoContext;
use crate::io::aligned_buffer::AlignedBuffer;

/// Operation type for batching
#[derive(Debug)]
pub enum BatchOp {
    /// Read operation: (offset, size)
    Read { offset: u64, size: usize },
    /// Write operation: (offset, data)
    Write { offset: u64, data: AlignedBuffer },
    /// Sync operation
    Sync,
}

/// Pending background operation
#[derive(Debug)]
pub struct PendingOp {
    /// File identifier
    pub file_id: u64,
    /// File path (for creating new rings if needed)
    pub file_path: PathBuf,
    /// The operation to perform
    pub op: BatchOp,
    /// I/O context
    pub context: IoContext,
    /// Completion channel
    pub tx: oneshot::Sender<BatchResult>,
    /// When this op was queued
    pub queued_at: Instant,
}

/// Result of a batch operation
#[derive(Debug)]
pub enum BatchResult {
    /// Read completed with data
    Read(Result<AlignedBuffer, String>),
    /// Write completed with bytes written
    Write(Result<usize, String>),
    /// Sync completed
    Sync(Result<(), String>),
}

/// Background I/O batcher
///
/// Collects background operations until batch size or deadline is reached,
/// then signals for batch submission.
pub struct BackgroundBatcher {
    /// Pending operations
    pending: Mutex<Vec<PendingOp>>,

    /// Maximum batch size before forced submit
    max_batch_size: usize,

    /// Maximum wait time before submit
    max_wait: Duration,

    /// Notify when batch is ready for submission
    batch_ready: Notify,

    /// Last submit time
    last_submit: Mutex<Instant>,

    /// Total bytes in pending batch
    pending_bytes: AtomicU64,

    /// Whether batcher is shutdown
    shutdown: AtomicBool,
}

impl BackgroundBatcher {
    /// Create a new background batcher
    pub fn new(config: &SchedulerConfig) -> Self {
        Self {
            pending: Mutex::new(Vec::with_capacity(config.background_batch_size)),
            max_batch_size: config.background_batch_size,
            max_wait: config.background_batch_deadline,
            batch_ready: Notify::new(),
            last_submit: Mutex::new(Instant::now()),
            pending_bytes: AtomicU64::new(0),
            shutdown: AtomicBool::new(false),
        }
    }

    /// Create with specific batch size and deadline
    pub fn with_config(max_batch_size: usize, max_wait: Duration) -> Self {
        Self {
            pending: Mutex::new(Vec::with_capacity(max_batch_size)),
            max_batch_size,
            max_wait,
            batch_ready: Notify::new(),
            last_submit: Mutex::new(Instant::now()),
            pending_bytes: AtomicU64::new(0),
            shutdown: AtomicBool::new(false),
        }
    }

    /// Submit a background operation for batching
    ///
    /// Returns a receiver that will get the result when the op completes.
    pub fn submit(&self, op: PendingOp) -> bool {
        if self.shutdown.load(Ordering::Relaxed) {
            return false;
        }

        let bytes = match &op.op {
            BatchOp::Read { size, .. } => *size as u64,
            BatchOp::Write { data, .. } => data.len() as u64,
            BatchOp::Sync => 0,
        };

        let should_notify = {
            let mut pending = self.pending.lock();
            pending.push(op);
            self.pending_bytes.fetch_add(bytes, Ordering::Relaxed);
            pending.len() >= self.max_batch_size
        };

        if should_notify {
            self.batch_ready.notify_one();
        }

        true
    }

    /// Submit a read operation
    pub fn submit_read(
        &self,
        file_id: u64,
        file_path: PathBuf,
        offset: u64,
        size: usize,
        context: IoContext,
    ) -> Option<oneshot::Receiver<BatchResult>> {
        let (tx, rx) = oneshot::channel();
        let op = PendingOp {
            file_id,
            file_path,
            op: BatchOp::Read { offset, size },
            context,
            tx,
            queued_at: Instant::now(),
        };

        if self.submit(op) {
            Some(rx)
        } else {
            None
        }
    }

    /// Submit a write operation
    pub fn submit_write(
        &self,
        file_id: u64,
        file_path: PathBuf,
        offset: u64,
        data: AlignedBuffer,
        context: IoContext,
    ) -> Option<oneshot::Receiver<BatchResult>> {
        let (tx, rx) = oneshot::channel();
        let op = PendingOp {
            file_id,
            file_path,
            op: BatchOp::Write { offset, data },
            context,
            tx,
            queued_at: Instant::now(),
        };

        if self.submit(op) {
            Some(rx)
        } else {
            None
        }
    }

    /// Submit a sync operation
    pub fn submit_sync(
        &self,
        file_id: u64,
        file_path: PathBuf,
        context: IoContext,
    ) -> Option<oneshot::Receiver<BatchResult>> {
        let (tx, rx) = oneshot::channel();
        let op = PendingOp {
            file_id,
            file_path,
            op: BatchOp::Sync,
            context,
            tx,
            queued_at: Instant::now(),
        };

        if self.submit(op) {
            Some(rx)
        } else {
            None
        }
    }

    /// Wait for batch to be ready (either full or deadline reached)
    ///
    /// Returns when:
    /// - Batch size threshold is reached
    /// - Deadline since last submit is reached
    /// - Shutdown is requested
    pub async fn wait_for_batch(&self) {
        loop {
            let time_until_deadline = {
                let last = *self.last_submit.lock();
                let elapsed = last.elapsed();
                if elapsed >= self.max_wait {
                    Duration::ZERO
                } else {
                    self.max_wait - elapsed
                }
            };

            if time_until_deadline.is_zero() {
                // Deadline reached, check if we have pending ops
                if !self.pending.lock().is_empty() {
                    return;
                }
            }

            tokio::select! {
                _ = self.batch_ready.notified() => {
                    // Batch size threshold reached
                    return;
                }
                _ = tokio::time::sleep(time_until_deadline.max(Duration::from_micros(100))) => {
                    // Timeout - check deadline
                    if !self.pending.lock().is_empty() {
                        return;
                    }
                }
            }

            if self.shutdown.load(Ordering::Relaxed) {
                return;
            }
        }
    }

    /// Take all pending operations for submission
    ///
    /// Returns operations grouped by file_id for efficient batch submission.
    pub fn take_batch(&self) -> Vec<PendingOp> {
        let mut pending = self.pending.lock();
        let batch = std::mem::take(&mut *pending);
        self.pending_bytes.store(0, Ordering::Relaxed);
        *self.last_submit.lock() = Instant::now();
        batch
    }

    /// Take pending operations grouped by file
    pub fn take_batch_by_file(&self) -> Vec<(u64, Vec<PendingOp>)> {
        let batch = self.take_batch();
        group_by_file(batch)
    }

    /// Get number of pending operations
    pub fn pending_count(&self) -> usize {
        self.pending.lock().len()
    }

    /// Get total bytes in pending batch
    pub fn pending_bytes(&self) -> u64 {
        self.pending_bytes.load(Ordering::Relaxed)
    }

    /// Check if batch is ready (either full or has pending ops past deadline)
    pub fn is_ready(&self) -> bool {
        let pending = self.pending.lock();
        if pending.is_empty() {
            return false;
        }
        if pending.len() >= self.max_batch_size {
            return true;
        }
        // Check if oldest op is past deadline
        if let Some(oldest) = pending.first() {
            oldest.queued_at.elapsed() >= self.max_wait
        } else {
            false
        }
    }

    /// Shutdown the batcher
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        self.batch_ready.notify_waiters();
    }

    /// Check if shutdown
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }

    /// Get max batch size
    pub fn max_batch_size(&self) -> usize {
        self.max_batch_size
    }

    /// Get max wait duration
    pub fn max_wait(&self) -> Duration {
        self.max_wait
    }
}

impl Default for BackgroundBatcher {
    fn default() -> Self {
        Self::with_config(32, Duration::from_millis(1))
    }
}

/// Group operations by file ID
fn group_by_file(ops: Vec<PendingOp>) -> Vec<(u64, Vec<PendingOp>)> {
    use std::collections::HashMap;

    let mut by_file: HashMap<u64, Vec<PendingOp>> = HashMap::new();

    for op in ops {
        by_file.entry(op.file_id).or_default().push(op);
    }

    by_file.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_context() -> IoContext {
        IoContext::new(
            super::super::Urgency::Background,
            super::super::OpKind::Write,
        )
    }

    #[test]
    fn test_batcher_creation() {
        let batcher = BackgroundBatcher::with_config(32, Duration::from_millis(1));
        assert_eq!(batcher.max_batch_size(), 32);
        assert_eq!(batcher.pending_count(), 0);
    }

    #[test]
    fn test_submit_and_take() {
        let batcher = BackgroundBatcher::with_config(32, Duration::from_millis(1));

        // Submit a few ops
        let _rx1 = batcher.submit_read(1, PathBuf::from("/test"), 0, 4096, make_test_context());
        let _rx2 = batcher.submit_read(1, PathBuf::from("/test"), 4096, 4096, make_test_context());
        let _rx3 = batcher.submit_read(2, PathBuf::from("/test2"), 0, 4096, make_test_context());

        assert_eq!(batcher.pending_count(), 3);

        // Take batch
        let batch = batcher.take_batch();
        assert_eq!(batch.len(), 3);
        assert_eq!(batcher.pending_count(), 0);
    }

    #[test]
    fn test_group_by_file() {
        let batcher = BackgroundBatcher::with_config(32, Duration::from_millis(1));

        // Submit ops for 2 files
        batcher.submit_read(1, PathBuf::from("/f1"), 0, 4096, make_test_context());
        batcher.submit_read(1, PathBuf::from("/f1"), 4096, 4096, make_test_context());
        batcher.submit_read(2, PathBuf::from("/f2"), 0, 4096, make_test_context());
        batcher.submit_read(1, PathBuf::from("/f1"), 8192, 4096, make_test_context());

        let grouped = batcher.take_batch_by_file();

        // Should have 2 groups
        assert_eq!(grouped.len(), 2);

        // Find file 1's group
        let file1_ops = grouped.iter().find(|(id, _)| *id == 1).map(|(_, ops)| ops);
        assert!(file1_ops.is_some());
        assert_eq!(file1_ops.unwrap().len(), 3);

        // Find file 2's group
        let file2_ops = grouped.iter().find(|(id, _)| *id == 2).map(|(_, ops)| ops);
        assert!(file2_ops.is_some());
        assert_eq!(file2_ops.unwrap().len(), 1);
    }

    #[test]
    fn test_batch_ready_on_size() {
        let batcher = BackgroundBatcher::with_config(3, Duration::from_secs(10));

        // Submit 2 ops - not ready
        batcher.submit_read(1, PathBuf::from("/f1"), 0, 4096, make_test_context());
        batcher.submit_read(1, PathBuf::from("/f1"), 4096, 4096, make_test_context());
        assert!(!batcher.is_ready());

        // Submit 3rd - should be ready
        batcher.submit_read(1, PathBuf::from("/f1"), 8192, 4096, make_test_context());
        assert!(batcher.is_ready());
    }

    #[test]
    fn test_pending_bytes() {
        let batcher = BackgroundBatcher::with_config(32, Duration::from_millis(1));

        batcher.submit_read(1, PathBuf::from("/f1"), 0, 4096, make_test_context());
        assert_eq!(batcher.pending_bytes(), 4096);

        batcher.submit_read(1, PathBuf::from("/f1"), 4096, 8192, make_test_context());
        assert_eq!(batcher.pending_bytes(), 4096 + 8192);

        batcher.take_batch();
        assert_eq!(batcher.pending_bytes(), 0);
    }

    #[test]
    fn test_shutdown() {
        let batcher = BackgroundBatcher::with_config(32, Duration::from_millis(1));

        assert!(!batcher.is_shutdown());

        // Submit works before shutdown
        let rx = batcher.submit_read(1, PathBuf::from("/f1"), 0, 4096, make_test_context());
        assert!(rx.is_some());

        batcher.shutdown();
        assert!(batcher.is_shutdown());

        // Submit fails after shutdown
        let rx = batcher.submit_read(1, PathBuf::from("/f1"), 0, 4096, make_test_context());
        assert!(rx.is_none());
    }
}
