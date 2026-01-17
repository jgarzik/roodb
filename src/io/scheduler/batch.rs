//! Batch submission and priority queue management
//!
//! Handles selection of I/O requests from priority queues and
//! batching for efficient submission to io_uring.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;

use super::config::SchedulerConfig;
use super::{IoPriority, IoRequest};

/// Priority queue set for I/O requests
///
/// Maintains separate queues for each priority level with
/// deadline-aware selection.
pub struct PriorityQueues {
    /// One queue per priority level
    queues: [Mutex<VecDeque<IoRequest>>; IoPriority::COUNT],
}

impl PriorityQueues {
    /// Create new empty priority queues
    pub fn new() -> Self {
        Self {
            queues: [
                Mutex::new(VecDeque::new()),
                Mutex::new(VecDeque::new()),
                Mutex::new(VecDeque::new()),
                Mutex::new(VecDeque::new()),
            ],
        }
    }

    /// Push a request to the appropriate queue
    pub fn push(&self, request: IoRequest) {
        let queue_idx = request.priority as usize;
        self.queues[queue_idx].lock().push_back(request);
    }

    /// Get total queue depth across all priorities
    pub fn total_depth(&self) -> usize {
        self.queues.iter().map(|q| q.lock().len()).sum()
    }

    /// Get queue depth for a specific priority
    pub fn depth(&self, priority: IoPriority) -> usize {
        self.queues[priority as usize].lock().len()
    }

    /// Check if all queues are empty
    pub fn is_empty(&self) -> bool {
        self.queues.iter().all(|q| q.lock().is_empty())
    }

    /// Select a batch of requests respecting priorities and deadlines
    ///
    /// Selection algorithm:
    /// 1. First, take any expired deadline requests (any priority)
    /// 2. Fill remaining slots by priority order
    /// 3. Limit compaction requests per batch for fairness
    pub fn select_batch(&self, config: &SchedulerConfig) -> Vec<IoRequest> {
        let mut batch = Vec::with_capacity(config.max_batch_size);
        let now = Instant::now();

        // Phase 1: Expired deadlines first (any priority)
        for queue in &self.queues {
            let mut q = queue.lock();
            let mut i = 0;
            while i < q.len() && batch.len() < config.max_batch_size {
                if q[i].deadline.is_some_and(|d| d <= now) {
                    batch.push(q.remove(i).unwrap());
                } else {
                    i += 1;
                }
            }
        }

        if batch.len() >= config.max_batch_size {
            return batch;
        }

        // Phase 2: Fill by priority order
        for (priority_idx, queue) in self.queues.iter().enumerate() {
            // Limit compaction ops per batch for fairness
            let max_from_this = if priority_idx == IoPriority::Compaction as usize {
                (config.max_batch_size / 4).max(1)
            } else {
                config.max_batch_size
            };

            let mut q = queue.lock();
            let mut taken = 0;

            while taken < max_from_this && batch.len() < config.max_batch_size {
                if let Some(req) = q.pop_front() {
                    batch.push(req);
                    taken += 1;
                } else {
                    break;
                }
            }
        }

        batch
    }

    /// Drain all requests from a specific priority queue
    pub fn drain(&self, priority: IoPriority) -> Vec<IoRequest> {
        self.queues[priority as usize].lock().drain(..).collect()
    }

    /// Drain all requests from all queues
    pub fn drain_all(&self) -> Vec<IoRequest> {
        let mut all = Vec::new();
        for queue in &self.queues {
            all.extend(queue.lock().drain(..));
        }
        all
    }
}

impl Default for PriorityQueues {
    fn default() -> Self {
        Self::new()
    }
}

/// Batch builder for collecting I/O operations
///
/// Collects requests until batch is full or explicitly submitted.
pub struct BatchBuilder {
    /// Collected requests
    requests: Vec<IoRequest>,
    /// Maximum batch size
    max_size: usize,
}

impl BatchBuilder {
    /// Create a new batch builder
    pub fn new(max_size: usize) -> Self {
        Self {
            requests: Vec::with_capacity(max_size),
            max_size,
        }
    }

    /// Add a request to the batch
    ///
    /// Returns true if added, false if batch is full.
    pub fn add(&mut self, request: IoRequest) -> bool {
        if self.requests.len() >= self.max_size {
            return false;
        }
        self.requests.push(request);
        true
    }

    /// Check if batch is full
    pub fn is_full(&self) -> bool {
        self.requests.len() >= self.max_size
    }

    /// Check if batch is empty
    pub fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }

    /// Get current batch size
    pub fn len(&self) -> usize {
        self.requests.len()
    }

    /// Take the batch, leaving an empty builder
    pub fn take(&mut self) -> Vec<IoRequest> {
        std::mem::take(&mut self.requests)
    }

    /// Clear the batch
    pub fn clear(&mut self) {
        self.requests.clear();
    }
}

/// Shared state for batch coordination
pub struct BatchCoordinator {
    /// Priority queues
    queues: Arc<PriorityQueues>,
    /// Configuration
    config: SchedulerConfig,
}

impl BatchCoordinator {
    /// Create a new batch coordinator
    pub fn new(config: SchedulerConfig) -> Self {
        Self {
            queues: Arc::new(PriorityQueues::new()),
            config,
        }
    }

    /// Get a reference to the priority queues
    pub fn queues(&self) -> &PriorityQueues {
        &self.queues
    }

    /// Get an Arc to the priority queues
    pub fn queues_arc(&self) -> Arc<PriorityQueues> {
        Arc::clone(&self.queues)
    }

    /// Submit a request
    pub fn submit(&self, request: IoRequest) {
        self.queues.push(request);
    }

    /// Select the next batch
    pub fn select_batch(&self) -> Vec<IoRequest> {
        self.queues.select_batch(&self.config)
    }

    /// Get total queue depth
    pub fn queue_depth(&self) -> usize {
        self.queues.total_depth()
    }

    /// Check if work is available
    pub fn has_work(&self) -> bool {
        !self.queues.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::scheduler::{next_request_id, IoOp};
    use tokio::sync::oneshot;

    fn make_request(priority: IoPriority, deadline_ms: Option<u64>) -> IoRequest {
        let (tx, _rx) = oneshot::channel();
        IoRequest {
            id: next_request_id(),
            priority,
            op: IoOp::Sync { file_id: 1 },
            deadline: deadline_ms.map(|ms| Instant::now() + std::time::Duration::from_millis(ms)),
            submitted_at: Instant::now(),
            completion_tx: tx,
        }
    }

    #[test]
    fn test_priority_ordering() {
        let queues = PriorityQueues::new();
        let config = SchedulerConfig::default();

        // Add requests in reverse priority order
        queues.push(make_request(IoPriority::Compaction, None));
        queues.push(make_request(IoPriority::Flush, None));
        queues.push(make_request(IoPriority::QueryRead, None));
        queues.push(make_request(IoPriority::Wal, None));

        let batch = queues.select_batch(&config);

        // Should come out in priority order
        assert_eq!(batch.len(), 4);
        assert_eq!(batch[0].priority, IoPriority::Wal);
        assert_eq!(batch[1].priority, IoPriority::QueryRead);
        assert_eq!(batch[2].priority, IoPriority::Flush);
        assert_eq!(batch[3].priority, IoPriority::Compaction);
    }

    #[test]
    fn test_compaction_limiting() {
        let queues = PriorityQueues::new();
        let config = SchedulerConfig::default().with_max_batch_size(8);

        // Add many compaction requests
        for _ in 0..10 {
            queues.push(make_request(IoPriority::Compaction, None));
        }

        // Add one WAL request
        queues.push(make_request(IoPriority::Wal, None));

        let batch = queues.select_batch(&config);

        // Should have WAL first, then limited compaction
        assert!(batch.len() <= 8);
        assert_eq!(batch[0].priority, IoPriority::Wal);

        // Compaction should be limited to max_batch_size/4
        let compaction_count = batch
            .iter()
            .filter(|r| r.priority == IoPriority::Compaction)
            .count();
        assert!(compaction_count <= 2); // 8/4 = 2
    }

    #[test]
    fn test_queue_depth() {
        let queues = PriorityQueues::new();

        queues.push(make_request(IoPriority::Wal, None));
        queues.push(make_request(IoPriority::QueryRead, None));
        queues.push(make_request(IoPriority::QueryRead, None));

        assert_eq!(queues.total_depth(), 3);
        assert_eq!(queues.depth(IoPriority::Wal), 1);
        assert_eq!(queues.depth(IoPriority::QueryRead), 2);
        assert_eq!(queues.depth(IoPriority::Flush), 0);
    }
}
