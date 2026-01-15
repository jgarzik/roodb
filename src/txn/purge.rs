//! Background purge task for cleaning up old undo records
//!
//! Similar to InnoDB's purge threads, this periodically removes
//! undo records that are no longer needed for MVCC visibility.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::interval;

use super::TransactionManager;

/// Background task for purging old undo records
///
/// Undo records can be purged when:
/// - For INSERT undo logs: After the transaction commits
/// - For UPDATE/DELETE undo logs: When no active transaction has a read view
///   that might need the old version
pub struct PurgeTask {
    /// Reference to the transaction manager
    txn_manager: Arc<TransactionManager>,

    /// How often to run the purge
    interval: Duration,

    /// Shutdown signal receiver
    shutdown_rx: watch::Receiver<bool>,
}

impl PurgeTask {
    /// Create a new purge task
    ///
    /// # Arguments
    /// * `txn_manager` - The transaction manager to purge from
    /// * `interval` - How often to run the purge (e.g., every 10 seconds)
    /// * `shutdown_rx` - Receiver for shutdown signal
    pub fn new(
        txn_manager: Arc<TransactionManager>,
        interval: Duration,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            txn_manager,
            interval,
            shutdown_rx,
        }
    }

    /// Run the purge task
    ///
    /// This should be spawned as a background task:
    /// ```ignore
    /// let (shutdown_tx, shutdown_rx) = watch::channel(false);
    /// let purge = PurgeTask::new(txn_manager, Duration::from_secs(10), shutdown_rx);
    /// tokio::spawn(purge.run());
    /// // Later: shutdown_tx.send(true).unwrap();
    /// ```
    pub async fn run(mut self) {
        let mut ticker = interval(self.interval);

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    self.purge_once();
                }
                _ = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        tracing::info!("Purge task shutting down");
                        break;
                    }
                }
            }
        }
    }

    /// Run one purge cycle
    fn purge_once(&self) {
        let min_active = self.txn_manager.min_active_txn_id();

        // Only purge if there's something to purge
        if min_active == u64::MAX {
            return;
        }

        let before_count = self.txn_manager.undo_log().record_count();

        // Purge undo log
        self.txn_manager.undo_log().purge(min_active);

        // Purge committed transaction set
        self.txn_manager.purge_committed(min_active);

        let after_count = self.txn_manager.undo_log().record_count();
        let purged = before_count.saturating_sub(after_count);

        if purged > 0 {
            tracing::debug!(
                "Purged {} undo records (min_active_txn={})",
                purged,
                min_active
            );
        }
    }
}

/// Create a purge task with default settings
///
/// Returns the task and a shutdown sender.
#[allow(dead_code)]
pub fn create_purge_task(txn_manager: Arc<TransactionManager>) -> (PurgeTask, watch::Sender<bool>) {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = PurgeTask::new(
        txn_manager,
        Duration::from_secs(10), // Default: every 10 seconds
        shutdown_rx,
    );
    (task, shutdown_tx)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_purge_task_creation() {
        let mgr = Arc::new(TransactionManager::new());
        let (task, shutdown_tx) = create_purge_task(mgr.clone());

        // Verify it's created with correct interval
        assert_eq!(task.interval, Duration::from_secs(10));

        // Clean shutdown
        drop(shutdown_tx);
    }

    #[tokio::test]
    async fn test_purge_cycle() {
        let mgr = Arc::new(TransactionManager::new());

        // Add some undo records
        mgr.undo_log().log_insert(10, "test".to_string(), vec![1]);
        mgr.undo_log().log_insert(20, "test".to_string(), vec![2]);
        mgr.undo_log().log_insert(30, "test".to_string(), vec![3]);

        assert_eq!(mgr.undo_log().record_count(), 3);

        // Start a transaction with ID > 20
        let _txn = mgr
            .begin(super::super::IsolationLevel::RepeatableRead, false)
            .unwrap();
        // This will have ID >= 1, but let's manually check min_active

        // The purge should consider min_active_txn_id
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = PurgeTask::new(mgr.clone(), Duration::from_secs(1), shutdown_rx);

        // Run one purge cycle manually
        task.purge_once();

        // Since we have an active transaction, records below its ID may be purged
        // but records at or above are kept

        // Cleanup
        drop(shutdown_tx);
    }
}
