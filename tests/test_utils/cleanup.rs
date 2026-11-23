//! atexit-based cleanup: shuts the shared server down and removes test data
//! directories on normal process exit.
//!
//! Uses libc::atexit to ensure cleanup runs on normal exit and panic unwind.
//! Note: Does not run on SIGINT (Ctrl+C) or other signals unless a handler
//! explicitly calls exit(). Cross-platform (POSIX + Windows MSVCRT).

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

static CLEANUP_REGISTERED: AtomicBool = AtomicBool::new(false);

/// Directories to remove when the test process exits.
static SCRATCH_DIRS: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());
static NEXT_SCRATCH: AtomicU64 = AtomicU64::new(0);

/// Create an empty directory for a test's data, removed when the test
/// process exits. A data directory holds preallocated log segments (over
/// 100 MiB), so one left behind by every test of every run fills a disk.
pub fn scratch_dir(name: &str) -> PathBuf {
    register_cleanup_handler();
    let mut path = std::env::temp_dir();
    path.push(format!(
        "roodb_test_{}_{}_{}",
        name,
        std::process::id(),
        NEXT_SCRATCH.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("create test directory");
    SCRATCH_DIRS.lock().unwrap().push(path.clone());
    path
}

/// Register the cleanup handler with atexit (idempotent).
pub(crate) fn register_cleanup_handler() {
    if CLEANUP_REGISTERED.swap(true, Ordering::SeqCst) {
        return;
    }
    unsafe {
        libc::atexit(cleanup_handler);
    }
}

/// Called by atexit on process exit.
extern "C" fn cleanup_handler() {
    // Wrap in catch_unwind to prevent UB from panics across extern "C" boundary
    let _ = std::panic::catch_unwind(|| {
        // Only cleanup if server was actually initialized (avoid starting server during shutdown)
        if let Some(manager) = super::server_manager::ServerManager::try_get() {
            manager.cleanup();
        }
        if let Ok(mut dirs) = SCRATCH_DIRS.lock() {
            for dir in dirs.drain(..) {
                let _ = std::fs::remove_dir_all(dir);
            }
        }
    });
}
