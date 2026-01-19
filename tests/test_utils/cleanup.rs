//! atexit-based cleanup handler for guaranteed server shutdown.
//!
//! Uses libc::atexit to ensure cleanup runs on normal exit, panic unwind,
//! and SIGINT (Ctrl+C). Cross-platform (POSIX + Windows MSVCRT).

use std::sync::atomic::{AtomicBool, Ordering};

static CLEANUP_REGISTERED: AtomicBool = AtomicBool::new(false);

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
    // Only cleanup if server was actually initialized (avoid starting server during shutdown)
    if let Some(manager) = super::server_manager::ServerManager::try_get() {
        manager.cleanup();
    }
}
