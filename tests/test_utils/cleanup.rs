//! atexit-based cleanup handler for server shutdown on normal process exit.
//!
//! Uses libc::atexit to ensure cleanup runs on normal exit and panic unwind.
//! Note: Does not run on SIGINT (Ctrl+C) or other signals unless a handler
//! explicitly calls exit(). Cross-platform (POSIX + Windows MSVCRT).

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
    // Wrap in catch_unwind to prevent UB from panics across extern "C" boundary
    let _ = std::panic::catch_unwind(|| {
        // Only cleanup if server was actually initialized (avoid starting server during shutdown)
        if let Some(manager) = super::server_manager::ServerManager::try_get() {
            manager.cleanup();
        }
    });
}
