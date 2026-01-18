//! Test harness for roodb_suite integration tests.
//!
//! Uses shared binary server for true E2E testing against actual roodb binaries.

use mysql_async::Conn;

use crate::test_utils::binary_server::BinaryServer;

/// Test server instance for integration tests.
///
/// Wraps the shared binary server. Each test gets unique table names
/// for isolation while sharing the same server instance.
pub struct TestServer {
    #[allow(dead_code)]
    test_name: String,
}

impl TestServer {
    /// Start a test session (connects to shared binary server).
    pub async fn start(name: &str) -> Self {
        // Ensure binary server is running (lazy init on first call)
        let _ = BinaryServer::global();
        Self {
            test_name: name.to_string(),
        }
    }

    /// Start with password - incompatible with shared server.
    ///
    /// # Panics
    /// Always panics. Use `#[ignore]` on tests that require custom passwords.
    pub async fn start_with_password(_name: &str, _password: &str) -> Self {
        panic!(
            "start_with_password is incompatible with shared binary server. \
             Mark this test with #[ignore] or use a dedicated test server."
        );
    }

    /// Get a connection from the shared pool.
    pub async fn connect(&self) -> Conn {
        BinaryServer::connect().await
    }

    /// Get the server port.
    #[allow(dead_code)]
    pub fn port(&self) -> u16 {
        BinaryServer::port()
    }

    /// Shutdown (no-op: shared server stays alive for other tests).
    pub async fn shutdown(self) {
        // No-op: the shared binary server persists for all tests
    }
}
