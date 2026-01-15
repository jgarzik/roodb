//! Test harness for roodb_suite integration tests.
//!
//! Provides TestServer for starting/stopping RooDB instances and connecting via mysql_async.

use std::path::PathBuf;
use std::sync::Arc;

use mysql_async::{Conn, Opts, OptsBuilder, Pool, SslOpts};
use parking_lot::RwLock;

use roodb::catalog::Catalog;
use roodb::io::PosixIOFactory;
use roodb::server::listener::{start_test_server, ServerHandle};
use roodb::storage::lsm::{LsmConfig, LsmEngine};
use roodb::storage::StorageEngine;

/// Test server instance for integration tests.
pub struct TestServer {
    handle: ServerHandle,
    port: u16,
    storage: Arc<dyn StorageEngine>,
    #[allow(dead_code)]
    catalog: Arc<RwLock<Catalog>>,
    data_dir: PathBuf,
    pool: Pool,
}

impl TestServer {
    /// Start a new test server with the given name (used for data directory).
    pub async fn start(name: &str) -> Self {
        // Initialize tracing (only first call succeeds, subsequent are no-ops)
        let _ = tracing_subscriber::fmt()
            .with_env_filter("roodb=warn")
            .try_init();

        // Setup data directory
        let data_dir = Self::test_dir(name);
        Self::cleanup_dir(&data_dir);

        // Create TLS config
        let tls_config = crate::test_utils::certs::test_tls_config();

        // Create storage and catalog
        let factory = Arc::new(PosixIOFactory);
        let config = LsmConfig {
            dir: data_dir.clone(),
        };
        let storage: Arc<dyn StorageEngine> =
            Arc::new(LsmEngine::open(factory, config).await.unwrap());
        let catalog = Arc::new(RwLock::new(Catalog::new()));

        // Find available port
        let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = std::net::TcpListener::bind(addr).unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let server_addr: std::net::SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

        // Start server
        let handle = start_test_server(server_addr, tls_config, storage.clone(), catalog.clone())
            .await
            .expect("Failed to start test server");

        // Give server time to bind
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Create connection pool
        let ssl_opts = SslOpts::default().with_danger_accept_invalid_certs(true);
        let opts: Opts = OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(port)
            .user(Some("root"))
            .ssl_opts(ssl_opts)
            .max_allowed_packet(Some(16_777_216))
            .wait_timeout(Some(28800))
            .into();
        let pool = Pool::new(opts);

        TestServer {
            handle,
            port,
            storage,
            catalog,
            data_dir,
            pool,
        }
    }

    /// Get a new connection from the pool.
    pub async fn connect(&self) -> Conn {
        self.pool.get_conn().await.expect("Failed to connect")
    }

    /// Get the server port.
    #[allow(dead_code)]
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Shutdown the server and cleanup.
    pub async fn shutdown(self) {
        // Disconnect pool
        self.pool.disconnect().await.unwrap();

        // Shutdown server
        self.handle.shutdown();
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Close storage
        self.storage.close().await.unwrap();

        // Cleanup data directory
        Self::cleanup_dir(&self.data_dir);
    }

    fn test_dir(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("roodb_suite_{}_{}", name, std::process::id()));
        path
    }

    fn cleanup_dir(path: &PathBuf) {
        let _ = std::fs::remove_dir_all(path);
    }
}
