//! Test harness for roodb_suite integration tests.
//!
//! Provides TestServer for starting/stopping RooDB instances and connecting via mysql_async.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use mysql_async::{Conn, Opts, OptsBuilder, Pool, SslOpts};
use parking_lot::RwLock;

use roodb::catalog::system_tables::{SYSTEM_GRANTS, SYSTEM_USERS};
use roodb::catalog::Catalog;
use roodb::executor::encoding::{encode_row, encode_row_key};
use roodb::executor::{Datum, Row};
use roodb::io::PosixIOFactory;
use roodb::server::hash_password;
use roodb::server::listener::{start_test_server, ServerHandle};
use roodb::sql::Privilege;
use roodb::storage::lsm::{LsmConfig, LsmEngine};
use roodb::storage::StorageEngine;

/// Row ID counter for test initialization
static TEST_ROW_ID: AtomicU64 = AtomicU64::new(1);

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
        Self::start_with_password(name, "").await
    }

    /// Start a new test server with a specific root password.
    pub async fn start_with_password(name: &str, password: &str) -> Self {
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

        // Initialize root user with empty password for tests
        Self::initialize_root_user(&storage, password).await;

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

    /// Initialize root user in storage for testing.
    ///
    /// This creates the root user with the given password and grants ALL PRIVILEGES.
    async fn initialize_root_user(storage: &Arc<dyn StorageEngine>, password: &str) {
        let password_hash = hash_password(password);

        // Create user row for root@%
        let user_row = Row::new(vec![
            Datum::String("root".to_string()),                  // username
            Datum::String("%".to_string()),                     // host
            Datum::String(password_hash),                       // password_hash
            Datum::String("mysql_native_password".to_string()), // auth_plugin
            Datum::Null,                                        // ssl_subject
            Datum::Null,                                        // ssl_issuer
            Datum::Bool(false),                                 // account_locked
            Datum::Bool(false),                                 // password_expired
            Datum::Null,                                        // created_at
            Datum::Null,                                        // updated_at
        ]);

        // Create grant row for ALL PRIVILEGES on *.*
        let grant_row = Row::new(vec![
            Datum::String("root".to_string()),                  // grantee
            Datum::String("%".to_string()),                     // grantee_host
            Datum::String("USER".to_string()),                  // grantee_type
            Datum::String(Privilege::All.to_str().to_string()), // privilege
            Datum::String("GLOBAL".to_string()),                // object_type
            Datum::Null,                                        // database_name
            Datum::Null,                                        // table_name
            Datum::Bool(true),                                  // with_grant_option
            Datum::Null,                                        // granted_by
            Datum::Null,                                        // granted_at
        ]);

        // Encode and write rows
        let user_key = encode_row_key(SYSTEM_USERS, TEST_ROW_ID.fetch_add(1, Ordering::SeqCst));
        let user_value = encode_row(&user_row);

        let grant_key = encode_row_key(SYSTEM_GRANTS, TEST_ROW_ID.fetch_add(1, Ordering::SeqCst));
        let grant_value = encode_row(&grant_row);

        storage
            .put(&user_key, &user_value)
            .await
            .expect("Failed to write user");
        storage
            .put(&grant_key, &grant_value)
            .await
            .expect("Failed to write grant");
    }
}
