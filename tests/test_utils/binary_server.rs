//! Binary-based test server for true E2E testing
//!
//! Spawns actual `roodb_init` and `roodb` binaries for integration tests.

use std::process::{Child, Command, Stdio};
use std::sync::OnceLock;
use std::time::Duration;

use mysql_async::{Conn, Opts, OptsBuilder, Pool, SslOpts};
use tempfile::TempDir;

use super::certs::{write_raft_cluster_certs, RaftCertFiles};

const TEST_PORT: u16 = 13307;
const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(100);

static BINARY_SERVER: OnceLock<BinaryServer> = OnceLock::new();

/// Shared binary server instance for all tests
pub struct BinaryServer {
    port: u16,
    opts: Opts,
    _data_dir: TempDir,
    _raft_cert_files: RaftCertFiles,
    _server_process: Child,
}

impl BinaryServer {
    /// Get or initialize the global binary server instance (synchronous init)
    pub fn global() -> &'static Self {
        BINARY_SERVER.get_or_init(|| Self::spawn_blocking())
    }

    /// Get a new connection (creates fresh pool per-call to avoid cross-runtime issues)
    pub async fn connect() -> Conn {
        let server = Self::global();
        let pool = Pool::new(server.opts.clone());
        pool.get_conn()
            .await
            .expect("Failed to connect to binary server")
    }

    /// Get the server port
    pub fn port() -> u16 {
        Self::global().port
    }

    /// Kill any existing server on our port
    fn kill_existing_server() {
        // Try to kill any process using our port (Linux/macOS)
        #[cfg(unix)]
        {
            let _ = Command::new("fuser")
                .args(["-k", &format!("{}/tcp", TEST_PORT)])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    fn spawn_blocking() -> Self {
        // Kill any existing server on our port first
        Self::kill_existing_server();

        // Initialize tracing (only first call succeeds)
        let _ = tracing_subscriber::fmt()
            .with_env_filter("roodb=warn")
            .try_init();

        // Create temp directories
        let data_dir = TempDir::new().expect("Failed to create data temp dir");
        let raft_cert_files = write_raft_cluster_certs();

        // Run roodb_init
        let init_status = Command::new(env!("CARGO_BIN_EXE_roodb_init"))
            .arg(data_dir.path())
            .env("ROODB_ROOT_PASSWORD", "")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .status()
            .expect("Failed to run roodb_init");

        if !init_status.success() {
            panic!("roodb_init failed with status: {}", init_status);
        }

        // Spawn roodb server with mTLS certs (use node1 cert for single-node mode)
        let server_process = Command::new(env!("CARGO_BIN_EXE_roodb"))
            .arg(TEST_PORT.to_string())
            .arg(data_dir.path())
            .arg(&raft_cert_files.node1_cert_path)
            .arg(&raft_cert_files.node1_key_path)
            .arg(&raft_cert_files.ca_cert_path)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("Failed to spawn roodb server");

        // Build connection options
        let ssl_opts = SslOpts::default().with_danger_accept_invalid_certs(true);
        let opts: Opts = OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(TEST_PORT)
            .user(Some("root"))
            .ssl_opts(ssl_opts)
            .max_allowed_packet(Some(16_777_216))
            .wait_timeout(Some(28800))
            .into();

        // Poll for readiness (blocking)
        let start = std::time::Instant::now();
        loop {
            match std::net::TcpStream::connect(("127.0.0.1", TEST_PORT)) {
                Ok(_) => break,
                Err(_) if start.elapsed() < STARTUP_TIMEOUT => {
                    std::thread::sleep(POLL_INTERVAL);
                }
                Err(e) => {
                    panic!("Server failed to start within {:?}: {}", STARTUP_TIMEOUT, e);
                }
            }
        }

        BinaryServer {
            port: TEST_PORT,
            opts,
            _data_dir: data_dir,
            _raft_cert_files: raft_cert_files,
            _server_process: server_process,
        }
    }
}

impl Drop for BinaryServer {
    fn drop(&mut self) {
        // Note: Child process is killed when _server_process is dropped
        // TempDirs are cleaned up automatically
    }
}
