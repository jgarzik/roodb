//! Server lifecycle manager with guaranteed cleanup via atexit.
//!
//! Provides a singleton ServerManager that:
//! - Spawns the roodb server on first access
//! - Registers atexit handler for guaranteed cleanup
//! - Kills any existing server on the test port
//! - Works cross-platform (macOS, Linux, Windows)

use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use mysql_async::{Conn, Opts, OptsBuilder, Pool, SslOpts};
use tempfile::TempDir;

use super::certs::{write_raft_cluster_certs, RaftCertFiles};
use super::cleanup::register_cleanup_handler;

const TEST_PORT: u16 = 13307;
const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(100);
const KILL_WAIT: Duration = Duration::from_millis(500);

static SERVER_MANAGER: OnceLock<ServerManager> = OnceLock::new();

/// Singleton server manager for test lifecycle.
pub struct ServerManager {
    inner: Arc<Mutex<ServerState>>,
}

struct ServerState {
    server_process: Option<Child>,
    port: u16,
    data_dir: Option<TempDir>,
    cert_files: Option<RaftCertFiles>,
    opts: Option<Opts>,
    initialized: bool,
}

impl ServerManager {
    /// Get the global ServerManager if already initialized (no lazy init).
    pub fn try_get() -> Option<&'static Self> {
        SERVER_MANAGER.get()
    }

    /// Get the global ServerManager instance (lazy init).
    pub fn global() -> &'static Self {
        SERVER_MANAGER.get_or_init(|| {
            let manager = Self {
                inner: Arc::new(Mutex::new(ServerState {
                    server_process: None,
                    port: TEST_PORT,
                    data_dir: None,
                    cert_files: None,
                    opts: None,
                    initialized: false,
                })),
            };
            manager.initialize_once();
            manager
        })
    }

    /// Get a new connection (creates fresh pool per-call to avoid cross-runtime issues).
    pub async fn connect(&self) -> Conn {
        let opts = {
            let state = self.inner.lock().unwrap();
            state.opts.clone().expect("Server not initialized")
        };
        let pool = Pool::new(opts);
        pool.get_conn()
            .await
            .expect("Failed to connect to binary server")
    }

    /// Get the server port.
    pub fn port(&self) -> u16 {
        let state = self.inner.lock().unwrap();
        state.port
    }

    /// Initialize the server (called once on first access).
    fn initialize_once(&self) {
        let mut state = self.inner.lock().unwrap();
        if state.initialized {
            return;
        }

        // Register atexit handler for cleanup
        register_cleanup_handler();

        // Kill any existing server on our port
        Self::kill_existing_server_cross_platform(state.port);

        // Initialize tracing (only first call succeeds)
        let _ = tracing_subscriber::fmt()
            .with_env_filter("roodb=warn")
            .try_init();

        // Create temp directories
        let data_dir = TempDir::new().expect("Failed to create data temp dir");
        let cert_files = write_raft_cluster_certs();

        // Run roodb_init
        let init_status = Command::new(env!("CARGO_BIN_EXE_roodb_init"))
            .arg("--data-dir")
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
            .arg("--port")
            .arg(state.port.to_string())
            .arg("--data-dir")
            .arg(data_dir.path())
            .arg("--cert-path")
            .arg(&cert_files.node1_cert_path)
            .arg("--key-path")
            .arg(&cert_files.node1_key_path)
            .arg("--raft-ca-cert-path")
            .arg(&cert_files.ca_cert_path)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("Failed to spawn roodb server");

        // Build connection options
        let ssl_opts = SslOpts::default().with_danger_accept_invalid_certs(true);
        let opts: Opts = OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(state.port)
            .user(Some("root"))
            .ssl_opts(ssl_opts)
            .max_allowed_packet(Some(16_777_216))
            .wait_timeout(Some(28800))
            .into();

        // Poll for readiness (blocking)
        let start = std::time::Instant::now();
        loop {
            match std::net::TcpStream::connect(("127.0.0.1", state.port)) {
                Ok(_) => break,
                Err(_) if start.elapsed() < STARTUP_TIMEOUT => {
                    std::thread::sleep(POLL_INTERVAL);
                }
                Err(e) => {
                    panic!("Server failed to start within {:?}: {}", STARTUP_TIMEOUT, e);
                }
            }
        }

        state.server_process = Some(server_process);
        state.data_dir = Some(data_dir);
        state.cert_files = Some(cert_files);
        state.opts = Some(opts);
        state.initialized = true;
    }

    /// Cleanup server resources (called by atexit handler).
    pub fn cleanup(&self) {
        let mut state = self.inner.lock().unwrap();
        if let Some(ref mut child) = state.server_process {
            let _ = child.kill();
            let _ = child.wait();
        }
        state.server_process = None;
        // TempDirs are cleaned up automatically when dropped
    }

    /// Kill any existing server on the specified port (cross-platform).
    fn kill_existing_server_cross_platform(port: u16) {
        #[cfg(unix)]
        {
            // Use lsof + kill (works on both macOS and Linux)
            if let Ok(output) = Command::new("lsof")
                .args(["-ti", &format!(":{}", port)])
                .output()
            {
                let pids = String::from_utf8_lossy(&output.stdout);
                for pid in pids.split_whitespace() {
                    let _ = Command::new("kill")
                        .args(["-9", pid])
                        .stdout(Stdio::null())
                        .stderr(Stdio::null())
                        .status();
                }
            }
            std::thread::sleep(KILL_WAIT);
        }

        #[cfg(windows)]
        {
            // Use netstat + taskkill on Windows
            if let Ok(output) = Command::new("netstat").args(["-ano"]).output() {
                let output_str = String::from_utf8_lossy(&output.stdout);
                for line in output_str.lines() {
                    if line.contains(&format!(":{}", port)) && line.contains("LISTENING") {
                        // Extract PID from last column
                        if let Some(pid) = line.split_whitespace().last() {
                            let _ = Command::new("taskkill")
                                .args(["/F", "/PID", pid])
                                .stdout(Stdio::null())
                                .stderr(Stdio::null())
                                .status();
                        }
                    }
                }
            }
            std::thread::sleep(KILL_WAIT);
        }
    }
}
