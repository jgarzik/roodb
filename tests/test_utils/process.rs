//! A real server process on its own data directory, for tests that need to
//! stop the server - cleanly or by killing it - and start it again.

use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use mysql_async::{Opts, OptsBuilder, SslOpts};
use tempfile::TempDir;

use super::certs::{write_raft_cluster_certs, RaftCertFiles};

const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);

/// A `roodb` server under test.
pub struct ServerProcess {
    data_dir: TempDir,
    certs: RaftCertFiles,
    port: u16,
    child: Option<Child>,
    /// Extra environment for the server, e.g. `ROODB_DURABILITY`.
    env: Vec<(String, String)>,
}

/// Find a port whose Raft companion port (port + 1000) is free too.
fn free_port() -> u16 {
    for _ in 0..200 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("addr").port();
        if port > 64_000 {
            continue;
        }
        if std::net::TcpListener::bind(("127.0.0.1", port + 1000)).is_ok() {
            return port;
        }
    }
    panic!("no free port pair found");
}

impl ServerProcess {
    /// Initialise a fresh data directory with `roodb_init`. The server is not
    /// started yet.
    pub fn init(env: &[(&str, &str)]) -> Self {
        let data_dir = TempDir::new().expect("data dir");
        let certs = write_raft_cluster_certs();
        let status = Command::new(env!("CARGO_BIN_EXE_roodb_init"))
            .arg("--data-dir")
            .arg(data_dir.path())
            .env("ROODB_ROOT_PASSWORD", "")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("run roodb_init");
        assert!(status.success(), "roodb_init failed: {status}");
        Self {
            data_dir,
            certs,
            port: free_port(),
            child: None,
            env: env
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    /// Where the server's log output goes.
    fn log_path(&self) -> std::path::PathBuf {
        self.certs.ca_cert_path.with_file_name("server.log")
    }

    /// Start the server and wait until it accepts connections.
    pub fn start(&mut self) {
        assert!(self.child.is_none(), "server already running");
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.log_path())
            .expect("open server log");
        let mut command = Command::new(env!("CARGO_BIN_EXE_roodb"));
        command
            .arg("--port")
            .arg(self.port.to_string())
            .arg("--bind")
            .arg("127.0.0.1")
            .arg("--data-dir")
            .arg(self.data_dir.path())
            .arg("--cert-path")
            .arg(&self.certs.node1_cert_path)
            .arg("--key-path")
            .arg(&self.certs.node1_key_path)
            .arg("--raft-ca-cert-path")
            .arg(&self.certs.ca_cert_path)
            .stdout(Stdio::null())
            .stderr(log);
        for (key, value) in &self.env {
            command.env(key, value);
        }
        let mut child = command.spawn().expect("spawn roodb");

        let start = Instant::now();
        loop {
            if std::net::TcpStream::connect(("127.0.0.1", self.port)).is_ok() {
                break;
            }
            if let Some(status) = child.try_wait().expect("poll server") {
                panic!(
                    "server exited during startup with {status}:\n{}",
                    self.log_tail()
                );
            }
            assert!(
                start.elapsed() < STARTUP_TIMEOUT,
                "server did not start within {STARTUP_TIMEOUT:?}:\n{}",
                self.log_tail()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        self.child = Some(child);
    }

    /// The last part of the server's log, for failure messages.
    pub fn log_tail(&self) -> String {
        let log = std::fs::read_to_string(self.log_path()).unwrap_or_default();
        let start = log.len().saturating_sub(4000);
        log[log.ceil_char_boundary(start)..].to_string()
    }

    /// Kill the server without warning, as a crash or `kill -9` would.
    pub fn kill(&mut self) {
        let mut child = self.child.take().expect("server not running");
        child.kill().expect("kill server");
        child.wait().expect("reap server");
    }

    /// Ask the server to stop (SIGTERM) and wait for it to exit by itself.
    /// Returns its exit status.
    #[cfg(unix)]
    pub fn stop(&mut self) -> std::process::ExitStatus {
        let mut child = self.child.take().expect("server not running");
        // SAFETY: sends a signal to a process we started and still own.
        let rc = unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGTERM) };
        assert_eq!(rc, 0, "kill(SIGTERM) failed");
        let start = Instant::now();
        loop {
            if let Some(status) = child.try_wait().expect("poll server") {
                return status;
            }
            if start.elapsed() > STARTUP_TIMEOUT {
                let _ = child.kill();
                let _ = child.wait();
                panic!("server ignored SIGTERM:\n{}", self.log_tail());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Names of the table files in the data directory.
    pub fn table_files(&self) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(self.data_dir.path())
            .expect("read data dir")
            .map(|e| {
                e.expect("dir entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .filter(|n| n.ends_with(".sst"))
            .collect();
        names.sort();
        names
    }

    /// Connection options for the running server.
    pub fn opts(&self) -> Opts {
        OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(self.port)
            .user(Some("root"))
            .ssl_opts(SslOpts::default().with_danger_accept_invalid_certs(true))
            .max_allowed_packet(Some(16_777_216))
            .wait_timeout(Some(28800))
            .into()
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
