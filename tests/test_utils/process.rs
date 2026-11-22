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
    /// Shared by the nodes of one cluster: they trust one CA.
    certs: std::sync::Arc<RaftCertFiles>,
    /// The Raft address of a member to join, for a node that starts empty.
    join: Option<String>,
    port: u16,
    child: Option<Child>,
    /// Extra environment for the server, e.g. `ROODB_DURABILITY`.
    env: Vec<(String, String)>,
}

/// Ports already handed to a server of this test process. A port is only
/// probed, not held, between being chosen and the server binding it, so two
/// tests starting at once could be given the same one; each would then take
/// the other's server for its own.
static TAKEN_PORTS: std::sync::Mutex<Vec<u16>> = std::sync::Mutex::new(Vec::new());

/// Find a port whose Raft companion port (port + 1000) is free too, and
/// that no other server of this process has been given.
///
/// The ports come from below the range the system hands out by itself.
/// Other tests in this process listen on ports the system picks for them;
/// if a server's port came from that range too, such a test could be given
/// the port while the server is down between a kill and a restart, and the
/// restarted server's test would then be talking to a stranger.
fn free_port() -> u16 {
    const FIRST: u16 = 20_000;
    const COUNT: u16 = 8_000;
    let start = (std::process::id() % u32::from(COUNT)) as u16;
    let mut taken = TAKEN_PORTS.lock().unwrap();
    for step in 0..COUNT {
        let port = FIRST + (start + step) % COUNT;
        let clash = taken
            .iter()
            .any(|&t| t == port || t + 1000 == port || t == port + 1000);
        if clash {
            continue;
        }
        let free = std::net::TcpListener::bind(("127.0.0.1", port)).is_ok()
            && std::net::TcpListener::bind(("127.0.0.1", port + 1000)).is_ok();
        if free {
            taken.push(port);
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
            certs: std::sync::Arc::new(certs),
            join: None,
            port: free_port(),
            child: None,
            env: env
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    /// A further node for this server's cluster: an empty data directory,
    /// no `roodb_init`, started with `--join` pointing at this server. Not
    /// started yet.
    pub fn joiner(&self) -> Self {
        Self {
            data_dir: TempDir::new().expect("data dir"),
            certs: self.certs.clone(),
            join: Some(self.raft_addr()),
            port: free_port(),
            child: None,
            env: self.env.clone(),
        }
    }

    /// A node that takes this one's place after it lost its data: the same
    /// addresses, an empty data directory, started with `--join` pointing
    /// at `member`. Not started yet. (This server must not be running.)
    pub fn replacement(&self, member: &ServerProcess) -> Self {
        assert!(
            self.child.is_none(),
            "the server it replaces is still running"
        );
        Self {
            data_dir: TempDir::new().expect("data dir"),
            certs: self.certs.clone(),
            join: Some(member.raft_addr()),
            port: self.port,
            child: None,
            env: self.env.clone(),
        }
    }

    /// Where other nodes reach this one.
    pub fn raft_addr(&self) -> String {
        format!("127.0.0.1:{}", self.port + 1000)
    }

    /// Where the server's log output goes.
    fn log_path(&self) -> std::path::PathBuf {
        self.data_dir.path().join("server.log")
    }

    /// Start the server process, without waiting for anything.
    fn spawn(&self) -> Child {
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
            .args(
                self.join
                    .iter()
                    .flat_map(|member| ["--join", member.as_str()]),
            )
            // The server logs to standard output; panics and fatal errors go
            // to standard error. Both are wanted when a test fails.
            .stdout(log.try_clone().expect("clone log handle"))
            .stderr(log);
        for (key, value) in &self.env {
            command.env(key, value);
        }
        command.spawn().expect("spawn roodb")
    }

    /// Start a server that is expected to refuse to run, wait for it to
    /// exit, and return what it logged.
    pub fn start_expecting_exit(&mut self) -> String {
        assert!(self.child.is_none(), "server already running");
        // Kept, so that it is killed with this value if it does not exit.
        self.child = Some(self.spawn());
        let start = Instant::now();
        while !self.has_exited() {
            assert!(
                start.elapsed() < STARTUP_TIMEOUT * 2,
                "the server is still running:\n{}",
                self.log_tail()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        self.child = None;
        self.log_tail()
    }

    /// Start the server and wait until it accepts connections.
    pub fn start(&mut self) {
        assert!(self.child.is_none(), "server already running");
        let mut child = self.spawn();

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
        // Something answered on the port. If our server has exited, it was
        // not ours.
        if let Some(status) = child.try_wait().expect("poll server") {
            panic!(
                "server exited during startup with {status}:\n{}",
                self.log_tail()
            );
        }
        self.child = Some(child);
    }

    /// The last part of the server's log, for failure messages.
    pub fn log_tail(&self) -> String {
        let log = std::fs::read_to_string(self.log_path()).unwrap_or_default();
        let start = log.len().saturating_sub(4000);
        log[log.ceil_char_boundary(start)..].to_string()
    }

    /// Names of the segment files of the server's log.
    pub fn log_segments(&self) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(self.data_dir.path().join("raft"))
            .map(|entries| {
                entries
                    .filter_map(|entry| entry.ok())
                    .map(|entry| entry.file_name().to_string_lossy().into_owned())
                    .filter(|name| name.starts_with("log-"))
                    .collect()
            })
            .unwrap_or_default();
        names.sort();
        names
    }

    /// Whether the server has exited by itself.
    pub fn has_exited(&mut self) -> bool {
        match self.child.as_mut() {
            Some(child) => child.try_wait().expect("poll server").is_some(),
            None => true,
        }
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
