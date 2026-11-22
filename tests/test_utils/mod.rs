//! Shared test utilities
//!
//! Note: clippy reports false-positive dead_code warnings because it can't
//! trace usage across test binaries. These utilities are used by multiple tests.

pub mod auth;
pub mod certs;
pub mod cleanup;
pub mod process;
pub mod server_manager;

/// The database the tests work in. A new server has none but the system's
/// own, so whoever starts one creates this before tests connect to it.
pub const TEST_DATABASE: &str = "test";

/// Create the tests' database on the server at `port`.
pub async fn create_test_database_on(port: u16) -> mysql_async::Result<()> {
    use mysql_async::prelude::Queryable;
    use mysql_async::{Opts, OptsBuilder, SslOpts};

    let opts: Opts = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some("root"))
        .ssl_opts(SslOpts::default().with_danger_accept_invalid_certs(true))
        .into();
    let mut conn = mysql_async::Conn::new(opts).await?;
    conn.query_drop(format!("CREATE DATABASE IF NOT EXISTS {TEST_DATABASE}"))
        .await?;
    conn.disconnect().await
}

/// As `create_test_database_on`, for a caller that is not async and a
/// server that has just started: it may take a moment before it leads and
/// can take the statement. Gives up, with the last error, after `patience`.
pub fn create_test_database(port: u16, patience: std::time::Duration) -> Result<(), String> {
    // A thread of its own, so that this also works when the caller is
    // inside a runtime.
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        runtime.block_on(async {
            let start = std::time::Instant::now();
            loop {
                match create_test_database_on(port).await {
                    Ok(()) => return Ok(()),
                    Err(e) if start.elapsed() >= patience => return Err(e.to_string()),
                    Err(_) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
                }
            }
        })
    })
    .join()
    .map_err(|_| "the thread creating the test database panicked".to_string())?
}
