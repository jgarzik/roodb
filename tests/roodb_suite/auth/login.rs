//! Login authentication tests

use crate::roodb_suite::harness::TestServer;

/// Test successful login with correct credentials
#[tokio::test]
async fn test_login_success() {
    let server = TestServer::start("auth_login_success").await;

    // Connection should succeed with empty password (default for test server)
    let conn = server.connect().await;
    drop(conn);

    server.shutdown().await;
}

/// Test login with password
#[tokio::test]
async fn test_login_with_password() {
    use mysql_async::{Opts, OptsBuilder, Pool, SslOpts};

    let server = TestServer::start_with_password("auth_login_password", "secret123").await;

    // Build client with correct password
    let ssl_opts = SslOpts::default().with_danger_accept_invalid_certs(true);
    let opts: Opts = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(server.port())
        .user(Some("root"))
        .pass(Some("secret123"))
        .ssl_opts(ssl_opts)
        .max_allowed_packet(Some(16_777_216))
        .wait_timeout(Some(28800))
        .into();

    let pool = Pool::new(opts);
    let conn = pool
        .get_conn()
        .await
        .expect("Login with correct password should succeed");
    drop(conn);
    pool.disconnect().await.unwrap();

    server.shutdown().await;
}

/// Test login with wrong password fails
#[tokio::test]
async fn test_login_wrong_password() {
    use mysql_async::{Opts, OptsBuilder, Pool, SslOpts};

    let server = TestServer::start_with_password("auth_wrong_password", "correctpassword").await;

    // Build client with wrong password
    let ssl_opts = SslOpts::default().with_danger_accept_invalid_certs(true);
    let opts: Opts = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(server.port())
        .user(Some("root"))
        .pass(Some("wrongpassword"))
        .ssl_opts(ssl_opts)
        .max_allowed_packet(Some(16_777_216))
        .wait_timeout(Some(28800))
        .into();

    let pool = Pool::new(opts);
    let result = pool.get_conn().await;

    // Login should fail
    assert!(result.is_err(), "Login with wrong password should fail");

    pool.disconnect().await.unwrap();
    server.shutdown().await;
}

/// Test login with unknown user fails
#[tokio::test]
async fn test_login_unknown_user() {
    use mysql_async::{Opts, OptsBuilder, Pool, SslOpts};

    let server = TestServer::start("auth_unknown_user").await;

    // Build client with unknown user
    let ssl_opts = SslOpts::default().with_danger_accept_invalid_certs(true);
    let opts: Opts = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(server.port())
        .user(Some("unknownuser"))
        .ssl_opts(ssl_opts)
        .max_allowed_packet(Some(16_777_216))
        .wait_timeout(Some(28800))
        .into();

    let pool = Pool::new(opts);
    let result = pool.get_conn().await;

    // Login should fail
    assert!(result.is_err(), "Login with unknown user should fail");

    pool.disconnect().await.unwrap();
    server.shutdown().await;
}
