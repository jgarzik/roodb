//! TDS (SQL Server protocol) tests, driven by the tiberius client.
//!
//! Every scenario runs over both transports the server accepts: TDS 7.x
//! (PRELOGIN in cleartext, TLS tunnelled, `EncryptionLevel::Required`)
//! and TDS 8.0 (TLS first, `EncryptionLevel::Strict`).

mod login;
mod query;
mod raw;
mod requests;
mod rpc;
mod types;

use tiberius::{AuthMethod, Client, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::roodb_suite::harness::TestServer;

pub type TdsClient = Client<Compat<TcpStream>>;

/// The two transports, with a short name for test table names.
pub const TRANSPORTS: [(EncryptionLevel, &str); 2] = [
    (EncryptionLevel::Required, "tds7"),
    (EncryptionLevel::Strict, "tds8"),
];

/// Connect to the shared test server over TDS.
pub async fn connect_as(
    server: &TestServer,
    encryption: EncryptionLevel,
    user: &str,
    password: &str,
) -> tiberius::Result<TdsClient> {
    let mut config = Config::new();
    config.host("127.0.0.1");
    config.port(server.tds_port());
    config.authentication(AuthMethod::sql_server(user, password));
    config.encryption(encryption);
    config.trust_cert();

    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;
    Client::connect(config, tcp.compat_write()).await
}

/// Connect as root (empty password on the test server).
pub async fn connect(server: &TestServer, encryption: EncryptionLevel) -> TdsClient {
    connect_as(server, encryption, "root", "")
        .await
        .unwrap_or_else(|e| panic!("TDS login over {:?} failed: {e}", encryption))
}
