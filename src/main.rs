//! RooDB server binary

use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;

use roodb::raft::RaftNode;
use roodb::server::listener::MySqlServer;
use roodb::tls::TlsConfig;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // Parse CLI args (minimal for now)
    let args: Vec<String> = env::args().collect();

    let node_id: u64 = args
        .get(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    let mysql_port: u16 = args
        .get(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(3307);

    let raft_port: u16 = args
        .get(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(5000);

    let data_dir = args
        .get(4)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("./data"));

    let cert_path = args
        .get(5)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("./certs/server.crt"));

    let key_path = args
        .get(6)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("./certs/server.key"));

    tracing::info!(
        node_id,
        mysql_port,
        raft_port,
        ?data_dir,
        "Starting RooDB"
    );

    // Load TLS config
    let tls_config = TlsConfig::from_files(&cert_path, &key_path).await?;

    // Initialize Raft node
    let raft_addr: SocketAddr = format!("127.0.0.1:{}", raft_port).parse()?;
    let raft_node = RaftNode::new(node_id, raft_addr, tls_config.clone()).await?;

    // Auto-bootstrap if single node
    raft_node.bootstrap_single_node().await?;

    // Start MySQL-compatible server
    let mysql_addr: SocketAddr = format!("0.0.0.0:{}", mysql_port).parse()?;
    let server = MySqlServer::new(mysql_addr, tls_config, raft_node);
    server.run().await?;

    Ok(())
}
