//! RooDB server binary

use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::RwLock;

use roodb::catalog::Catalog;
use roodb::io::default_io_factory;
use roodb::raft::RaftNode;
use roodb::server::listener::RooDbServer;
use roodb::storage::{LsmConfig, LsmEngine, StorageEngine};
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

    let port: u16 = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(3307);

    let data_dir = args
        .get(2)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("./data"));

    let cert_path = args
        .get(3)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("./certs/server.crt"));

    let key_path = args
        .get(4)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("./certs/server.key"));

    tracing::info!(port, ?data_dir, "Starting RooDB");

    // Load TLS config
    let tls_config = TlsConfig::from_files(&cert_path, &key_path).await?;

    // Initialize storage engine
    let io_factory = Arc::new(default_io_factory());
    let storage_config = LsmConfig { dir: data_dir };
    let storage: Arc<dyn StorageEngine> =
        Arc::new(LsmEngine::open(io_factory, storage_config).await?);

    // Initialize catalog with system tables
    let catalog = Arc::new(RwLock::new(Catalog::with_system_tables()));

    // Initialize Raft node (single-node mode)
    // Raft RPC uses port + 1000 by convention
    let raft_port = port + 1000;
    let raft_addr: SocketAddr = format!("0.0.0.0:{}", raft_port).parse()?;

    let mut raft_node =
        RaftNode::new(1, raft_addr, tls_config.clone(), storage.clone()).await?;
    raft_node.start_rpc_server().await?;
    raft_node.bootstrap_single_node().await?;
    let raft_node = Arc::new(raft_node);

    tracing::info!(raft_port, "Raft node bootstrapped (single-node mode)");

    // Start RooDB server
    let addr: SocketAddr = format!("0.0.0.0:{}", port).parse()?;
    let server = RooDbServer::new(addr, tls_config, storage, catalog, raft_node);
    server.run().await?;

    Ok(())
}
