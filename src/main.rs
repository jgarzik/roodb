//! RooDB server binary

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use std::sync::Arc;

use parking_lot::RwLock;

use roodb::catalog::Catalog;
use roodb::io::default_io_factory;
use roodb::raft::RaftNode;
use roodb::server::listener::RooDbServer;
use roodb::storage::schema_version::is_initialized;
use roodb::storage::{set_node_id, LsmConfig, LsmEngine, StorageEngine};
use roodb::tls::{RaftTlsConfig, TlsConfig};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(about = "RooDB distributed SQL database server")]
struct Cli {
    #[arg(long, default_value = "3307", env = "ROODB_PORT")]
    port: u16,
    #[arg(long, default_value = "./data", env = "ROODB_DATA_DIR")]
    data_dir: PathBuf,
    #[arg(long, default_value = "./certs/server.crt", env = "ROODB_CERT_PATH")]
    cert_path: PathBuf,
    #[arg(long, default_value = "./certs/server.key", env = "ROODB_KEY_PATH")]
    key_path: PathBuf,
    #[arg(long, default_value = "./certs/ca.crt", env = "ROODB_CA_CERT_PATH")]
    raft_ca_cert_path: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize rustls crypto provider
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // Parse CLI args
    let Cli {
        port,
        data_dir,
        cert_path,
        key_path,
        raft_ca_cert_path,
    } = Cli::parse();

    tracing::info!(port, ?data_dir, "Starting RooDB");

    // Load TLS config for client connections (no client cert auth)
    let tls_config = TlsConfig::from_files(&cert_path, &key_path).await?;

    // Load Raft TLS config with mTLS (mutual cert auth)
    let raft_tls_config =
        RaftTlsConfig::from_files_with_ca(&cert_path, &key_path, &raft_ca_cert_path).await?;

    // Initialize storage engine
    let io_factory = Arc::new(default_io_factory());
    let storage_config = LsmConfig { dir: data_dir };
    let storage: Arc<dyn StorageEngine> =
        Arc::new(LsmEngine::open(io_factory, storage_config).await?);

    // Check if database is initialized
    if !is_initialized(&storage).await? {
        eprintln!("ERROR: Database not initialized. Run 'roodb_init' first.");
        std::process::exit(1);
    }

    // Initialize catalog with system tables
    let catalog = Arc::new(RwLock::new(Catalog::with_system_tables()));

    // Initialize Raft node (single-node mode)
    // Raft RPC uses port + 1000 by convention
    let raft_port = port + 1000;
    let raft_addr: SocketAddr = format!("0.0.0.0:{}", raft_port).parse()?;

    let node_id = 1;
    let mut raft_node = RaftNode::new(
        node_id,
        raft_addr,
        raft_tls_config,
        storage.clone(),
        catalog.clone(),
    )
    .await?;

    // Set node ID for globally unique row ID generation
    set_node_id(node_id);

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
