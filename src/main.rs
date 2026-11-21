//! RooDB server binary

use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use parking_lot::RwLock;
use tracing_subscriber::EnvFilter;

use roodb::catalog::Catalog;
use roodb::io::{IoBackend, IoEngine};
use roodb::raft::{Durability, LogStore, LogStoreConfig, RaftNode};
use roodb::server::listener::RooDbServer;
use roodb::storage::schema_version::is_initialized;
use roodb::storage::{set_node_id, LsmConfig, LsmEngine, StorageEngine};
use roodb::tls::{RaftTlsConfig, TlsConfig};

#[derive(Parser)]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(about = "RooDB distributed SQL database server")]
struct Cli {
    /// Address to listen on (client and Raft ports)
    #[arg(long, default_value = "0.0.0.0", env = "ROODB_BIND")]
    bind: IpAddr,
    #[arg(long, default_value = "3307", env = "ROODB_PORT")]
    port: u16,
    /// Also accept TDS (SQL Server protocol) clients on this port
    #[arg(long, env = "ROODB_TDS_PORT")]
    tds_port: Option<u16>,
    #[arg(long, default_value = "./data", env = "ROODB_DATA_DIR")]
    data_dir: PathBuf,
    /// File I/O backend: auto (io_uring where available), uring, or posix
    #[arg(long, default_value = "auto", env = "ROODB_IO_BACKEND")]
    io_backend: IoBackend,
    /// When a commit is acknowledged: os (once written; survives a server
    /// crash) or fsync (once flushed to stable storage; survives power loss)
    #[arg(long, default_value = "os", env = "ROODB_DURABILITY")]
    durability: Durability,
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
        bind,
        port,
        tds_port,
        data_dir,
        io_backend,
        durability,
        cert_path,
        key_path,
        raft_ca_cert_path,
    } = Cli::parse();

    tracing::info!(port, ?data_dir, "Starting RooDB");

    // Raft RPC uses port + 1000 by convention
    let raft_port = port
        .checked_add(1000)
        .ok_or("--port is too high: the Raft port is port + 1000")?;
    if let Some(tds_port) = tds_port {
        if tds_port == port || tds_port == raft_port {
            return Err(format!(
                "--tds-port {} collides with the client port {} or the Raft port {}",
                tds_port, port, raft_port
            )
            .into());
        }
    }

    // Load TLS config for client connections (no client cert auth)
    let tls_config = TlsConfig::from_files(&cert_path, &key_path).await?;

    // Load Raft TLS config with mTLS (mutual cert auth)
    let raft_tls_config =
        RaftTlsConfig::from_files_with_ca(&cert_path, &key_path, &raft_ca_cert_path).await?;

    // Initialize storage engine
    let io = IoEngine::new(io_backend)?;
    tracing::info!(backend = %io.backend(), "I/O engine started");
    let storage_config = LsmConfig {
        dir: data_dir.clone(),
        ..Default::default()
    };
    let storage: Arc<dyn StorageEngine> =
        Arc::new(LsmEngine::open(io.clone(), storage_config).await?);

    // Check if database is initialized
    if !is_initialized(&storage).await? {
        eprintln!("ERROR: Database not initialized. Run 'roodb_init' first.");
        std::process::exit(1);
    }

    // Initialize catalog with system tables
    let catalog = Arc::new(RwLock::new(Catalog::with_system_tables()));

    // The Raft log: the server's write-ahead log.
    let log_store = LogStore::open(
        io.clone(),
        LogStoreConfig::for_data_dir(&data_dir, durability),
    )
    .await?;
    tracing::info!(%durability, "Raft log opened");

    // Initialize Raft node (single-node mode)
    let raft_addr = SocketAddr::new(bind, raft_port);

    let node_id = 1;
    let mut raft_node = RaftNode::new(
        node_id,
        raft_addr,
        raft_tls_config,
        storage.clone(),
        catalog.clone(),
        log_store,
        true,
    )
    .await?;

    // Set node ID for globally unique row ID generation
    set_node_id(node_id);

    raft_node.start_rpc_server().await?;
    raft_node.bootstrap_single_node().await?;
    let raft_node = Arc::new(raft_node);

    tracing::info!(raft_port, "Raft node bootstrapped (single-node mode)");

    // Start RooDB server
    let addr = SocketAddr::new(bind, port);
    let mut server = RooDbServer::new(addr, tls_config, storage, catalog, raft_node);
    if let Some(tds_port) = tds_port {
        server = server.enable_tds(SocketAddr::new(bind, tds_port));
    }
    server.run().await?;

    Ok(())
}
