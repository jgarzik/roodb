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
    /// Join the cluster that the node at this address (host:port of its
    /// Raft port) belongs to, instead of running alone. Several may be
    /// given, separated by commas. Used on an empty data directory; a node
    /// that has joined once remembers its cluster.
    #[arg(long, value_delimiter = ',', env = "ROODB_JOIN")]
    join: Vec<String>,
    /// Port for traffic between the nodes of a cluster (default: port + 1000)
    #[arg(long, env = "ROODB_RAFT_PORT")]
    raft_port: Option<u16>,
    /// Host name or address at which the other nodes reach this one
    /// (default: the bind address)
    #[arg(long, env = "ROODB_ADVERTISE_HOST")]
    advertise_host: Option<String>,
}

/// The file in the data directory that holds this node's ID.
const NODE_ID_FILE: &str = "NODE_ID";

/// The node ID recorded in the data directory, if there is one.
fn stored_node_id(data_dir: &std::path::Path) -> Result<Option<u64>, Box<dyn std::error::Error>> {
    match std::fs::read_to_string(data_dir.join(NODE_ID_FILE)) {
        Ok(text) => Ok(Some(text.trim().parse().map_err(|_| {
            format!("{NODE_ID_FILE} in the data directory does not hold a node ID")
        })?)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Record this node's ID in the data directory, durably: a node must never
/// come back under another identity.
fn store_node_id(data_dir: &std::path::Path, id: u64) -> std::io::Result<()> {
    use std::io::Write;
    let path = data_dir.join(NODE_ID_FILE);
    let temporary = data_dir.join(format!("{NODE_ID_FILE}.tmp"));
    let mut file = std::fs::File::create(&temporary)?;
    writeln!(file, "{id}")?;
    file.sync_all()?;
    std::fs::rename(&temporary, &path)?;
    std::fs::File::open(data_dir)?.sync_all()
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
        join,
        raft_port,
        advertise_host,
    } = Cli::parse();

    tracing::info!(port, ?data_dir, "Starting RooDB");

    // Raft RPC uses port + 1000 unless told otherwise
    let raft_port = match raft_port {
        Some(raft_port) => raft_port,
        None => port
            .checked_add(1000)
            .ok_or("--port is too high: the Raft port is port + 1000")?,
    };
    if raft_port == port {
        return Err("--raft-port must differ from --port".into());
    }
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

    // Who this node is. The first node of a cluster is initialised by
    // roodb_init and is node 1. Any other starts with nothing, asks the
    // cluster named by --join to admit it, and is told its ID. Either way
    // the ID is recorded, and a restart goes by the record.
    let advertised_raft_addr = format!(
        "{}:{raft_port}",
        advertise_host.unwrap_or_else(|| bind.to_string())
    );
    let initialized = is_initialized(&storage).await?;
    let (node_id, joining) = match stored_node_id(&data_dir)? {
        Some(id) => (id, false),
        None if join.is_empty() => {
            if !initialized {
                eprintln!(
                    "ERROR: Database not initialized. Run 'roodb_init' first, \
                     or give --join to join an existing cluster."
                );
                std::process::exit(1);
            }
            store_node_id(&data_dir, 1)?;
            (1, false)
        }
        None => {
            if initialized {
                return Err(
                    "--join was given, but the data directory already holds a database \
                     of its own; a node joins with an empty data directory"
                        .into(),
                );
            }
            let id = RaftNode::request_membership(&raft_tls_config, &join, &advertised_raft_addr)
                .await?;
            store_node_id(&data_dir, id)?;
            tracing::info!(node_id = id, "Admitted to the cluster");
            (id, true)
        }
    };

    // Initialize catalog with system tables
    let catalog = Arc::new(RwLock::new(Catalog::with_system_tables()));

    // The Raft log: the server's write-ahead log.
    let log_store = LogStore::open(
        io.clone(),
        LogStoreConfig::for_data_dir(&data_dir, durability),
    )
    .await?;
    tracing::info!(%durability, "Raft log opened");

    // The Raft node. Node 1 starts as the only member of its cluster
    // unless its membership says otherwise; a node that joined is one of
    // several from the start.
    let raft_addr = SocketAddr::new(bind, raft_port);
    let started_alone = node_id == 1 && !joining;
    let mut raft_node = RaftNode::new(
        node_id,
        raft_addr,
        raft_tls_config,
        storage.clone(),
        catalog.clone(),
        log_store,
        started_alone,
    )
    .await?;
    raft_node.set_advertised_addr(advertised_raft_addr);

    // Set node ID for globally unique row ID generation
    set_node_id(node_id);

    raft_node.start_rpc_server().await?;
    if raft_node.is_alone() {
        raft_node.bootstrap_single_node().await?;
        tracing::info!(raft_port, node_id, "Running as a single-node cluster");
    } else {
        raft_node.wait_until_ready().await?;
        tracing::info!(raft_port, node_id, "Running as a member of a cluster");
    }
    let raft_node = Arc::new(raft_node);

    // Start RooDB server
    let addr = SocketAddr::new(bind, port);
    let mut server = RooDbServer::new(
        addr,
        tls_config,
        storage.clone(),
        catalog,
        raft_node.clone(),
    );
    if let Some(tds_port) = tds_port {
        server = server.enable_tds(SocketAddr::new(bind, tds_port));
    }
    server.run_until(shutdown_signal()).await?;

    // Asked to stop. Nothing here is needed for correctness - a killed
    // server recovers from its log - but stopping in order writes memory out
    // to tables, so the next start has next to nothing to replay.
    tracing::info!("Shutting down");
    // Flushing can take a while. A second request means "now": everything
    // acknowledged is already in the log, so exiting here loses nothing.
    tokio::spawn(async {
        shutdown_signal().await;
        tracing::warn!("Second stop request: exiting without flushing");
        std::process::exit(1);
    });
    if let Err(e) = raft_node.shutdown().await {
        tracing::warn!(error = %e, "Raft shutdown reported an error");
    }
    storage.close().await?;
    tracing::info!("Shutdown complete");

    Ok(())
}

/// Completes when the process is asked to stop: SIGINT or SIGTERM on Unix,
/// Ctrl-C elsewhere.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let (Ok(mut interrupt), Ok(mut terminate)) = (
            signal(SignalKind::interrupt()),
            signal(SignalKind::terminate()),
        ) else {
            tracing::warn!("cannot listen for termination signals; stop with kill");
            return std::future::pending().await;
        };
        tokio::select! {
            _ = interrupt.recv() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        if tokio::signal::ctrl_c().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}
