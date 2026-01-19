//! RooDB database initialization binary
//!
//! Usage: roodb_init [data_dir]
//!
//! Exit codes:
//!   0 - Success (initialized or already initialized)
//!   2 - No password configured
//!   3 - Storage error

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;

use roodb::init::{initialize_database, InitConfig, InitError};
use roodb::io::default_io_factory;
use roodb::storage::schema_version::is_initialized;
use roodb::storage::{LsmConfig, LsmEngine, StorageEngine};

#[derive(Parser)]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(about = "Initialize RooDB database")]
struct Cli {
    #[arg(long, default_value = "./data", env = "ROODB_DATA_DIR")]
    data_dir: PathBuf,
}

#[tokio::main]
async fn main() {
    // Parse CLI args
    let Cli { data_dir } = Cli::parse();

    eprintln!("RooDB initialization");
    eprintln!("Data directory: {}", data_dir.display());

    // Open storage engine
    let io_factory = Arc::new(default_io_factory());
    let storage_config = LsmConfig { dir: data_dir };
    let storage: Arc<dyn StorageEngine> = match LsmEngine::open(io_factory, storage_config).await {
        Ok(engine) => Arc::new(engine),
        Err(e) => {
            eprintln!("ERROR: Failed to open storage: {}", e);
            std::process::exit(3);
        }
    };

    // Check if already initialized
    match is_initialized(&storage).await {
        Ok(true) => {
            eprintln!("Database already initialized, nothing to do");
            let _ = storage.close().await;
            std::process::exit(0); // Idempotent: success if already done
        }
        Ok(false) => {
            // Continue with initialization
        }
        Err(e) => {
            eprintln!("ERROR: Failed to check initialization status: {}", e);
            let _ = storage.close().await;
            std::process::exit(3);
        }
    }

    // Read configuration from environment
    let config = InitConfig::from_env();

    // Determine password
    let password = match config.determine_password() {
        Ok(p) => p,
        Err(InitError::Config(msg)) => {
            eprintln!("ERROR: {}", msg);
            let _ = storage.close().await;
            std::process::exit(2);
        }
        Err(e) => {
            eprintln!("ERROR: {}", e);
            let _ = storage.close().await;
            std::process::exit(2);
        }
    };

    // Initialize database
    match initialize_database(&storage, &password).await {
        Ok(()) => {
            eprintln!("Database initialized successfully");
            eprintln!("Root user 'root'@'%' created with ALL PRIVILEGES");
        }
        Err(e) => {
            eprintln!("ERROR: Failed to initialize database: {}", e);
            let _ = storage.close().await;
            std::process::exit(3);
        }
    }

    // Close storage
    if let Err(e) = storage.close().await {
        eprintln!("WARNING: Failed to close storage cleanly: {}", e);
    }

    std::process::exit(0);
}
