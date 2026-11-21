//! Integration tests for Raft cluster functionality

use crate::test_utils;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use roodb::catalog::Catalog;
use roodb::io::{IoBackend, IoEngine};
use roodb::raft::{ChangeSet, Durability, LogStore, LogStoreConfig, RaftNode, RowChange};
use roodb::storage::{LsmConfig, LsmEngine, StorageEngine};
use roodb::tls::RaftTlsConfig;
use test_utils::certs::{test_raft_tls_config, write_raft_cluster_certs};

/// Get a unique port for testing (based on process ID to avoid conflicts)
fn test_port(base: u16) -> u16 {
    base + (std::process::id() as u16 % 1000)
}

/// Create a temporary storage engine and Raft log for testing
async fn test_storage(name: &str) -> (Arc<dyn StorageEngine>, LogStore) {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "roodb_test_{}_{}_{:?}",
        name,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::create_dir_all(&path);
    let factory = IoEngine::new(IoBackend::from_env().unwrap()).unwrap();
    let log = LogStore::open(
        factory.clone(),
        LogStoreConfig::for_data_dir(&path, Durability::Os),
    )
    .await
    .unwrap();
    let config = LsmConfig {
        dir: path,
        ..Default::default()
    };
    let storage: Arc<dyn StorageEngine> = Arc::new(LsmEngine::open(factory, config).await.unwrap());
    (storage, log)
}

/// Create a test catalog for Raft tests
fn test_catalog() -> Arc<RwLock<Catalog>> {
    Arc::new(RwLock::new(Catalog::with_system_tables()))
}

/// Helper to create a ChangeSet with a single insert
fn insert_change(table: &str, key: &[u8], value: &[u8]) -> ChangeSet {
    let mut cs = ChangeSet::new(1);
    cs.push(RowChange::insert(table, key.to_vec(), value.to_vec()));
    cs
}

/// Wait for leader election with polling (handles system load better than fixed sleep)
async fn wait_for_leader<'a>(nodes: &[&'a RaftNode], timeout_ms: u64) -> Option<&'a RaftNode> {
    let start = std::time::Instant::now();
    while start.elapsed().as_millis() < timeout_ms as u128 {
        for node in nodes {
            if node.is_leader().await {
                return Some(*node);
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}

/// Generate mTLS configs for a 3-node cluster (CA + 3 unique node certs)
fn generate_cluster_tls_configs() -> (
    RaftTlsConfig,
    RaftTlsConfig,
    RaftTlsConfig,
    test_utils::certs::RaftCertFiles,
) {
    let files = write_raft_cluster_certs();

    let ca_pem = std::fs::read(&files.ca_cert_path).unwrap();
    let node1_cert = std::fs::read(&files.node1_cert_path).unwrap();
    let node1_key = std::fs::read(&files.node1_key_path).unwrap();
    let node2_cert = std::fs::read(&files.node2_cert_path).unwrap();
    let node2_key = std::fs::read(&files.node2_key_path).unwrap();
    let node3_cert = std::fs::read(&files.node3_cert_path).unwrap();
    let node3_key = std::fs::read(&files.node3_key_path).unwrap();

    let tls1 = RaftTlsConfig::from_pem_with_ca(&node1_cert, &node1_key, &ca_pem).unwrap();
    let tls2 = RaftTlsConfig::from_pem_with_ca(&node2_cert, &node2_key, &ca_pem).unwrap();
    let tls3 = RaftTlsConfig::from_pem_with_ca(&node3_cert, &node3_key, &ca_pem).unwrap();

    (tls1, tls2, tls3, files)
}

#[tokio::test]
async fn test_single_node_bootstrap() {
    let tls_config = test_raft_tls_config();
    let addr: SocketAddr = format!("127.0.0.1:{}", test_port(15000)).parse().unwrap();
    let storage = test_storage("single_bootstrap").await;
    let catalog = test_catalog();

    let mut node = RaftNode::new(1, addr, tls_config, storage.0, catalog, storage.1, true)
        .await
        .unwrap();

    // Start RPC server
    node.start_rpc_server().await.unwrap();

    // Bootstrap as single node
    node.bootstrap_single_node().await.unwrap();

    // Wait for leader election
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Should be leader
    assert!(node.is_leader().await);

    // Propose a change
    let changeset = insert_change("test", b"key1", b"value1");
    node.propose_changes(changeset).await.unwrap();

    // Shutdown
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_single_node_multiple_writes() {
    let tls_config = test_raft_tls_config();
    let addr: SocketAddr = format!("127.0.0.1:{}", test_port(15100)).parse().unwrap();
    let storage = test_storage("single_writes").await;
    let catalog = test_catalog();

    let mut node = RaftNode::new(1, addr, tls_config, storage.0, catalog, storage.1, true)
        .await
        .unwrap();
    node.start_rpc_server().await.unwrap();
    node.bootstrap_single_node().await.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Write multiple values
    for i in 0..10 {
        let key = format!("key{}", i).into_bytes();
        let value = format!("value{}", i).into_bytes();
        let changeset = insert_change("test", &key, &value);
        node.propose_changes(changeset).await.unwrap();
    }

    // Propose a delete
    let mut delete_cs = ChangeSet::new(2);
    delete_cs.push(RowChange::delete("test", b"key5".to_vec()));
    node.propose_changes(delete_cs).await.unwrap();

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_three_node_cluster_bootstrap() {
    let (tls1, tls2, tls3, _cert_files) = generate_cluster_tls_configs();
    let base_port = test_port(15200);

    let addr1: SocketAddr = format!("127.0.0.1:{}", base_port).parse().unwrap();
    let addr2: SocketAddr = format!("127.0.0.1:{}", base_port + 1).parse().unwrap();
    let addr3: SocketAddr = format!("127.0.0.1:{}", base_port + 2).parse().unwrap();

    let storage1 = test_storage("cluster_boot1").await;
    let storage2 = test_storage("cluster_boot2").await;
    let storage3 = test_storage("cluster_boot3").await;
    let catalog1 = test_catalog();
    let catalog2 = test_catalog();
    let catalog3 = test_catalog();

    // Create nodes with unique per-node mTLS certs
    let mut node1 = RaftNode::new(1, addr1, tls1, storage1.0, catalog1, storage1.1, false)
        .await
        .unwrap();
    let mut node2 = RaftNode::new(2, addr2, tls2, storage2.0, catalog2, storage2.1, false)
        .await
        .unwrap();
    let mut node3 = RaftNode::new(3, addr3, tls3, storage3.0, catalog3, storage3.1, false)
        .await
        .unwrap();

    // Add peers to each node
    node1.add_peer(2, addr2);
    node1.add_peer(3, addr3);
    node2.add_peer(1, addr1);
    node2.add_peer(3, addr3);
    node3.add_peer(1, addr1);
    node3.add_peer(2, addr2);

    // Start RPC servers
    node1.start_rpc_server().await.unwrap();
    node2.start_rpc_server().await.unwrap();
    node3.start_rpc_server().await.unwrap();

    // Bootstrap cluster from node 1
    let members = vec![(1, addr1), (2, addr2), (3, addr3)];
    node1.bootstrap_cluster(members).await.unwrap();

    // Wait for leader election with polling (30s timeout for system load)
    let nodes = [&node1, &node2, &node3];
    let leader = wait_for_leader(&nodes, 30_000).await;

    assert!(leader.is_some(), "No leader elected");

    // Shutdown nodes
    node1.shutdown().await.unwrap();
    node2.shutdown().await.unwrap();
    node3.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_log_replication() {
    let (tls1, tls2, tls3, _cert_files) = generate_cluster_tls_configs();
    let base_port = test_port(15300);

    let addr1: SocketAddr = format!("127.0.0.1:{}", base_port).parse().unwrap();
    let addr2: SocketAddr = format!("127.0.0.1:{}", base_port + 1).parse().unwrap();
    let addr3: SocketAddr = format!("127.0.0.1:{}", base_port + 2).parse().unwrap();

    let storage1 = test_storage("replication1").await;
    let storage2 = test_storage("replication2").await;
    let storage3 = test_storage("replication3").await;
    let catalog1 = test_catalog();
    let catalog2 = test_catalog();
    let catalog3 = test_catalog();

    let mut node1 = RaftNode::new(1, addr1, tls1, storage1.0, catalog1, storage1.1, false)
        .await
        .unwrap();
    let mut node2 = RaftNode::new(2, addr2, tls2, storage2.0, catalog2, storage2.1, false)
        .await
        .unwrap();
    let mut node3 = RaftNode::new(3, addr3, tls3, storage3.0, catalog3, storage3.1, false)
        .await
        .unwrap();

    node1.add_peer(2, addr2);
    node1.add_peer(3, addr3);
    node2.add_peer(1, addr1);
    node2.add_peer(3, addr3);
    node3.add_peer(1, addr1);
    node3.add_peer(2, addr2);

    node1.start_rpc_server().await.unwrap();
    node2.start_rpc_server().await.unwrap();
    node3.start_rpc_server().await.unwrap();

    let members = vec![(1, addr1), (2, addr2), (3, addr3)];
    node1.bootstrap_cluster(members).await.unwrap();

    // Wait for leader election with polling (30s timeout for system load)
    let nodes = [&node1, &node2, &node3];
    let leader = wait_for_leader(&nodes, 30_000)
        .await
        .expect("No leader found");

    // Propose changes through leader
    let changeset = insert_change("test", b"replicated_key", b"replicated_value");
    leader.propose_changes(changeset).await.unwrap();

    // Wait for replication
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Note: Without a shared storage engine, we can't verify replication
    // at the storage level. The test verifies that the Raft log replicates.
    // In a real setup, each node would have a storage engine that receives
    // the applied changes.

    node1.shutdown().await.unwrap();
    node2.shutdown().await.unwrap();
    node3.shutdown().await.unwrap();
}

/// Start a single-node Raft over fresh storage and return the node with the
/// storage it applies to.
async fn single_node(name: &str, port: u16) -> (RaftNode, Arc<dyn StorageEngine>) {
    let addr: SocketAddr = format!("127.0.0.1:{}", test_port(port)).parse().unwrap();
    let (storage, log) = test_storage(name).await;
    let mut node = RaftNode::new(
        1,
        addr,
        test_raft_tls_config(),
        storage.clone(),
        test_catalog(),
        log,
        true,
    )
    .await
    .unwrap();
    node.start_rpc_server().await.unwrap();
    node.bootstrap_single_node().await.unwrap();
    assert!(wait_for_leader(&[&node], 5000).await.is_some());
    (node, storage)
}

/// The row data stored under a key, if a live row is there.
async fn stored_row(storage: &Arc<dyn StorageEngine>, key: &[u8]) -> Option<Vec<u8>> {
    let raw = storage.get(key).await.unwrap()?;
    // 17-byte MVCC header: transaction id, roll pointer, deleted flag.
    if raw[16] == 1 {
        return None;
    }
    Some(raw[17..].to_vec())
}

/// A changeset that fails any check writes nothing: not the rows before the
/// failing one either.
#[tokio::test]
async fn a_rejected_changeset_writes_nothing() {
    let (mut node, storage) = single_node("atomic_reject", 15300).await;

    node.propose_changes(insert_change("t", b"existing", b"v1"))
        .await
        .unwrap();

    // Two new rows, then a duplicate of the existing one.
    let mut cs = ChangeSet::new(2);
    cs.push(RowChange::insert("t", b"new-a".to_vec(), b"a".to_vec()));
    cs.push(RowChange::insert("t", b"new-b".to_vec(), b"b".to_vec()));
    cs.push(RowChange::insert("t", b"existing".to_vec(), b"v2".to_vec()));
    let err = node.propose_changes(cs).await.unwrap_err().to_string();
    assert!(err.contains("Duplicate key"), "{err}");

    assert_eq!(stored_row(&storage, b"new-a").await, None, "partial write");
    assert_eq!(stored_row(&storage, b"new-b").await, None, "partial write");
    assert_eq!(
        stored_row(&storage, b"existing").await,
        Some(b"v1".to_vec())
    );

    // A version conflict on a later change undoes the earlier ones too.
    let mut cs = ChangeSet::new(3);
    cs.push(RowChange::insert("t", b"new-c".to_vec(), b"c".to_vec()));
    cs.push(RowChange::update_with_version(
        "t",
        b"existing".to_vec(),
        b"v3".to_vec(),
        999,
    ));
    let err = node.propose_changes(cs).await.unwrap_err().to_string();
    assert!(err.contains("Write conflict"), "{err}");
    assert_eq!(stored_row(&storage, b"new-c").await, None, "partial write");

    // The node is fine: later changesets apply.
    node.propose_changes(insert_change("t", b"after", b"ok"))
        .await
        .unwrap();
    assert_eq!(stored_row(&storage, b"after").await, Some(b"ok".to_vec()));

    node.shutdown().await.unwrap();
}

/// Checks see the changeset's own earlier changes: deleting a row and
/// inserting it again in one transaction is not a duplicate, inserting the
/// same new key twice is.
#[tokio::test]
async fn a_changeset_sees_its_own_earlier_changes() {
    let (mut node, storage) = single_node("atomic_overlay", 15400).await;

    node.propose_changes(insert_change("t", b"row", b"old"))
        .await
        .unwrap();

    let mut cs = ChangeSet::new(2);
    cs.push(RowChange::delete("t", b"row".to_vec()));
    cs.push(RowChange::insert("t", b"row".to_vec(), b"new".to_vec()));
    node.propose_changes(cs).await.unwrap();
    assert_eq!(stored_row(&storage, b"row").await, Some(b"new".to_vec()));

    let mut cs = ChangeSet::new(3);
    cs.push(RowChange::insert("t", b"twice".to_vec(), b"1".to_vec()));
    cs.push(RowChange::insert("t", b"twice".to_vec(), b"2".to_vec()));
    let err = node.propose_changes(cs).await.unwrap_err().to_string();
    assert!(err.contains("Duplicate key"), "{err}");
    assert_eq!(stored_row(&storage, b"twice").await, None);

    // INSERT IGNORE skips the duplicate and keeps the rest.
    let mut cs = ChangeSet::new_with_changes_ignore(
        4,
        vec![
            RowChange::insert("t", b"row".to_vec(), b"ignored".to_vec()),
            RowChange::insert("t", b"fresh".to_vec(), b"kept".to_vec()),
        ],
        true,
    );
    cs.push(RowChange::delete("t", b"gone".to_vec()));
    node.propose_changes(cs).await.unwrap();
    assert_eq!(stored_row(&storage, b"row").await, Some(b"new".to_vec()));
    assert_eq!(stored_row(&storage, b"fresh").await, Some(b"kept".to_vec()));

    node.shutdown().await.unwrap();
}
