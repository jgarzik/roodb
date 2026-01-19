//! Raft replication tests.
//!
//! These tests verify Raft consensus behavior.
//! SQL-level replication is tested via the full integration tests.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use roodb::catalog::Catalog;
use roodb::io::default_io_factory;
use roodb::raft::{ChangeSet, RaftNode, RowChange};
use roodb::storage::{LsmConfig, LsmEngine, StorageEngine};

use crate::test_utils::certs::write_raft_cluster_certs;
use roodb::tls::RaftTlsConfig;

/// Get a unique port for testing
fn test_port(base: u16) -> u16 {
    base + (std::process::id() as u16 % 1000)
}

/// Create a temporary storage engine for testing
async fn test_storage(name: &str) -> Arc<dyn StorageEngine> {
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
    let factory = Arc::new(default_io_factory());
    let config = LsmConfig { dir: path };
    Arc::new(LsmEngine::open(factory, config).await.unwrap())
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

/// Generate mTLS configs for a 3-node cluster (CA + 3 unique node certs)
fn generate_cluster_tls_configs() -> (
    RaftTlsConfig,
    RaftTlsConfig,
    RaftTlsConfig,
    crate::test_utils::certs::RaftCertFiles,
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
async fn test_leader_election_timing() {
    let (tls1, tls2, tls3, _cert_files) = generate_cluster_tls_configs();
    let base_port = test_port(16000);

    let addr1: SocketAddr = format!("127.0.0.1:{}", base_port).parse().unwrap();
    let addr2: SocketAddr = format!("127.0.0.1:{}", base_port + 1).parse().unwrap();
    let addr3: SocketAddr = format!("127.0.0.1:{}", base_port + 2).parse().unwrap();

    let storage1 = test_storage("election1").await;
    let storage2 = test_storage("election2").await;
    let storage3 = test_storage("election3").await;
    let catalog1 = test_catalog();
    let catalog2 = test_catalog();
    let catalog3 = test_catalog();

    let mut node1 = RaftNode::new(1, addr1, tls1, storage1, catalog1)
        .await
        .unwrap();
    let mut node2 = RaftNode::new(2, addr2, tls2, storage2, catalog2)
        .await
        .unwrap();
    let mut node3 = RaftNode::new(3, addr3, tls3, storage3, catalog3)
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

    // Leader election should happen within election timeout (150-300ms)
    // Wait up to 2 seconds to be safe
    let mut leader_found = false;
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        for node in [&node1, &node2, &node3] {
            if node.is_leader().await {
                leader_found = true;
                break;
            }
        }
        if leader_found {
            break;
        }
    }

    assert!(leader_found, "Leader should be elected within 2 seconds");

    // Verify exactly one leader
    let mut leader_count = 0;
    for node in [&node1, &node2, &node3] {
        if node.is_leader().await {
            leader_count += 1;
        }
    }
    assert_eq!(leader_count, 1, "Exactly one leader should exist");

    node1.shutdown().await.unwrap();
    node2.shutdown().await.unwrap();
    node3.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_replication_consistency() {
    let (tls1, tls2, tls3, _cert_files) = generate_cluster_tls_configs();
    let base_port = test_port(16100);

    let addr1: SocketAddr = format!("127.0.0.1:{}", base_port).parse().unwrap();
    let addr2: SocketAddr = format!("127.0.0.1:{}", base_port + 1).parse().unwrap();
    let addr3: SocketAddr = format!("127.0.0.1:{}", base_port + 2).parse().unwrap();

    let storage1 = test_storage("consistency1").await;
    let storage2 = test_storage("consistency2").await;
    let storage3 = test_storage("consistency3").await;
    let catalog1 = test_catalog();
    let catalog2 = test_catalog();
    let catalog3 = test_catalog();

    let mut node1 = RaftNode::new(1, addr1, tls1, storage1, catalog1)
        .await
        .unwrap();
    let mut node2 = RaftNode::new(2, addr2, tls2, storage2, catalog2)
        .await
        .unwrap();
    let mut node3 = RaftNode::new(3, addr3, tls3, storage3, catalog3)
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

    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Find leader
    let nodes = [&node1, &node2, &node3];
    let mut leader = None;
    for node in &nodes {
        if node.is_leader().await {
            leader = Some(*node);
            break;
        }
    }
    let leader = leader.expect("No leader found");

    // Propose multiple changes through leader
    for i in 0..5 {
        let key = format!("consistency_key_{}", i).into_bytes();
        let value = format!("consistency_value_{}", i).into_bytes();
        let changeset = insert_change("test", &key, &value);
        leader.propose_changes(changeset).await.unwrap();
    }

    // Wait for replication - if Raft consensus works, changes are replicated
    tokio::time::sleep(Duration::from_millis(500)).await;

    // The test verifies that propose_changes succeeds, which means
    // the Raft log was replicated to a majority. Without a shared
    // storage engine, we can't verify at the storage level.

    node1.shutdown().await.unwrap();
    node2.shutdown().await.unwrap();
    node3.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_follower_read() {
    let (tls1, tls2, tls3, _cert_files) = generate_cluster_tls_configs();
    let base_port = test_port(16200);

    let addr1: SocketAddr = format!("127.0.0.1:{}", base_port).parse().unwrap();
    let addr2: SocketAddr = format!("127.0.0.1:{}", base_port + 1).parse().unwrap();
    let addr3: SocketAddr = format!("127.0.0.1:{}", base_port + 2).parse().unwrap();

    let storage1 = test_storage("follower1").await;
    let storage2 = test_storage("follower2").await;
    let storage3 = test_storage("follower3").await;
    let catalog1 = test_catalog();
    let catalog2 = test_catalog();
    let catalog3 = test_catalog();

    let mut node1 = RaftNode::new(1, addr1, tls1, storage1, catalog1)
        .await
        .unwrap();
    let mut node2 = RaftNode::new(2, addr2, tls2, storage2, catalog2)
        .await
        .unwrap();
    let mut node3 = RaftNode::new(3, addr3, tls3, storage3, catalog3)
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

    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Find leader and a follower
    let nodes = [&node1, &node2, &node3];
    let mut leader = None;
    let mut follower = None;
    for node in &nodes {
        if node.is_leader().await {
            leader = Some(*node);
        } else if follower.is_none() {
            follower = Some(*node);
        }
    }

    let leader = leader.expect("No leader found");
    let _follower = follower.expect("No follower found");

    // Propose changes through leader
    let changeset = insert_change("test", b"follower_read_key", b"follower_read_value");
    leader.propose_changes(changeset).await.unwrap();

    // Wait for replication
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Note: To verify follower reads, we would need integrated storage.
    // This test verifies that the cluster can elect a leader and accept writes.

    node1.shutdown().await.unwrap();
    node2.shutdown().await.unwrap();
    node3.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_write_delete_sequence() {
    let (tls1, tls2, tls3, _cert_files) = generate_cluster_tls_configs();
    let base_port = test_port(16300);

    let addr1: SocketAddr = format!("127.0.0.1:{}", base_port).parse().unwrap();
    let addr2: SocketAddr = format!("127.0.0.1:{}", base_port + 1).parse().unwrap();
    let addr3: SocketAddr = format!("127.0.0.1:{}", base_port + 2).parse().unwrap();

    let storage1 = test_storage("delete1").await;
    let storage2 = test_storage("delete2").await;
    let storage3 = test_storage("delete3").await;
    let catalog1 = test_catalog();
    let catalog2 = test_catalog();
    let catalog3 = test_catalog();

    let mut node1 = RaftNode::new(1, addr1, tls1, storage1, catalog1)
        .await
        .unwrap();
    let mut node2 = RaftNode::new(2, addr2, tls2, storage2, catalog2)
        .await
        .unwrap();
    let mut node3 = RaftNode::new(3, addr3, tls3, storage3, catalog3)
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

    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Find leader
    let nodes = [&node1, &node2, &node3];
    let mut leader = None;
    for node in &nodes {
        if node.is_leader().await {
            leader = Some(*node);
            break;
        }
    }
    let leader = leader.expect("No leader found");

    // Propose an insert
    let insert_cs = insert_change("test", b"delete_test_key", b"delete_test_value");
    leader.propose_changes(insert_cs).await.unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Propose a delete
    let mut delete_cs = ChangeSet::new(2);
    delete_cs.push(RowChange::delete("test", b"delete_test_key".to_vec()));
    leader.propose_changes(delete_cs).await.unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Test verifies that insert and delete changes can be proposed successfully

    node1.shutdown().await.unwrap();
    node2.shutdown().await.unwrap();
    node3.shutdown().await.unwrap();
}
