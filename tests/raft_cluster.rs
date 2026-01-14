//! Integration tests for Raft cluster functionality

mod test_utils;

use std::net::SocketAddr;
use std::time::Duration;

use roodb::raft::{Command, RaftNode};
use test_utils::certs::test_tls_config;

/// Get a unique port for testing (based on process ID to avoid conflicts)
fn test_port(base: u16) -> u16 {
    base + (std::process::id() as u16 % 1000)
}

#[tokio::test]
async fn test_single_node_bootstrap() {
    let tls_config = test_tls_config();
    let addr: SocketAddr = format!("127.0.0.1:{}", test_port(15000)).parse().unwrap();

    let mut node = RaftNode::new(1, addr, tls_config).await.unwrap();

    // Start RPC server
    node.start_rpc_server().await.unwrap();

    // Bootstrap as single node
    node.bootstrap_single_node().await.unwrap();

    // Wait for leader election
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Should be leader
    assert!(node.is_leader().await);

    // Write a value
    let resp = node
        .write(Command::Put {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
        })
        .await
        .unwrap();
    assert!(matches!(resp, roodb::raft::CommandResponse::Ok(_)));

    // Read it back
    let value = node.read(b"key1").await.unwrap();
    assert_eq!(value, Some(b"value1".to_vec()));

    // Shutdown
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_single_node_multiple_writes() {
    let tls_config = test_tls_config();
    let addr: SocketAddr = format!("127.0.0.1:{}", test_port(15100)).parse().unwrap();

    let mut node = RaftNode::new(1, addr, tls_config).await.unwrap();
    node.start_rpc_server().await.unwrap();
    node.bootstrap_single_node().await.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Write multiple values
    for i in 0..10 {
        let key = format!("key{}", i).into_bytes();
        let value = format!("value{}", i).into_bytes();
        node.write(Command::Put { key, value }).await.unwrap();
    }

    // Read all back
    for i in 0..10 {
        let key = format!("key{}", i).into_bytes();
        let expected = format!("value{}", i).into_bytes();
        let value = node.read(&key).await.unwrap();
        assert_eq!(value, Some(expected));
    }

    // Delete one
    node.write(Command::Delete {
        key: b"key5".to_vec(),
    })
    .await
    .unwrap();

    // Verify deletion
    let value = node.read(b"key5").await.unwrap();
    assert_eq!(value, None);

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_three_node_cluster_bootstrap() {
    let tls_config = test_tls_config();
    let base_port = test_port(15200);

    let addr1: SocketAddr = format!("127.0.0.1:{}", base_port).parse().unwrap();
    let addr2: SocketAddr = format!("127.0.0.1:{}", base_port + 1).parse().unwrap();
    let addr3: SocketAddr = format!("127.0.0.1:{}", base_port + 2).parse().unwrap();

    // Create nodes
    let mut node1 = RaftNode::new(1, addr1, tls_config.clone()).await.unwrap();
    let mut node2 = RaftNode::new(2, addr2, tls_config.clone()).await.unwrap();
    let mut node3 = RaftNode::new(3, addr3, tls_config.clone()).await.unwrap();

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

    // Wait for leader election
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Find the leader
    let mut leader = None;
    for (i, node) in [&node1, &node2, &node3].iter().enumerate() {
        if node.is_leader().await {
            leader = Some(i + 1);
            break;
        }
    }

    assert!(leader.is_some(), "No leader elected");
    tracing::info!(leader_id = leader.unwrap(), "Leader elected");

    // Shutdown nodes
    node1.shutdown().await.unwrap();
    node2.shutdown().await.unwrap();
    node3.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_log_replication() {
    let tls_config = test_tls_config();
    let base_port = test_port(15300);

    let addr1: SocketAddr = format!("127.0.0.1:{}", base_port).parse().unwrap();
    let addr2: SocketAddr = format!("127.0.0.1:{}", base_port + 1).parse().unwrap();
    let addr3: SocketAddr = format!("127.0.0.1:{}", base_port + 2).parse().unwrap();

    let mut node1 = RaftNode::new(1, addr1, tls_config.clone()).await.unwrap();
    let mut node2 = RaftNode::new(2, addr2, tls_config.clone()).await.unwrap();
    let mut node3 = RaftNode::new(3, addr3, tls_config.clone()).await.unwrap();

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

    // Find leader and write through it
    let nodes = [&node1, &node2, &node3];
    let mut leader_node = None;
    for node in &nodes {
        if node.is_leader().await {
            leader_node = Some(*node);
            break;
        }
    }

    let leader = leader_node.expect("No leader found");

    // Write through leader
    leader
        .write(Command::Put {
            key: b"replicated_key".to_vec(),
            value: b"replicated_value".to_vec(),
        })
        .await
        .unwrap();

    // Wait for replication
    tokio::time::sleep(Duration::from_millis(500)).await;

    // All nodes should have the value in their state machine
    for node in &nodes {
        let value = node.storage().get(b"replicated_key");
        assert_eq!(value, Some(b"replicated_value".to_vec()));
    }

    node1.shutdown().await.unwrap();
    node2.shutdown().await.unwrap();
    node3.shutdown().await.unwrap();
}
