//! Raft replication tests.
//!
//! These tests verify Raft consensus behavior.
//! SQL-level replication is not yet implemented.

use std::net::SocketAddr;
use std::time::Duration;

use roodb::raft::{Command, RaftNode};

use crate::test_utils::certs::test_tls_config;

/// Get a unique port for testing
fn test_port(base: u16) -> u16 {
    base + (std::process::id() as u16 % 1000)
}

#[tokio::test]
async fn test_leader_election_timing() {
    let tls_config = test_tls_config();
    let base_port = test_port(16000);

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
    let tls_config = test_tls_config();
    let base_port = test_port(16100);

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

    // Write multiple values through leader
    for i in 0..5 {
        let key = format!("consistency_key_{}", i).into_bytes();
        let value = format!("consistency_value_{}", i).into_bytes();
        leader.write(Command::Put { key, value }).await.unwrap();
    }

    // Wait for replication
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify all nodes have consistent state
    for i in 0..5 {
        let key = format!("consistency_key_{}", i).into_bytes();
        let expected = format!("consistency_value_{}", i).into_bytes();

        for node in &nodes {
            let value = node.storage().get(&key);
            assert_eq!(
                value,
                Some(expected.clone()),
                "Node should have replicated value"
            );
        }
    }

    node1.shutdown().await.unwrap();
    node2.shutdown().await.unwrap();
    node3.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_follower_read() {
    let tls_config = test_tls_config();
    let base_port = test_port(16200);

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
    let follower = follower.expect("No follower found");

    // Write through leader
    leader
        .write(Command::Put {
            key: b"follower_read_key".to_vec(),
            value: b"follower_read_value".to_vec(),
        })
        .await
        .unwrap();

    // Wait for replication
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Read from follower's state machine (direct read, not through Raft)
    let value = follower.storage().get(b"follower_read_key");
    assert_eq!(
        value,
        Some(b"follower_read_value".to_vec()),
        "Follower should have replicated data"
    );

    node1.shutdown().await.unwrap();
    node2.shutdown().await.unwrap();
    node3.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_write_delete_sequence() {
    let tls_config = test_tls_config();
    let base_port = test_port(16300);

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

    // Write a value
    leader
        .write(Command::Put {
            key: b"delete_test_key".to_vec(),
            value: b"delete_test_value".to_vec(),
        })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Verify all nodes have the value
    for node in &nodes {
        let value = node.storage().get(b"delete_test_key");
        assert_eq!(value, Some(b"delete_test_value".to_vec()));
    }

    // Delete the value
    leader
        .write(Command::Delete {
            key: b"delete_test_key".to_vec(),
        })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Verify all nodes have deleted the value
    for node in &nodes {
        let value = node.storage().get(b"delete_test_key");
        assert_eq!(value, None, "Value should be deleted on all nodes");
    }

    node1.shutdown().await.unwrap();
    node2.shutdown().await.unwrap();
    node3.shutdown().await.unwrap();
}
