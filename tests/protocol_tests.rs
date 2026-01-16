//! RooDB protocol tests
//!
//! Tests for packet encoding/decoding, handshake, auth, and result sets.

mod test_utils;

use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::RwLock;

use roodb::catalog::{Catalog, DataType};
use roodb::executor::datum::Datum;
use roodb::executor::row::Row;
use roodb::io::PosixIOFactory;
use roodb::planner::logical::OutputColumn;
use roodb::protocol::roodb::auth::{verify_native_password, HandshakeResponse41};
use roodb::protocol::roodb::command::{parse_command, Command, ParsedCommand};
use roodb::protocol::roodb::handshake::{
    capabilities, HandshakeV10, AUTH_PLUGIN_NAME, SERVER_VERSION,
};
use roodb::protocol::roodb::packet::{
    decode_length_encoded_int, decode_length_encoded_string, decode_null_terminated_string,
    encode_length_encoded_bytes, encode_length_encoded_int, encode_length_encoded_string,
    encode_null_terminated_string,
};
use roodb::protocol::roodb::resultset::{
    encode_column_count, encode_eof_packet, encode_err_packet, encode_ok_packet, encode_text_row,
    ColumnDefinition41,
};
use roodb::protocol::roodb::types::{datatype_to_protocol, datum_to_text_bytes, ColumnType};
use roodb::server::listener::start_test_server;
use roodb::storage::lsm::{LsmConfig, LsmEngine};
use roodb::storage::StorageEngine;

// ============================================================================
// Packet encoding/decoding tests
// ============================================================================

#[test]
fn test_length_encoded_int_roundtrip() {
    let values = [0u64, 1, 250, 251, 1000, 65535, 65536, 1_000_000, u64::MAX];

    for val in values {
        let encoded = encode_length_encoded_int(val);
        let (decoded, consumed) = decode_length_encoded_int(&encoded).unwrap();
        assert_eq!(decoded, val, "mismatch for value {}", val);
        assert_eq!(consumed, encoded.len());
    }
}

#[test]
fn test_length_encoded_string_roundtrip() {
    let strings = ["", "a", "hello", "hello world", &"x".repeat(1000)];

    for s in strings {
        let encoded = encode_length_encoded_string(s);
        let (decoded, consumed) = decode_length_encoded_string(&encoded).unwrap();
        assert_eq!(decoded, s);
        assert_eq!(consumed, encoded.len());
    }
}

#[test]
fn test_null_terminated_string_roundtrip() {
    let strings = ["", "test", "hello world"];

    for s in strings {
        let encoded = encode_null_terminated_string(s);
        let (decoded, consumed) = decode_null_terminated_string(&encoded).unwrap();
        assert_eq!(decoded, s);
        assert_eq!(consumed, s.len() + 1); // +1 for null terminator
    }
}

#[test]
fn test_length_encoded_bytes() {
    let data = vec![1u8, 2, 3, 4, 5];
    let encoded = encode_length_encoded_bytes(&data);

    // First byte is length (5)
    assert_eq!(encoded[0], 5);
    // Rest is the data
    assert_eq!(&encoded[1..], &data[..]);
}

// ============================================================================
// Handshake tests
// ============================================================================

#[test]
fn test_handshake_v10_creation() {
    let handshake = HandshakeV10::new(42);

    assert_eq!(handshake.protocol_version, 10);
    assert_eq!(handshake.connection_id, 42);
    assert_eq!(handshake.server_version, SERVER_VERSION);
    assert_eq!(handshake.auth_plugin_name, AUTH_PLUGIN_NAME);

    // Scramble should be 20 bytes
    let scramble = handshake.scramble();
    assert_eq!(scramble.len(), 20);
}

#[test]
fn test_handshake_v10_capabilities() {
    let handshake = HandshakeV10::new(1);
    let caps = handshake.capabilities();

    // Verify essential capabilities are set
    assert!(caps & capabilities::CLIENT_PROTOCOL_41 != 0);
    assert!(caps & capabilities::CLIENT_SECURE_CONNECTION != 0);
    assert!(caps & capabilities::CLIENT_PLUGIN_AUTH != 0);
}

#[test]
fn test_handshake_v10_encode() {
    let handshake = HandshakeV10::new(123);
    let encoded = handshake.encode();

    // Protocol version should be first byte
    assert_eq!(encoded[0], 10);

    // Server version should follow (null-terminated)
    let version_end = encoded[1..].iter().position(|&b| b == 0).unwrap() + 1;
    let version = std::str::from_utf8(&encoded[1..version_end]).unwrap();
    assert_eq!(version, SERVER_VERSION);
}

// ============================================================================
// Authentication tests
// ============================================================================

#[test]
fn test_verify_empty_password() {
    let scramble = [0u8; 20];
    assert!(verify_native_password(&scramble, "", &[]));
    assert!(!verify_native_password(&scramble, "", &[1, 2, 3]));
}

#[test]
fn test_verify_password_wrong_length() {
    let scramble = [0u8; 20];
    // Non-empty password should have 20-byte response
    assert!(!verify_native_password(&scramble, "secret", &[1, 2, 3]));
}

#[test]
fn test_parse_minimal_handshake_response() {
    // Build a minimal handshake response packet
    let mut packet = vec![0u8; 32];

    // Capability flags (4 bytes)
    let caps = capabilities::CLIENT_PROTOCOL_41 | capabilities::CLIENT_SECURE_CONNECTION;
    packet[0..4].copy_from_slice(&caps.to_le_bytes());

    // Max packet size (4 bytes)
    packet[4..8].copy_from_slice(&(16_777_215u32).to_le_bytes());

    // Character set (1 byte)
    packet[8] = 45;

    // Reserved (23 bytes) - already zeros

    // Username (null-terminated)
    packet.extend_from_slice(b"testuser\0");

    // Auth response length (1 byte) + empty response
    packet.push(0);

    let response = HandshakeResponse41::parse(&packet).unwrap();
    assert_eq!(response.username, "testuser");
    assert_eq!(response.capability_flags, caps);
    assert!(response.auth_response.is_empty());
}

// ============================================================================
// Command parsing tests
// ============================================================================

#[test]
fn test_parse_com_quit() {
    let packet = vec![0x01];
    match parse_command(&packet).unwrap() {
        ParsedCommand::Quit => {}
        _ => panic!("expected Quit"),
    }
}

#[test]
fn test_parse_com_ping() {
    let packet = vec![0x0e];
    match parse_command(&packet).unwrap() {
        ParsedCommand::Ping => {}
        _ => panic!("expected Ping"),
    }
}

#[test]
fn test_parse_com_query() {
    let mut packet = vec![0x03];
    packet.extend_from_slice(b"SELECT 1");

    match parse_command(&packet).unwrap() {
        ParsedCommand::Query(sql) => assert_eq!(sql, "SELECT 1"),
        _ => panic!("expected Query"),
    }
}

#[test]
fn test_parse_com_init_db() {
    let mut packet = vec![0x02];
    packet.extend_from_slice(b"testdb");

    match parse_command(&packet).unwrap() {
        ParsedCommand::InitDb(db) => assert_eq!(db, "testdb"),
        _ => panic!("expected InitDb"),
    }
}

#[test]
fn test_command_try_from() {
    assert_eq!(Command::try_from(0x01).unwrap(), Command::Quit);
    assert_eq!(Command::try_from(0x03).unwrap(), Command::Query);
    assert_eq!(Command::try_from(0x0e).unwrap(), Command::Ping);
}

// ============================================================================
// Result set encoding tests
// ============================================================================

#[test]
fn test_ok_packet_encoding() {
    let packet = encode_ok_packet(10, 5, 0x0002, 1);

    assert_eq!(packet[0], 0x00); // OK header
    assert_eq!(packet[1], 10); // affected_rows
    assert_eq!(packet[2], 5); // last_insert_id
}

#[test]
fn test_err_packet_encoding() {
    let packet = encode_err_packet(1064, "42000", "Syntax error");

    assert_eq!(packet[0], 0xff); // ERR header
    assert_eq!(u16::from_le_bytes([packet[1], packet[2]]), 1064); // error code
    assert_eq!(packet[3], b'#'); // sql_state_marker
    assert_eq!(&packet[4..9], b"42000"); // sql_state
}

#[test]
fn test_eof_packet_encoding() {
    let packet = encode_eof_packet(0, 0x0002);

    assert_eq!(packet[0], 0xfe); // EOF header
    assert_eq!(packet.len(), 5);
}

#[test]
fn test_column_count_encoding() {
    let packet = encode_column_count(5);
    let (count, _) = decode_length_encoded_int(&packet).unwrap();
    assert_eq!(count, 5);
}

#[test]
fn test_text_row_encoding() {
    let row = Row::new(vec![
        Datum::Int(42),
        Datum::String("hello".to_string()),
        Datum::Null,
    ]);

    let encoded = encode_text_row(&row);

    // Should contain NULL marker (0xfb)
    assert!(encoded.contains(&0xfb));
}

#[test]
fn test_column_definition_encoding() {
    let col = OutputColumn {
        id: 0,
        name: "test_col".to_string(),
        data_type: DataType::Int,
        nullable: false,
    };

    let def = ColumnDefinition41::from_output_column(&col, "test_table", "test_db");
    let encoded = def.encode();

    // Should start with "def" catalog
    assert!(encoded.starts_with(&[3, b'd', b'e', b'f']));
}

// ============================================================================
// Type mapping tests
// ============================================================================

#[test]
fn test_datatype_to_protocol() {
    assert_eq!(datatype_to_protocol(&DataType::Boolean), ColumnType::Tiny);
    assert_eq!(datatype_to_protocol(&DataType::TinyInt), ColumnType::Tiny);
    assert_eq!(datatype_to_protocol(&DataType::SmallInt), ColumnType::Short);
    assert_eq!(datatype_to_protocol(&DataType::Int), ColumnType::Long);
    assert_eq!(
        datatype_to_protocol(&DataType::BigInt),
        ColumnType::LongLong
    );
    assert_eq!(datatype_to_protocol(&DataType::Float), ColumnType::Float);
    assert_eq!(datatype_to_protocol(&DataType::Double), ColumnType::Double);
    assert_eq!(
        datatype_to_protocol(&DataType::Varchar(255)),
        ColumnType::Varchar
    );
    assert_eq!(datatype_to_protocol(&DataType::Text), ColumnType::Blob);
    assert_eq!(datatype_to_protocol(&DataType::Blob), ColumnType::Blob);
    assert_eq!(
        datatype_to_protocol(&DataType::Timestamp),
        ColumnType::Datetime
    );
}

#[test]
fn test_datum_to_text_bytes_null() {
    let bytes = datum_to_text_bytes(&Datum::Null);
    assert_eq!(bytes, vec![0xfb]); // NULL marker
}

#[test]
fn test_datum_to_text_bytes_bool() {
    let true_bytes = datum_to_text_bytes(&Datum::Bool(true));
    let false_bytes = datum_to_text_bytes(&Datum::Bool(false));

    // Should be length-encoded "1" and "0"
    assert_eq!(true_bytes, vec![1, b'1']);
    assert_eq!(false_bytes, vec![1, b'0']);
}

#[test]
fn test_datum_to_text_bytes_int() {
    let bytes = datum_to_text_bytes(&Datum::Int(42));
    // Should be length-encoded "42"
    assert_eq!(bytes, vec![2, b'4', b'2']);
}

#[test]
fn test_datum_to_text_bytes_string() {
    let bytes = datum_to_text_bytes(&Datum::String("hello".to_string()));
    // Should be length-encoded "hello"
    assert_eq!(bytes, vec![5, b'h', b'e', b'l', b'l', b'o']);
}

#[test]
fn test_datum_to_text_bytes_bytes() {
    let data = vec![1u8, 2, 3];
    let bytes = datum_to_text_bytes(&Datum::Bytes(data.clone()));
    // Length prefix (3) followed by data
    assert_eq!(bytes[0], 3);
    assert_eq!(&bytes[1..], &data[..]);
}

// ============================================================================
// Server integration tests
// ============================================================================

fn test_dir(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "roodb_protocol_test_{}_{}",
        name,
        std::process::id()
    ));
    path
}

fn cleanup_dir(path: &PathBuf) {
    let _ = std::fs::remove_dir_all(path);
}

/// End-to-end integration test: start server, connect via client, run SQL
#[tokio::test]
async fn test_server_integration_e2e() {
    use mysql_async::prelude::*;
    use mysql_async::{Opts, OptsBuilder, SslOpts};

    // Initialize tracing for debug output
    let _ = tracing_subscriber::fmt()
        .with_env_filter("roodb=debug")
        .try_init();

    // Setup test infrastructure
    let tls_config = test_utils::certs::test_tls_config();
    let data_dir = test_dir("e2e");
    cleanup_dir(&data_dir);

    // Create storage and catalog
    let factory = Arc::new(PosixIOFactory);
    let config = LsmConfig {
        dir: data_dir.clone(),
    };
    let storage: Arc<dyn StorageEngine> = Arc::new(LsmEngine::open(factory, config).await.unwrap());
    let catalog = Arc::new(RwLock::new(Catalog::new()));

    // Initialize root user with empty password for tests
    test_utils::auth::initialize_root_user(&storage, "").await;

    // Bind to an available port and keep the listener to avoid TOCTOU race
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    std_listener.set_nonblocking(true).unwrap();
    let port = std_listener.local_addr().unwrap().port();
    let listener = tokio::net::TcpListener::from_std(std_listener).unwrap();

    // Start server with the pre-bound listener
    let handle = start_test_server(listener, tls_config, storage.clone(), catalog.clone())
        .await
        .expect("Failed to start test server");

    // Give server time to start accepting connections
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Build client with TLS (accept invalid certs for self-signed)
    // Set max_allowed_packet and wait_timeout to avoid init queries for @@variables
    let ssl_opts = SslOpts::default().with_danger_accept_invalid_certs(true);

    let opts: Opts = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some("root"))
        .ssl_opts(ssl_opts)
        .max_allowed_packet(Some(16_777_216)) // 16MB, skip @@max_allowed_packet query
        .wait_timeout(Some(28800)) // Default, skip @@wait_timeout query
        .into();

    // Connect
    let pool = mysql_async::Pool::new(opts);
    let mut conn = pool.get_conn().await.expect("Failed to connect");

    // Test CREATE TABLE
    conn.query_drop("CREATE TABLE test_table (id INT, name VARCHAR(100))")
        .await
        .expect("CREATE TABLE failed");

    // Test INSERT
    conn.query_drop("INSERT INTO test_table (id, name) VALUES (1, 'Alice')")
        .await
        .expect("INSERT 1 failed");
    conn.query_drop("INSERT INTO test_table (id, name) VALUES (2, 'Bob')")
        .await
        .expect("INSERT 2 failed");

    // Test SELECT
    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, name FROM test_table ORDER BY id")
        .await
        .expect("SELECT failed");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (1, "Alice".to_string()));
    assert_eq!(rows[1], (2, "Bob".to_string()));

    // Test DROP TABLE
    conn.query_drop("DROP TABLE test_table")
        .await
        .expect("DROP TABLE failed");

    // Cleanup
    drop(conn);
    pool.disconnect().await.unwrap();
    handle.shutdown();

    // Give server time to shutdown
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Cleanup storage
    storage.close().await.unwrap();
    cleanup_dir(&data_dir);
}

/// Test explicit transactions with BEGIN/COMMIT
#[tokio::test]
async fn test_transaction_begin_commit() {
    use mysql_async::prelude::*;
    use mysql_async::{Opts, OptsBuilder, SslOpts};

    let _ = tracing_subscriber::fmt()
        .with_env_filter("roodb=debug")
        .try_init();

    let tls_config = test_utils::certs::test_tls_config();
    let data_dir = test_dir("txn_commit");
    cleanup_dir(&data_dir);

    let factory = Arc::new(PosixIOFactory);
    let config = LsmConfig {
        dir: data_dir.clone(),
    };
    let storage: Arc<dyn StorageEngine> = Arc::new(LsmEngine::open(factory, config).await.unwrap());
    let catalog = Arc::new(RwLock::new(Catalog::new()));

    // Initialize root user with empty password for tests
    test_utils::auth::initialize_root_user(&storage, "").await;

    // Bind to an available port and keep the listener to avoid TOCTOU race
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    std_listener.set_nonblocking(true).unwrap();
    let port = std_listener.local_addr().unwrap().port();
    let listener = tokio::net::TcpListener::from_std(std_listener).unwrap();

    // Start server with the pre-bound listener
    let handle = start_test_server(listener, tls_config, storage.clone(), catalog.clone())
        .await
        .expect("Failed to start test server");

    // Give server time to start accepting connections
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let ssl_opts = SslOpts::default().with_danger_accept_invalid_certs(true);
    let opts: Opts = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some("root"))
        .ssl_opts(ssl_opts)
        .max_allowed_packet(Some(16_777_216))
        .wait_timeout(Some(28800))
        .into();

    let pool = mysql_async::Pool::new(opts);
    let mut conn = pool.get_conn().await.expect("Failed to connect");

    // Create table
    conn.query_drop("CREATE TABLE txn_test (id INT, value VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    // Begin transaction
    conn.query_drop("BEGIN").await.expect("BEGIN failed");

    // Insert within transaction
    conn.query_drop("INSERT INTO txn_test (id, value) VALUES (1, 'first')")
        .await
        .expect("INSERT failed");

    // Query within same transaction should see the row
    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, value FROM txn_test WHERE id = 1")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (1, "first".to_string()));

    // Commit transaction
    conn.query_drop("COMMIT").await.expect("COMMIT failed");

    // After commit, row should still be visible
    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, value FROM txn_test WHERE id = 1")
        .await
        .expect("SELECT after commit failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (1, "first".to_string()));

    // Cleanup
    conn.query_drop("DROP TABLE txn_test")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    pool.disconnect().await.unwrap();
    handle.shutdown();
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    storage.close().await.unwrap();
    cleanup_dir(&data_dir);
}

/// Test transaction ROLLBACK
#[tokio::test]
async fn test_transaction_rollback() {
    use mysql_async::prelude::*;
    use mysql_async::{Opts, OptsBuilder, SslOpts};

    let _ = tracing_subscriber::fmt()
        .with_env_filter("roodb=debug")
        .try_init();

    let tls_config = test_utils::certs::test_tls_config();
    let data_dir = test_dir("txn_rollback");
    cleanup_dir(&data_dir);

    let factory = Arc::new(PosixIOFactory);
    let config = LsmConfig {
        dir: data_dir.clone(),
    };
    let storage: Arc<dyn StorageEngine> = Arc::new(LsmEngine::open(factory, config).await.unwrap());
    let catalog = Arc::new(RwLock::new(Catalog::new()));

    // Initialize root user with empty password for tests
    test_utils::auth::initialize_root_user(&storage, "").await;

    // Bind to an available port and keep the listener to avoid TOCTOU race
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    std_listener.set_nonblocking(true).unwrap();
    let port = std_listener.local_addr().unwrap().port();
    let listener = tokio::net::TcpListener::from_std(std_listener).unwrap();

    // Start server with the pre-bound listener
    let handle = start_test_server(listener, tls_config, storage.clone(), catalog.clone())
        .await
        .expect("Failed to start test server");

    // Give server time to start accepting connections
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let ssl_opts = SslOpts::default().with_danger_accept_invalid_certs(true);
    let opts: Opts = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some("root"))
        .ssl_opts(ssl_opts)
        .max_allowed_packet(Some(16_777_216))
        .wait_timeout(Some(28800))
        .into();

    let pool = mysql_async::Pool::new(opts);
    let mut conn = pool.get_conn().await.expect("Failed to connect");

    // Create table and insert initial data
    conn.query_drop("CREATE TABLE rollback_test (id INT, value VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");
    conn.query_drop("INSERT INTO rollback_test (id, value) VALUES (1, 'original')")
        .await
        .expect("Initial INSERT failed");

    // Begin transaction
    conn.query_drop("BEGIN").await.expect("BEGIN failed");

    // Insert within transaction
    conn.query_drop("INSERT INTO rollback_test (id, value) VALUES (2, 'will_rollback')")
        .await
        .expect("INSERT failed");

    // Row should be visible within transaction
    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, value FROM rollback_test ORDER BY id")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 2);

    // Rollback
    conn.query_drop("ROLLBACK").await.expect("ROLLBACK failed");

    // After rollback, only original row should exist
    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, value FROM rollback_test ORDER BY id")
        .await
        .expect("SELECT after rollback failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (1, "original".to_string()));

    // Cleanup
    conn.query_drop("DROP TABLE rollback_test")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    pool.disconnect().await.unwrap();
    handle.shutdown();
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    storage.close().await.unwrap();
    cleanup_dir(&data_dir);
}

/// Test autocommit mode (each statement is implicitly committed)
#[tokio::test]
async fn test_autocommit() {
    use mysql_async::prelude::*;
    use mysql_async::{Opts, OptsBuilder, SslOpts};

    let _ = tracing_subscriber::fmt()
        .with_env_filter("roodb=debug")
        .try_init();

    let tls_config = test_utils::certs::test_tls_config();
    let data_dir = test_dir("autocommit");
    cleanup_dir(&data_dir);

    let factory = Arc::new(PosixIOFactory);
    let config = LsmConfig {
        dir: data_dir.clone(),
    };
    let storage: Arc<dyn StorageEngine> = Arc::new(LsmEngine::open(factory, config).await.unwrap());
    let catalog = Arc::new(RwLock::new(Catalog::new()));

    // Initialize root user with empty password for tests
    test_utils::auth::initialize_root_user(&storage, "").await;

    // Bind to an available port and keep the listener to avoid TOCTOU race
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    std_listener.set_nonblocking(true).unwrap();
    let port = std_listener.local_addr().unwrap().port();
    let listener = tokio::net::TcpListener::from_std(std_listener).unwrap();

    // Start server with the pre-bound listener
    let handle = start_test_server(listener, tls_config, storage.clone(), catalog.clone())
        .await
        .expect("Failed to start test server");

    // Give server time to start accepting connections
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let ssl_opts = SslOpts::default().with_danger_accept_invalid_certs(true);
    let opts: Opts = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some("root"))
        .ssl_opts(ssl_opts)
        .max_allowed_packet(Some(16_777_216))
        .wait_timeout(Some(28800))
        .into();

    let pool = mysql_async::Pool::new(opts.clone());
    let mut conn1 = pool.get_conn().await.expect("Failed to connect");

    // Create table
    conn1
        .query_drop("CREATE TABLE autocommit_test (id INT, value VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    // Insert in autocommit mode (no explicit BEGIN)
    conn1
        .query_drop("INSERT INTO autocommit_test (id, value) VALUES (1, 'auto')")
        .await
        .expect("INSERT failed");

    // Open second connection and verify data is visible
    // (proving the first insert was auto-committed)
    let mut conn2 = pool.get_conn().await.expect("Failed to get second conn");
    let rows: Vec<(i32, String)> = conn2
        .query("SELECT id, value FROM autocommit_test WHERE id = 1")
        .await
        .expect("SELECT from conn2 failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (1, "auto".to_string()));

    // Cleanup
    conn1
        .query_drop("DROP TABLE autocommit_test")
        .await
        .expect("DROP TABLE failed");

    drop(conn1);
    drop(conn2);
    pool.disconnect().await.unwrap();
    handle.shutdown();
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    storage.close().await.unwrap();
    cleanup_dir(&data_dir);
}
