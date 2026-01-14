//! Integration tests for the WAL subsystem

use std::path::PathBuf;
use std::sync::Arc;

use roodb::io::PosixIOFactory;
use roodb::wal::{Record, RecordType, WalConfig, WalManager, HEADER_SIZE};

#[cfg(target_os = "linux")]
use roodb::io::UringIOFactory;

fn test_dir(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("roodb_wal_test_{}_{}", name, std::process::id()));
    path
}

fn cleanup_dir(path: &PathBuf) {
    let _ = std::fs::remove_dir_all(path);
}

// ============ POSIX WAL Tests ============

#[tokio::test]
async fn test_posix_wal_append_single() {
    let dir = test_dir("posix_append_single");
    cleanup_dir(&dir);

    let factory = Arc::new(PosixIOFactory);
    let config = WalConfig {
        dir: dir.clone(),
        segment_size: 1024 * 1024, // 1MB
        sync_on_write: true,
    };

    let wal = WalManager::new(factory.clone(), config).await.unwrap();

    // Append a record
    let data = b"Hello, WAL!".to_vec();
    let lsn = wal.append(&*factory, data.clone()).await.unwrap();
    assert_eq!(lsn, 1);

    // Read back
    let records = wal.read_from(&*factory, 1).await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].lsn, 1);
    assert_eq!(records[0].data, data);
    assert_eq!(records[0].record_type, RecordType::Data);

    wal.close().await.unwrap();
    cleanup_dir(&dir);
}

#[tokio::test]
async fn test_posix_wal_append_multiple() {
    let dir = test_dir("posix_append_multi");
    cleanup_dir(&dir);

    let factory = Arc::new(PosixIOFactory);
    let config = WalConfig {
        dir: dir.clone(),
        segment_size: 1024 * 1024,
        sync_on_write: true,
    };

    let wal = WalManager::new(factory.clone(), config).await.unwrap();

    // Append multiple records
    for i in 1..=10 {
        let data = format!("Record {}", i).into_bytes();
        let lsn = wal.append(&*factory, data).await.unwrap();
        assert_eq!(lsn, i);
    }

    // Read all back
    let records = wal.read_from(&*factory, 1).await.unwrap();
    assert_eq!(records.len(), 10);

    for (i, record) in records.iter().enumerate() {
        let expected_lsn = (i + 1) as u64;
        assert_eq!(record.lsn, expected_lsn);
        assert_eq!(record.data, format!("Record {}", expected_lsn).into_bytes());
    }

    wal.close().await.unwrap();
    cleanup_dir(&dir);
}

#[tokio::test]
async fn test_posix_wal_recovery() {
    let dir = test_dir("posix_recovery");
    cleanup_dir(&dir);

    let factory = Arc::new(PosixIOFactory);
    let config = WalConfig {
        dir: dir.clone(),
        segment_size: 1024 * 1024,
        sync_on_write: true,
    };

    // Write some records
    {
        let wal = WalManager::new(factory.clone(), config.clone())
            .await
            .unwrap();
        for i in 1..=5 {
            wal.append(&*factory, format!("Data {}", i).into_bytes())
                .await
                .unwrap();
        }
        wal.close().await.unwrap();
    }

    // Reopen and verify recovery
    {
        let wal = WalManager::new(factory.clone(), config).await.unwrap();

        // LSN should continue from where we left off
        assert_eq!(wal.current_lsn(), 6);

        // All records should be readable
        let records = wal.read_from(&*factory, 1).await.unwrap();
        assert_eq!(records.len(), 5);

        for (i, record) in records.iter().enumerate() {
            assert_eq!(record.lsn, (i + 1) as u64);
            assert_eq!(record.data, format!("Data {}", i + 1).into_bytes());
        }

        wal.close().await.unwrap();
    }

    cleanup_dir(&dir);
}

#[tokio::test]
async fn test_posix_wal_read_from_offset() {
    let dir = test_dir("posix_read_offset");
    cleanup_dir(&dir);

    let factory = Arc::new(PosixIOFactory);
    let config = WalConfig {
        dir: dir.clone(),
        segment_size: 1024 * 1024,
        sync_on_write: true,
    };

    let wal = WalManager::new(factory.clone(), config).await.unwrap();

    // Append 10 records
    for i in 1..=10 {
        wal.append(&*factory, format!("Record {}", i).into_bytes())
            .await
            .unwrap();
    }

    // Read from LSN 5
    let records = wal.read_from(&*factory, 5).await.unwrap();
    assert_eq!(records.len(), 6); // Records 5-10

    assert_eq!(records[0].lsn, 5);
    assert_eq!(records[5].lsn, 10);

    wal.close().await.unwrap();
    cleanup_dir(&dir);
}

#[tokio::test]
async fn test_record_crc_validation() {
    // Test that CRC validation catches corruption
    let data = b"Test data for CRC".to_vec();
    let record = Record::new(42, data).unwrap();
    let mut encoded = record.encode();

    // Corrupt the data portion
    encoded[HEADER_SIZE] ^= 0xFF;

    // Decoding should fail with CRC mismatch
    let result = Record::decode(&encoded);
    assert!(result.is_err());
    assert!(format!("{:?}", result.unwrap_err()).contains("CrcMismatch"));
}

#[tokio::test]
async fn test_record_types() {
    // Test checkpoint record
    let checkpoint = Record::checkpoint(100);
    let encoded = checkpoint.encode();
    let decoded = Record::decode(&encoded).unwrap();
    assert_eq!(decoded.record_type, RecordType::Checkpoint);
    assert_eq!(decoded.lsn, 100);
    assert!(decoded.data.is_empty());

    // Test segment end record
    let segment_end = Record::segment_end(200);
    let encoded = segment_end.encode();
    let decoded = Record::decode(&encoded).unwrap();
    assert_eq!(decoded.record_type, RecordType::SegmentEnd);
    assert_eq!(decoded.lsn, 200);
    assert!(decoded.data.is_empty());
}

#[tokio::test]
async fn test_posix_wal_large_records() {
    let dir = test_dir("posix_large");
    cleanup_dir(&dir);

    let factory = Arc::new(PosixIOFactory);
    let config = WalConfig {
        dir: dir.clone(),
        segment_size: 1024 * 1024,
        sync_on_write: true,
    };

    let wal = WalManager::new(factory.clone(), config).await.unwrap();

    // Write records of varying sizes
    let sizes = [100, 1000, 10000, 50000];
    for (i, &size) in sizes.iter().enumerate() {
        let data = vec![i as u8; size];
        let lsn = wal.append(&*factory, data).await.unwrap();
        assert_eq!(lsn, (i + 1) as u64);
    }

    // Read back and verify
    let records = wal.read_from(&*factory, 1).await.unwrap();
    assert_eq!(records.len(), 4);

    for (i, record) in records.iter().enumerate() {
        assert_eq!(record.data.len(), sizes[i]);
        assert!(record.data.iter().all(|&b| b == i as u8));
    }

    wal.close().await.unwrap();
    cleanup_dir(&dir);
}

// ============ io_uring WAL Tests (Linux only) ============

#[cfg(target_os = "linux")]
#[tokio::test]
async fn test_uring_wal_append_single() {
    let dir = test_dir("uring_append_single");
    cleanup_dir(&dir);

    let factory = Arc::new(UringIOFactory);
    let config = WalConfig {
        dir: dir.clone(),
        segment_size: 1024 * 1024,
        sync_on_write: true,
    };

    let wal = WalManager::new(factory.clone(), config).await.unwrap();

    // Append a record
    let data = b"Hello, io_uring WAL!".to_vec();
    let lsn = wal.append(&*factory, data.clone()).await.unwrap();
    assert_eq!(lsn, 1);

    // Read back
    let records = wal.read_from(&*factory, 1).await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].lsn, 1);
    assert_eq!(records[0].data, data);

    wal.close().await.unwrap();
    cleanup_dir(&dir);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn test_uring_wal_append_multiple() {
    let dir = test_dir("uring_append_multi");
    cleanup_dir(&dir);

    let factory = Arc::new(UringIOFactory);
    let config = WalConfig {
        dir: dir.clone(),
        segment_size: 1024 * 1024,
        sync_on_write: true,
    };

    let wal = WalManager::new(factory.clone(), config).await.unwrap();

    // Append multiple records
    for i in 1..=10 {
        let data = format!("io_uring Record {}", i).into_bytes();
        let lsn = wal.append(&*factory, data).await.unwrap();
        assert_eq!(lsn, i);
    }

    // Read all back
    let records = wal.read_from(&*factory, 1).await.unwrap();
    assert_eq!(records.len(), 10);

    for (i, record) in records.iter().enumerate() {
        let expected_lsn = (i + 1) as u64;
        assert_eq!(record.lsn, expected_lsn);
    }

    wal.close().await.unwrap();
    cleanup_dir(&dir);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn test_uring_wal_recovery() {
    let dir = test_dir("uring_recovery");
    cleanup_dir(&dir);

    let factory = Arc::new(UringIOFactory);
    let config = WalConfig {
        dir: dir.clone(),
        segment_size: 1024 * 1024,
        sync_on_write: true,
    };

    // Write some records
    {
        let wal = WalManager::new(factory.clone(), config.clone())
            .await
            .unwrap();
        for i in 1..=5 {
            wal.append(&*factory, format!("Uring Data {}", i).into_bytes())
                .await
                .unwrap();
        }
        wal.close().await.unwrap();
    }

    // Reopen and verify recovery
    {
        let wal = WalManager::new(factory.clone(), config).await.unwrap();
        assert_eq!(wal.current_lsn(), 6);

        let records = wal.read_from(&*factory, 1).await.unwrap();
        assert_eq!(records.len(), 5);

        wal.close().await.unwrap();
    }

    cleanup_dir(&dir);
}
