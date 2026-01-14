//! Integration tests for the storage engine

use std::path::PathBuf;
use std::sync::Arc;

use roodb::io::PosixIOFactory;
use roodb::storage::lsm::{LsmEngine};
use roodb::storage::lsm::engine::LsmConfig;
use roodb::storage::StorageEngine;

#[cfg(target_os = "linux")]
use roodb::io::UringIOFactory;

fn test_dir(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("roodb_storage_test_{}_{}", name, std::process::id()));
    path
}

fn cleanup_dir(path: &PathBuf) {
    let _ = std::fs::remove_dir_all(path);
}

// ============ Memtable Unit Tests ============

use roodb::storage::lsm::memtable::Memtable;

#[test]
fn test_memtable_put_get() {
    let mem = Memtable::new();

    mem.put(b"key1", b"value1");
    mem.put(b"key2", b"value2");
    mem.put(b"key3", b"value3");

    assert_eq!(mem.get(b"key1"), Some(Some(b"value1".to_vec())));
    assert_eq!(mem.get(b"key2"), Some(Some(b"value2".to_vec())));
    assert_eq!(mem.get(b"key3"), Some(Some(b"value3".to_vec())));
    assert_eq!(mem.get(b"key4"), None);
}

#[test]
fn test_memtable_delete() {
    let mem = Memtable::new();

    mem.put(b"key1", b"value1");
    assert_eq!(mem.get(b"key1"), Some(Some(b"value1".to_vec())));

    mem.delete(b"key1");
    // Should return Some(None) for tombstone
    assert_eq!(mem.get(b"key1"), Some(None));
}

#[test]
fn test_memtable_scan() {
    let mem = Memtable::new();

    mem.put(b"a", b"1");
    mem.put(b"b", b"2");
    mem.put(b"c", b"3");
    mem.put(b"d", b"4");
    mem.put(b"e", b"5");

    // Scan range [b, d)
    let result = mem.scan(Some(b"b"), Some(b"d"));
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].0, b"b".to_vec());
    assert_eq!(result[1].0, b"c".to_vec());

    // Scan all
    let all = mem.scan(None, None);
    assert_eq!(all.len(), 5);
}

// ============ Block Unit Tests ============

use roodb::storage::lsm::block::{BlockBuilder, BlockReader, Entry};

#[test]
fn test_block_write_read() {
    let mut builder = BlockBuilder::new();

    builder.add(Entry::new(b"apple".to_vec(), b"red".to_vec()));
    builder.add(Entry::new(b"banana".to_vec(), b"yellow".to_vec()));
    builder.add(Entry::new(b"cherry".to_vec(), b"red".to_vec()));

    let block = builder.build();
    let reader = BlockReader::parse(&block, 0).unwrap();

    assert_eq!(reader.entries().len(), 3);

    let apple = reader.get(b"apple").unwrap();
    assert_eq!(apple.value, Some(b"red".to_vec()));

    let banana = reader.get(b"banana").unwrap();
    assert_eq!(banana.value, Some(b"yellow".to_vec()));

    assert!(reader.get(b"grape").is_none());
}

#[test]
fn test_block_tombstone() {
    let mut builder = BlockBuilder::new();

    builder.add(Entry::new(b"key1".to_vec(), b"value1".to_vec()));
    builder.add(Entry::tombstone(b"key2".to_vec()));

    let block = builder.build();
    let reader = BlockReader::parse(&block, 0).unwrap();

    let entry = reader.get(b"key2").unwrap();
    assert!(entry.is_tombstone());
}

// ============ SSTable Tests ============

use roodb::storage::lsm::sstable::{SstableReader, SstableWriter};

#[tokio::test]
async fn test_sstable_write_read() {
    let dir = test_dir("sstable_basic");
    cleanup_dir(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let factory = PosixIOFactory;
    let path = dir.join("test.sst");

    // Write SSTable
    {
        let mut writer = SstableWriter::create(&factory, &path).await.unwrap();

        writer.add(b"apple".to_vec(), Some(b"red".to_vec())).await.unwrap();
        writer.add(b"banana".to_vec(), Some(b"yellow".to_vec())).await.unwrap();
        writer.add(b"cherry".to_vec(), Some(b"red".to_vec())).await.unwrap();
        writer.add(b"date".to_vec(), None).await.unwrap(); // Tombstone

        writer.finish().await.unwrap();
    }

    // Read SSTable
    {
        let reader = SstableReader::open(&factory, &path).await.unwrap();

        // Point lookups
        let apple = reader.get(b"apple").await.unwrap();
        assert_eq!(apple, Some(Some(b"red".to_vec())));

        let banana = reader.get(b"banana").await.unwrap();
        assert_eq!(banana, Some(Some(b"yellow".to_vec())));

        let date = reader.get(b"date").await.unwrap();
        assert_eq!(date, Some(None)); // Tombstone

        let notfound = reader.get(b"grape").await.unwrap();
        assert_eq!(notfound, None);

        // Scan all
        let all = reader.scan().await.unwrap();
        assert_eq!(all.len(), 4);
    }

    cleanup_dir(&dir);
}

#[tokio::test]
async fn test_sstable_large() {
    let dir = test_dir("sstable_large");
    cleanup_dir(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let factory = PosixIOFactory;
    let path = dir.join("large.sst");

    // Write many entries (enough to span multiple blocks)
    {
        let mut writer = SstableWriter::create(&factory, &path).await.unwrap();

        for i in 0..1000u32 {
            let key = format!("key{:06}", i);
            let value = format!("value{:06}", i);
            writer.add(key.into_bytes(), Some(value.into_bytes())).await.unwrap();
        }

        writer.finish().await.unwrap();
    }

    // Read back and verify
    {
        let reader = SstableReader::open(&factory, &path).await.unwrap();

        // Spot check some entries
        let key500 = reader.get(b"key000500").await.unwrap();
        assert_eq!(key500, Some(Some(b"value000500".to_vec())));

        let key999 = reader.get(b"key000999").await.unwrap();
        assert_eq!(key999, Some(Some(b"value000999".to_vec())));

        // Scan should return all entries
        let all = reader.scan().await.unwrap();
        assert_eq!(all.len(), 1000);
    }

    cleanup_dir(&dir);
}

// ============ LSM Engine Tests ============

#[tokio::test]
async fn test_lsm_put_get() {
    let dir = test_dir("lsm_basic");
    cleanup_dir(&dir);

    let factory = Arc::new(PosixIOFactory);
    let config = LsmConfig { dir: dir.clone() };

    let engine = LsmEngine::open(factory, config).await.unwrap();

    // Put and get
    engine.put(b"key1", b"value1").await.unwrap();
    engine.put(b"key2", b"value2").await.unwrap();

    let v1 = engine.get(b"key1").await.unwrap();
    assert_eq!(v1, Some(b"value1".to_vec()));

    let v2 = engine.get(b"key2").await.unwrap();
    assert_eq!(v2, Some(b"value2".to_vec()));

    let v3 = engine.get(b"key3").await.unwrap();
    assert_eq!(v3, None);

    engine.close().await.unwrap();
    cleanup_dir(&dir);
}

#[tokio::test]
async fn test_lsm_delete() {
    let dir = test_dir("lsm_delete");
    cleanup_dir(&dir);

    let factory = Arc::new(PosixIOFactory);
    let config = LsmConfig { dir: dir.clone() };

    let engine = LsmEngine::open(factory, config).await.unwrap();

    engine.put(b"key1", b"value1").await.unwrap();
    assert_eq!(engine.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));

    engine.delete(b"key1").await.unwrap();
    assert_eq!(engine.get(b"key1").await.unwrap(), None);

    engine.close().await.unwrap();
    cleanup_dir(&dir);
}

#[tokio::test]
async fn test_lsm_scan() {
    let dir = test_dir("lsm_scan");
    cleanup_dir(&dir);

    let factory = Arc::new(PosixIOFactory);
    let config = LsmConfig { dir: dir.clone() };

    let engine = LsmEngine::open(factory, config).await.unwrap();

    engine.put(b"a", b"1").await.unwrap();
    engine.put(b"b", b"2").await.unwrap();
    engine.put(b"c", b"3").await.unwrap();
    engine.put(b"d", b"4").await.unwrap();
    engine.put(b"e", b"5").await.unwrap();

    // Scan range [b, d)
    let result = engine.scan(Some(b"b"), Some(b"d")).await.unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0], (b"b".to_vec(), b"2".to_vec()));
    assert_eq!(result[1], (b"c".to_vec(), b"3".to_vec()));

    // Scan all
    let all = engine.scan(None, None).await.unwrap();
    assert_eq!(all.len(), 5);

    engine.close().await.unwrap();
    cleanup_dir(&dir);
}

#[tokio::test]
async fn test_lsm_flush() {
    let dir = test_dir("lsm_flush");
    cleanup_dir(&dir);

    let factory = Arc::new(PosixIOFactory);
    let config = LsmConfig { dir: dir.clone() };

    let engine = LsmEngine::open(factory, config).await.unwrap();

    // Write some data
    for i in 0..100 {
        let key = format!("key{:04}", i);
        let value = format!("value{:04}", i);
        engine.put(key.as_bytes(), value.as_bytes()).await.unwrap();
    }

    // Flush to disk
    engine.flush().await.unwrap();

    // Verify data is still readable
    let v50 = engine.get(b"key0050").await.unwrap();
    assert_eq!(v50, Some(b"value0050".to_vec()));

    engine.close().await.unwrap();
    cleanup_dir(&dir);
}

#[tokio::test]
async fn test_lsm_recovery() {
    let dir = test_dir("lsm_recovery");
    cleanup_dir(&dir);

    let factory = Arc::new(PosixIOFactory);
    let config = LsmConfig { dir: dir.clone() };

    // Write and flush
    {
        let engine = LsmEngine::open(factory.clone(), config.clone()).await.unwrap();

        for i in 0..50 {
            let key = format!("key{:04}", i);
            let value = format!("value{:04}", i);
            engine.put(key.as_bytes(), value.as_bytes()).await.unwrap();
        }

        engine.flush().await.unwrap();
        engine.close().await.unwrap();
    }

    // Reopen and verify data persisted
    {
        let engine = LsmEngine::open(factory, config).await.unwrap();

        let v0 = engine.get(b"key0000").await.unwrap();
        assert_eq!(v0, Some(b"value0000".to_vec()));

        let v49 = engine.get(b"key0049").await.unwrap();
        assert_eq!(v49, Some(b"value0049".to_vec()));

        // Scan should return all
        let all = engine.scan(None, None).await.unwrap();
        assert_eq!(all.len(), 50);

        engine.close().await.unwrap();
    }

    cleanup_dir(&dir);
}

// ============ io_uring Tests (Linux only) ============

#[cfg(target_os = "linux")]
#[tokio::test]
async fn test_uring_lsm_basic() {
    let dir = test_dir("uring_lsm");
    cleanup_dir(&dir);

    let factory = Arc::new(UringIOFactory);
    let config = LsmConfig { dir: dir.clone() };

    let engine = LsmEngine::open(factory, config).await.unwrap();

    engine.put(b"key1", b"value1").await.unwrap();
    engine.put(b"key2", b"value2").await.unwrap();

    assert_eq!(engine.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
    assert_eq!(engine.get(b"key2").await.unwrap(), Some(b"value2".to_vec()));

    engine.close().await.unwrap();
    cleanup_dir(&dir);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn test_uring_sstable_write_read() {
    let dir = test_dir("uring_sstable");
    cleanup_dir(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let factory = UringIOFactory;
    let path = dir.join("test.sst");

    // Write SSTable
    {
        let mut writer = SstableWriter::create(&factory, &path).await.unwrap();

        for i in 0..100u32 {
            let key = format!("key{:04}", i);
            let value = format!("val{:04}", i);
            writer.add(key.into_bytes(), Some(value.into_bytes())).await.unwrap();
        }

        writer.finish().await.unwrap();
    }

    // Read SSTable
    {
        let reader = SstableReader::open(&factory, &path).await.unwrap();

        let v50 = reader.get(b"key0050").await.unwrap();
        assert_eq!(v50, Some(Some(b"val0050".to_vec())));

        let all = reader.scan().await.unwrap();
        assert_eq!(all.len(), 100);
    }

    cleanup_dir(&dir);
}
