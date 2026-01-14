//! Integration tests for the IO layer

use std::path::PathBuf;

use roodb::io::{AlignedBuffer, AsyncIO, AsyncIOFactory, PosixIOFactory, PAGE_SIZE};

#[cfg(target_os = "linux")]
use roodb::io::UringIOFactory;

fn test_file_path(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("roodb_io_test_{}_{}", name, std::process::id()));
    path
}

fn cleanup(path: &PathBuf) {
    let _ = std::fs::remove_file(path);
}

// ============ POSIX Tests ============

#[tokio::test]
async fn test_posix_write_read_roundtrip() {
    let path = test_file_path("posix_roundtrip");
    cleanup(&path);

    let factory = PosixIOFactory;
    let io = factory.open(&path, true).await.unwrap();

    // Write data
    let mut write_buf = AlignedBuffer::page().unwrap();
    write_buf.copy_from_slice(b"Hello, RooDB!").unwrap();
    let written = io.write_at(&write_buf, 0).await.unwrap();
    assert!(written > 0);

    // Sync to disk
    io.sync().await.unwrap();

    // Read back
    let mut read_buf = AlignedBuffer::page().unwrap();
    let read = io.read_at(&mut read_buf, 0).await.unwrap();
    assert!(read > 0);
    assert_eq!(&read_buf[..13], b"Hello, RooDB!");

    cleanup(&path);
}

#[tokio::test]
async fn test_posix_multiple_pages() {
    let path = test_file_path("posix_pages");
    cleanup(&path);

    let factory = PosixIOFactory;
    let io = factory.open(&path, true).await.unwrap();

    // Write to multiple pages
    for i in 0..4u8 {
        let mut buf = AlignedBuffer::page().unwrap();
        let data = vec![i; PAGE_SIZE];
        buf.copy_from_slice(&data).unwrap();
        io.write_at(&buf, (i as u64) * PAGE_SIZE as u64)
            .await
            .unwrap();
    }

    io.sync().await.unwrap();

    // Read back and verify
    for i in 0..4u8 {
        let mut buf = AlignedBuffer::page().unwrap();
        io.read_at(&mut buf, (i as u64) * PAGE_SIZE as u64)
            .await
            .unwrap();
        assert!(buf.iter().all(|&b| b == i));
    }

    cleanup(&path);
}

#[tokio::test]
async fn test_posix_file_size() {
    let path = test_file_path("posix_size");
    cleanup(&path);

    let factory = PosixIOFactory;
    let io = factory.open(&path, true).await.unwrap();

    // Initially empty
    let size = io.file_size().await.unwrap();
    assert_eq!(size, 0);

    // Write one page
    let buf = AlignedBuffer::page().unwrap();
    io.write_at(&buf, 0).await.unwrap();
    io.sync().await.unwrap();

    // Truncate to exact size
    io.truncate(PAGE_SIZE as u64).await.unwrap();
    let size = io.file_size().await.unwrap();
    assert_eq!(size, PAGE_SIZE as u64);

    cleanup(&path);
}

#[tokio::test]
async fn test_aligned_buffer_basics() {
    // Test alignment
    let buf = AlignedBuffer::new(100).unwrap();
    assert!((buf.as_ptr() as usize).is_multiple_of(PAGE_SIZE));
    assert_eq!(buf.capacity(), PAGE_SIZE); // Rounded up

    // Test page allocation
    let buf = AlignedBuffer::page().unwrap();
    assert_eq!(buf.capacity(), PAGE_SIZE);

    // Test multi-page allocation
    let buf = AlignedBuffer::pages(3).unwrap();
    assert_eq!(buf.capacity(), PAGE_SIZE * 3);
}

#[tokio::test]
async fn test_aligned_buffer_copy() {
    let mut buf = AlignedBuffer::page().unwrap();

    // Copy small data
    buf.copy_from_slice(b"test data").unwrap();
    assert_eq!(buf.len(), 9);
    assert_eq!(&buf[..9], b"test data");

    // Clear and verify
    buf.clear();
    assert_eq!(buf.len(), 0);

    // Zero fill
    buf.zero();
    assert_eq!(buf.len(), 0);
}

// ============ io_uring Tests (Linux only) ============

#[cfg(target_os = "linux")]
#[tokio::test]
async fn test_uring_write_read_roundtrip() {
    let path = test_file_path("uring_roundtrip");
    cleanup(&path);

    let factory = UringIOFactory;
    let io = factory.open(&path, true).await.unwrap();

    // Write data - need to pad to page size for direct IO
    let mut write_buf = AlignedBuffer::page().unwrap();
    write_buf.copy_from_slice(b"Hello, io_uring!").unwrap();
    // For direct IO, we write the full page
    write_buf.set_len(PAGE_SIZE);
    let written = io.write_at(&write_buf, 0).await.unwrap();
    assert_eq!(written, PAGE_SIZE);

    // Sync to disk
    io.sync().await.unwrap();

    // Read back
    let mut read_buf = AlignedBuffer::page().unwrap();
    let read = io.read_at(&mut read_buf, 0).await.unwrap();
    assert_eq!(read, PAGE_SIZE);
    assert_eq!(&read_buf[..16], b"Hello, io_uring!");

    cleanup(&path);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn test_uring_multiple_pages() {
    let path = test_file_path("uring_pages");
    cleanup(&path);

    let factory = UringIOFactory;
    let io = factory.open(&path, true).await.unwrap();

    // Write to multiple pages
    for i in 0..4u8 {
        let mut buf = AlignedBuffer::page().unwrap();
        let data = vec![i; PAGE_SIZE];
        buf.copy_from_slice(&data).unwrap();
        io.write_at(&buf, (i as u64) * PAGE_SIZE as u64)
            .await
            .unwrap();
    }

    io.sync().await.unwrap();

    // Read back and verify
    for i in 0..4u8 {
        let mut buf = AlignedBuffer::page().unwrap();
        io.read_at(&mut buf, (i as u64) * PAGE_SIZE as u64)
            .await
            .unwrap();
        assert!(buf.iter().all(|&b| b == i));
    }

    cleanup(&path);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn test_uring_file_operations() {
    let path = test_file_path("uring_ops");
    cleanup(&path);

    let factory = UringIOFactory;
    let io = factory.open(&path, true).await.unwrap();

    // Write and truncate
    let mut buf = AlignedBuffer::page().unwrap();
    buf.set_len(PAGE_SIZE);
    io.write_at(&buf, 0).await.unwrap();
    io.sync().await.unwrap();

    io.truncate(PAGE_SIZE as u64).await.unwrap();
    let size = io.file_size().await.unwrap();
    assert_eq!(size, PAGE_SIZE as u64);

    cleanup(&path);
}
