//! Tests for the block I/O engine, run against every backend this platform has.

use std::time::Duration;

use roodb::io::{AlignedBuffer, IoBackend, IoEngine, OpenOptions, Urgency, PAGE_SIZE};

fn backends() -> Vec<IoBackend> {
    let mut all = vec![IoBackend::Posix];
    if cfg!(target_os = "linux") {
        all.push(IoBackend::Uring);
    }
    all
}

fn page_of(byte: u8) -> AlignedBuffer {
    let mut buf = AlignedBuffer::page().unwrap();
    buf.copy_from_slice(&[byte; PAGE_SIZE]).unwrap();
    buf
}

#[tokio::test]
async fn auto_resolves_to_a_real_backend() {
    let engine = IoEngine::new(IoBackend::Auto).unwrap();
    assert_ne!(engine.backend(), IoBackend::Auto);
}

#[test]
fn backend_names_parse() {
    assert_eq!("uring".parse::<IoBackend>().unwrap(), IoBackend::Uring);
    assert_eq!("io_uring".parse::<IoBackend>().unwrap(), IoBackend::Uring);
    assert_eq!("POSIX".parse::<IoBackend>().unwrap(), IoBackend::Posix);
    assert_eq!("auto".parse::<IoBackend>().unwrap(), IoBackend::Auto);
    assert!("aio".parse::<IoBackend>().is_err());
}

#[tokio::test]
async fn write_sync_read_roundtrip() {
    for backend in backends() {
        let dir = tempfile::tempdir().unwrap();
        let engine = IoEngine::new(backend).unwrap();
        assert_eq!(engine.backend(), backend);
        let file = engine
            .open(&dir.path().join("f"), OpenOptions::create_new())
            .await
            .unwrap();

        let mut buf = AlignedBuffer::page().unwrap();
        buf.copy_from_slice(b"Hello, RooDB!").unwrap();
        let buf = file.write_at(buf, 0, Urgency::Foreground).await.unwrap();
        assert_eq!(&buf[..], b"Hello, RooDB!", "{backend}: buffer handed back");
        file.sync_data(Urgency::Critical).await.unwrap();
        file.sync_all(Urgency::Critical).await.unwrap();

        let read = file
            .read_at(AlignedBuffer::page().unwrap(), 0, Urgency::Foreground)
            .await
            .unwrap();
        assert_eq!(read.len(), PAGE_SIZE, "{backend}");
        assert_eq!(&read[..13], b"Hello, RooDB!", "{backend}");
        assert!(read[13..].iter().all(|b| *b == 0), "{backend}: zero padded");
        assert_eq!(file.size().unwrap(), PAGE_SIZE as u64, "{backend}");
    }
}

#[tokio::test]
async fn padding_does_not_leak_stale_buffer_contents() {
    for backend in backends() {
        let dir = tempfile::tempdir().unwrap();
        let engine = IoEngine::new(backend).unwrap();
        let file = engine
            .open(&dir.path().join("f"), OpenOptions::create_new())
            .await
            .unwrap();
        // A reused buffer still holds old bytes past the new length.
        let mut buf = page_of(0xAB);
        buf.copy_from_slice(b"short").unwrap();
        file.write_at(buf, 0, Urgency::Foreground).await.unwrap();
        let read = file
            .read_at(AlignedBuffer::page().unwrap(), 0, Urgency::Foreground)
            .await
            .unwrap();
        assert_eq!(&read[..5], b"short");
        assert!(read[5..].iter().all(|b| *b == 0), "{backend}");
    }
}

#[tokio::test]
async fn read_stops_at_end_of_file() {
    for backend in backends() {
        let dir = tempfile::tempdir().unwrap();
        let engine = IoEngine::new(backend).unwrap();
        let file = engine
            .open(&dir.path().join("f"), OpenOptions::create_new())
            .await
            .unwrap();
        file.write_at(page_of(1), 0, Urgency::Foreground)
            .await
            .unwrap();
        file.write_at(page_of(2), PAGE_SIZE as u64, Urgency::Foreground)
            .await
            .unwrap();

        // Ask for four pages where only two exist.
        let read = file
            .read_at(AlignedBuffer::pages(4).unwrap(), 0, Urgency::Foreground)
            .await
            .unwrap();
        assert_eq!(read.len(), 2 * PAGE_SIZE, "{backend}");
        assert!(read[..PAGE_SIZE].iter().all(|b| *b == 1));
        assert!(read[PAGE_SIZE..].iter().all(|b| *b == 2));

        // Entirely past the end.
        let read = file
            .read_at(
                AlignedBuffer::page().unwrap(),
                8 * PAGE_SIZE as u64,
                Urgency::Foreground,
            )
            .await
            .unwrap();
        assert_eq!(read.len(), 0, "{backend}");
    }
}

#[tokio::test]
async fn unaligned_offsets_are_rejected() {
    for backend in backends() {
        let dir = tempfile::tempdir().unwrap();
        let engine = IoEngine::new(backend).unwrap();
        let file = engine
            .open(&dir.path().join("f"), OpenOptions::create_new())
            .await
            .unwrap();
        assert!(file
            .write_at(page_of(1), 100, Urgency::Foreground)
            .await
            .is_err());
        assert!(file
            .read_at(AlignedBuffer::page().unwrap(), 1, Urgency::Foreground)
            .await
            .is_err());
    }
}

#[tokio::test]
async fn large_transfer_roundtrip() {
    for backend in backends() {
        let dir = tempfile::tempdir().unwrap();
        let engine = IoEngine::new(backend).unwrap();
        let file = engine
            .open(&dir.path().join("f"), OpenOptions::create_new())
            .await
            .unwrap();
        let pages = 512; // 2 MiB
        let mut buf = AlignedBuffer::pages(pages).unwrap();
        let data: Vec<u8> = (0..pages * PAGE_SIZE).map(|i| (i % 251) as u8).collect();
        buf.copy_from_slice(&data).unwrap();
        file.write_at_sync(buf, 0, Urgency::Critical).await.unwrap();
        let read = file
            .read_at(AlignedBuffer::pages(pages).unwrap(), 0, Urgency::Foreground)
            .await
            .unwrap();
        assert_eq!(&read[..], &data[..], "{backend}");
    }
}

#[tokio::test]
async fn open_options_are_honoured() {
    for backend in backends() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        let engine = IoEngine::new(backend).unwrap();

        assert!(
            engine.open(&path, OpenOptions::existing()).await.is_err(),
            "{backend}: missing file"
        );
        let file = engine.open(&path, OpenOptions::create_new()).await.unwrap();
        file.write_at(page_of(7), 0, Urgency::Foreground)
            .await
            .unwrap();
        drop(file);
        assert!(
            engine.open(&path, OpenOptions::create_new()).await.is_err(),
            "{backend}: create_new on an existing file"
        );

        let file = engine.open(&path, OpenOptions::create()).await.unwrap();
        assert_eq!(file.size().unwrap(), PAGE_SIZE as u64, "{backend}: kept");
        drop(file);

        let file = engine.open(&path, OpenOptions::read_only()).await.unwrap();
        assert!(
            file.write_at(page_of(8), 0, Urgency::Foreground)
                .await
                .is_err(),
            "{backend}: write to a read-only file"
        );
        drop(file);

        let file = engine
            .open(&path, OpenOptions::create_truncate())
            .await
            .unwrap();
        assert_eq!(file.size().unwrap(), 0, "{backend}: truncated");
    }
}

#[tokio::test]
async fn allocate_and_set_len() {
    for backend in backends() {
        let dir = tempfile::tempdir().unwrap();
        let engine = IoEngine::new(backend).unwrap();
        let file = engine
            .open(&dir.path().join("f"), OpenOptions::create_new())
            .await
            .unwrap();
        file.write_at(page_of(9), 0, Urgency::Foreground)
            .await
            .unwrap();
        file.allocate(64 * PAGE_SIZE as u64).await.unwrap();
        assert_eq!(file.size().unwrap(), 64 * PAGE_SIZE as u64, "{backend}");
        let read = file
            .read_at(AlignedBuffer::pages(2).unwrap(), 0, Urgency::Foreground)
            .await
            .unwrap();
        assert!(read[..PAGE_SIZE].iter().all(|b| *b == 9), "{backend}: kept");
        assert!(
            read[PAGE_SIZE..].iter().all(|b| *b == 0),
            "{backend}: zeros"
        );

        file.set_len(PAGE_SIZE as u64).unwrap();
        assert_eq!(file.size().unwrap(), PAGE_SIZE as u64, "{backend}");
    }
}

/// Far more requests than the ring holds, from many tasks at once. The old
/// backend hung when a batch exceeded its 64-entry ring.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn thousands_of_concurrent_requests_complete() {
    const PAGES: usize = 6000;
    for backend in backends() {
        let dir = tempfile::tempdir().unwrap();
        let engine = IoEngine::new(backend).unwrap();
        let file = engine
            .open(&dir.path().join("f"), OpenOptions::create_new())
            .await
            .unwrap();

        let writes: Vec<_> = (0..PAGES)
            .map(|i| {
                let file = file.clone();
                tokio::spawn(async move {
                    let urgency = match i % 3 {
                        0 => Urgency::Critical,
                        1 => Urgency::Foreground,
                        _ => Urgency::Background,
                    };
                    file.write_at(page_of((i % 251) as u8), (i * PAGE_SIZE) as u64, urgency)
                        .await
                        .unwrap();
                })
            })
            .collect();
        tokio::time::timeout(Duration::from_secs(60), async {
            for w in writes {
                w.await.unwrap();
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{backend}: writes hung"));

        let before = engine.stats();
        let reads: Vec<_> = (0..PAGES)
            .map(|i| {
                let file = file.clone();
                tokio::spawn(async move {
                    let buf = file
                        .read_at(
                            AlignedBuffer::page().unwrap(),
                            (i * PAGE_SIZE) as u64,
                            Urgency::Foreground,
                        )
                        .await
                        .unwrap();
                    assert_eq!(buf.len(), PAGE_SIZE);
                    assert!(buf.iter().all(|b| *b == (i % 251) as u8), "page {i}");
                })
            })
            .collect();
        tokio::time::timeout(Duration::from_secs(60), async {
            for r in reads {
                r.await.unwrap();
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{backend}: reads hung"));

        let after = engine.stats();
        assert_eq!(after.ops - before.ops, PAGES as u64, "{backend}");
        assert_eq!(
            after.bytes_read - before.bytes_read,
            (PAGES * PAGE_SIZE) as u64,
            "{backend}"
        );
        if backend == IoBackend::Uring {
            // The point of the reactor: many requests per system call.
            let submits = after.submits - before.submits;
            assert!(
                submits < PAGES as u64 / 2,
                "uring made {submits} submit calls for {PAGES} requests"
            );
        }
    }
}

/// A caller that stops waiting must not disturb the request or the engine: the
/// write still lands, and its buffer and file stay alive until it has.
#[tokio::test]
async fn abandoned_request_still_completes() {
    for backend in backends() {
        let dir = tempfile::tempdir().unwrap();
        let engine = IoEngine::new(backend).unwrap();
        let path = dir.path().join("f");
        let file = engine.open(&path, OpenOptions::create_new()).await.unwrap();

        for i in 0..64u64 {
            // Poll the write once so it is submitted, then drop it.
            let write = file.write_at(page_of(0x5A), i * PAGE_SIZE as u64, Urgency::Foreground);
            let _ = tokio::time::timeout(Duration::ZERO, write).await;
        }
        // Drop the only user-visible handle while requests may be in flight.
        drop(file);

        let file = engine.open(&path, OpenOptions::existing()).await.unwrap();
        file.sync_data(Urgency::Critical).await.unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            if file.size().unwrap() == 64 * PAGE_SIZE as u64 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{backend}: abandoned writes never landed"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let read = file
            .read_at(AlignedBuffer::pages(64).unwrap(), 0, Urgency::Foreground)
            .await
            .unwrap();
        assert!(read.iter().all(|b| *b == 0x5A), "{backend}");
    }
}

#[tokio::test]
async fn files_outliving_the_engine_fail_cleanly() {
    for backend in backends() {
        let dir = tempfile::tempdir().unwrap();
        let engine = IoEngine::new(backend).unwrap();
        let file = engine
            .open(&dir.path().join("f"), OpenOptions::create_new())
            .await
            .unwrap();
        drop(engine);
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            file.write_at(page_of(1), 0, Urgency::Foreground),
        )
        .await
        .unwrap_or_else(|_| panic!("{backend}: request hung after engine shutdown"));
        assert!(result.is_err(), "{backend}");
    }
}

#[tokio::test]
async fn many_files_can_be_open_at_once() {
    for backend in backends() {
        let dir = tempfile::tempdir().unwrap();
        let engine = IoEngine::new(backend).unwrap();
        let mut files = Vec::new();
        for i in 0..300 {
            let file = engine
                .open(&dir.path().join(format!("f{i}")), OpenOptions::create_new())
                .await
                .unwrap();
            file.write_at(page_of(i as u8), 0, Urgency::Foreground)
                .await
                .unwrap();
            files.push(file);
        }
        for (i, file) in files.iter().enumerate() {
            let read = file
                .read_at(AlignedBuffer::page().unwrap(), 0, Urgency::Foreground)
                .await
                .unwrap();
            assert!(read.iter().all(|b| *b == i as u8), "{backend}: file {i}");
        }
        // Close and reopen to recycle registration slots.
        drop(files);
        for i in 0..300 {
            let file = engine
                .open(&dir.path().join(format!("f{i}")), OpenOptions::existing())
                .await
                .unwrap();
            let read = file
                .read_at(AlignedBuffer::page().unwrap(), 0, Urgency::Foreground)
                .await
                .unwrap();
            assert!(read.iter().all(|b| *b == i as u8), "{backend}: reopen {i}");
        }
    }
}
