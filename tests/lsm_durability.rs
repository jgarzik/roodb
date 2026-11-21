//! What the LSM must guarantee about data that has been flushed, and about
//! reopening after a crash.
//!
//! "Crash" here means dropping the engine without `close()`: whatever was only
//! in memory is gone and the directory is left exactly as the last completed
//! write left it.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use roodb::io::{IoBackend, IoEngine};
use roodb::storage::lsm::engine::LsmConfig;
use roodb::storage::lsm::LsmEngine;
use roodb::storage::StorageEngine;

fn io() -> IoEngine {
    IoEngine::new(IoBackend::from_env().unwrap()).unwrap()
}

fn config(dir: &Path) -> LsmConfig {
    LsmConfig {
        dir: dir.to_path_buf(),
        ..Default::default()
    }
}

/// A write that overlaps a flush used to land in a memtable that had already
/// been written out and discarded.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn writes_racing_with_flush_are_not_lost() {
    const WRITERS: usize = 8;
    const PER_WRITER: usize = 4000;

    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(LsmEngine::open(io(), config(dir.path())).await.unwrap());
    let stop = Arc::new(AtomicBool::new(false));

    let flusher = {
        let engine = engine.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                engine.flush().await.unwrap();
                tokio::task::yield_now().await;
            }
        })
    };
    let writers: Vec<_> = (0..WRITERS)
        .map(|w| {
            let engine = engine.clone();
            tokio::spawn(async move {
                for i in 0..PER_WRITER {
                    let key = format!("w{w}-{i:06}");
                    engine.put(key.as_bytes(), key.as_bytes()).await.unwrap();
                    if i % 64 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
            })
        })
        .collect();
    for writer in writers {
        writer.await.unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    flusher.await.unwrap();

    for w in 0..WRITERS {
        for i in 0..PER_WRITER {
            let key = format!("w{w}-{i:06}");
            assert_eq!(
                engine.get(key.as_bytes()).await.unwrap(),
                Some(key.clone().into_bytes()),
                "{key} was acknowledged and then lost"
            );
        }
    }
    engine.close().await.unwrap();
}

/// A batch is applied as a unit: after a crash the files hold either all of a
/// batch or none of it, never part.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_batch_is_never_split_by_a_flush() {
    const BATCHES: usize = 3000;

    let dir = tempfile::tempdir().unwrap();
    {
        let engine = Arc::new(LsmEngine::open(io(), config(dir.path())).await.unwrap());
        let stop = Arc::new(AtomicBool::new(false));
        let flusher = {
            let engine = engine.clone();
            let stop = stop.clone();
            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    engine.flush().await.unwrap();
                    tokio::task::yield_now().await;
                }
            })
        };
        for i in 0..BATCHES {
            let batch = vec![
                (format!("a{i:06}").into_bytes(), Some(vec![1u8; 100])),
                (format!("m{i:06}").into_bytes(), Some(vec![2u8; 100])),
                (format!("z{i:06}").into_bytes(), Some(vec![3u8; 100])),
                // The marker written by the previous batch is removed by this one.
                (
                    format!("marker{:06}", i.saturating_sub(1)).into_bytes(),
                    None,
                ),
                (format!("marker{i:06}").into_bytes(), Some(b"last".to_vec())),
            ];
            engine.write_batch(batch).await.unwrap();
            if i % 16 == 0 {
                tokio::task::yield_now().await;
            }
        }
        stop.store(true, Ordering::Relaxed);
        flusher.await.unwrap();
        // Crash: no close(), the memtable is lost.
    }

    let engine = LsmEngine::open(io(), config(dir.path())).await.unwrap();
    let mut complete = 0;
    for i in 0..BATCHES {
        let a = engine.get(format!("a{i:06}").as_bytes()).await.unwrap();
        let m = engine.get(format!("m{i:06}").as_bytes()).await.unwrap();
        let z = engine.get(format!("z{i:06}").as_bytes()).await.unwrap();
        assert_eq!(a.is_some(), m.is_some(), "batch {i} was split");
        assert_eq!(a.is_some(), z.is_some(), "batch {i} was split");
        if a.is_some() {
            complete += 1;
        }
    }
    assert!(complete > 0, "nothing was flushed; the test proved nothing");
    // Batches are applied in order, so what survives is a prefix and exactly
    // one marker - that of the last surviving batch - is present.
    let markers = engine.scan(Some(b"marker"), Some(b"markes")).await.unwrap();
    assert_eq!(markers.len(), 1, "markers: {markers:?}");
    assert_eq!(
        markers[0].0,
        format!("marker{:06}", complete - 1).into_bytes()
    );
    engine.close().await.unwrap();
}
