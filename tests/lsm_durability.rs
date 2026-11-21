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
use roodb::storage::lsm::engine::{CrashPoint, LsmConfig};
use roodb::storage::lsm::LsmEngine;
use roodb::storage::{StorageEngine, StorageError};

fn io() -> IoEngine {
    IoEngine::new(IoBackend::from_env().unwrap()).unwrap()
}

fn config(dir: &Path) -> LsmConfig {
    LsmConfig {
        dir: dir.to_path_buf(),
        ..Default::default()
    }
}

fn sst_files(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|n| n.ends_with(".sst"))
        .collect();
    names.sort();
    names
}

fn value_of(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| seed.wrapping_add((i % 251) as u8))
        .collect()
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

/// Values larger than one block used to vanish at flush: the block builder
/// refused them and the writer ignored the refusal.
#[tokio::test]
async fn large_values_survive_flush_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let sizes = [
        1usize, 4000, 4084, 4085, 4096, 5000, 65_534, 65_535, 70_000, 1_000_000,
    ];

    let engine = LsmEngine::open(io(), config(dir.path())).await.unwrap();
    for (i, size) in sizes.iter().enumerate() {
        let key = format!("big{i:02}");
        engine
            .put(key.as_bytes(), &value_of(*size, i as u8))
            .await
            .unwrap();
        // Small neighbours on both sides share blocks with the big entries.
        engine
            .put(format!("big{i:02}a").as_bytes(), b"after")
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();

    for (i, size) in sizes.iter().enumerate() {
        let got = engine.get(format!("big{i:02}").as_bytes()).await.unwrap();
        assert_eq!(
            got,
            Some(value_of(*size, i as u8)),
            "size {size} after flush"
        );
    }
    engine.close().await.unwrap();
    drop(engine);

    let engine = LsmEngine::open(io(), config(dir.path())).await.unwrap();
    for (i, size) in sizes.iter().enumerate() {
        let got = engine.get(format!("big{i:02}").as_bytes()).await.unwrap();
        assert_eq!(
            got,
            Some(value_of(*size, i as u8)),
            "size {size} after reopen"
        );
        let small = engine.get(format!("big{i:02}a").as_bytes()).await.unwrap();
        assert_eq!(small, Some(b"after".to_vec()), "neighbour of size {size}");
    }
    let all = engine.scan(None, None).await.unwrap();
    assert_eq!(all.len(), sizes.len() * 2);
    engine.close().await.unwrap();
}

/// A key the on-disk format cannot hold is refused when it is written, not
/// dropped later.
#[tokio::test]
async fn oversized_keys_are_rejected_at_write_time() {
    let dir = tempfile::tempdir().unwrap();
    let engine = LsmEngine::open(io(), config(dir.path())).await.unwrap();

    let ok_key = vec![b'k'; 4096];
    engine.put(&ok_key, b"v").await.unwrap();

    let huge_key = vec![b'k'; 70_000];
    let err = engine.put(&huge_key, b"v").await.unwrap_err();
    assert!(matches!(err, StorageError::KeyTooLarge { .. }), "{err}");
    let err = engine
        .write_batch(vec![(huge_key.clone(), Some(b"v".to_vec()))])
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::KeyTooLarge { .. }), "{err}");
    let err = engine.delete(&huge_key).await.unwrap_err();
    assert!(matches!(err, StorageError::KeyTooLarge { .. }), "{err}");

    engine.flush().await.unwrap();
    assert_eq!(engine.get(&ok_key).await.unwrap(), Some(b"v".to_vec()));
    engine.close().await.unwrap();
}

/// A table whose contents are damaged is detected, not parsed.
#[tokio::test]
async fn a_corrupted_sstable_is_detected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = LsmEngine::open(io(), config(dir.path())).await.unwrap();
    for i in 0..2000 {
        engine
            .put(format!("key{i:05}").as_bytes(), &[7u8; 64])
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    engine.close().await.unwrap();
    drop(engine);

    let files = sst_files(dir.path());
    let path = dir.path().join(&files[0]);
    let original = std::fs::read(&path).unwrap();

    // Damage the footer (the last block of the file).
    let mut damaged = original.clone();
    let at = damaged.len() - 2000;
    damaged[at] ^= 0xFF;
    std::fs::write(&path, &damaged).unwrap();
    assert!(
        LsmEngine::open(io(), config(dir.path())).await.is_err(),
        "a damaged footer went unnoticed"
    );

    // Damage a data block: opening may succeed, reading that block must not.
    let mut damaged = original.clone();
    damaged[100] ^= 0xFF;
    std::fs::write(&path, &damaged).unwrap();
    if let Ok(engine) = LsmEngine::open(io(), config(dir.path())).await {
        assert!(
            engine.get(b"key00000").await.is_err(),
            "a damaged block was served"
        );
    }
}

/// A table the manifest lists but the directory lacks means data is gone.
/// Opening must fail rather than serve what is left.
#[tokio::test]
async fn a_missing_sstable_fails_open() {
    let dir = tempfile::tempdir().unwrap();
    let engine = LsmEngine::open(io(), config(dir.path())).await.unwrap();
    engine.put(b"k", b"v").await.unwrap();
    engine.flush().await.unwrap();
    engine.close().await.unwrap();
    drop(engine);

    let files = sst_files(dir.path());
    assert_eq!(files.len(), 1);
    std::fs::remove_file(dir.path().join(&files[0])).unwrap();

    let result = LsmEngine::open(io(), config(dir.path())).await;
    assert!(result.is_err(), "open succeeded with a table missing");
}

/// After a crash an SSTable name must not be handed out twice: a file left
/// behind by the crashed process may still be lying there under that name.
#[tokio::test]
async fn files_left_by_a_crash_are_removed_and_never_adopted() {
    let dir = tempfile::tempdir().unwrap();

    let engine = LsmEngine::open(io(), config(dir.path())).await.unwrap();
    for i in 0..100 {
        engine
            .put(format!("first{i:04}").as_bytes(), b"one")
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    engine.close().await.unwrap();
    drop(engine);
    let live = sst_files(dir.path());
    assert_eq!(live.len(), 1);

    // What a crash mid-flush leaves: files the manifest never heard of, under
    // the names the next flushes would have used, longer than what will be
    // written next, plus a half-written manifest.
    for seq in 0..64u64 {
        let name = format!("{seq:06}.sst");
        if live.contains(&name) {
            continue;
        }
        std::fs::write(dir.path().join(name), vec![0xA5u8; 256 * 1024]).unwrap();
    }
    std::fs::write(dir.path().join("MANIFEST.tmp"), b"{ \"sequ").unwrap();

    let engine = LsmEngine::open(io(), config(dir.path())).await.unwrap();
    assert_eq!(
        sst_files(dir.path()),
        live,
        "open() should delete files the manifest does not list"
    );
    assert!(!dir.path().join("MANIFEST.tmp").exists());

    for i in 0..100 {
        engine
            .put(format!("second{i:04}").as_bytes(), b"two")
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    engine.close().await.unwrap();
    drop(engine);

    let engine = LsmEngine::open(io(), config(dir.path())).await.unwrap();
    for i in 0..100 {
        assert_eq!(
            engine.get(format!("first{i:04}").as_bytes()).await.unwrap(),
            Some(b"one".to_vec())
        );
        assert_eq!(
            engine
                .get(format!("second{i:04}").as_bytes())
                .await
                .unwrap(),
            Some(b"two".to_vec())
        );
    }
    engine.close().await.unwrap();
}

/// A delete must keep hiding older versions that sit in deeper levels. The
/// tombstone used to be discarded as soon as it was compacted once.
#[tokio::test]
async fn a_deleted_key_stays_deleted_through_compaction() {
    let dir = tempfile::tempdir().unwrap();
    // Tiny levels, so a handful of flushes pushes data several levels down.
    let cfg = LsmConfig {
        l0_compaction_trigger: 2,
        level_base_bytes: 16 * 1024,
        ..config(dir.path())
    };
    let engine = LsmEngine::open(io(), cfg.clone()).await.unwrap();

    engine.put(b"victim", b"old value").await.unwrap();
    for round in 0..12 {
        for i in 0..200 {
            engine
                .put(format!("fill{round:02}-{i:04}").as_bytes(), &[9u8; 100])
                .await
                .unwrap();
        }
        engine.flush().await.unwrap();
    }
    assert_eq!(
        engine.get(b"victim").await.unwrap(),
        Some(b"old value".to_vec())
    );

    engine.delete(b"victim").await.unwrap();
    for round in 12..24 {
        for i in 0..200 {
            engine
                .put(format!("fill{round:02}-{i:04}").as_bytes(), &[9u8; 100])
                .await
                .unwrap();
        }
        engine.flush().await.unwrap();
        assert_eq!(
            engine.get(b"victim").await.unwrap(),
            None,
            "deleted key came back after flush round {round}"
        );
    }
    let hits = engine.scan(Some(b"victim"), Some(b"victin")).await.unwrap();
    assert!(hits.is_empty(), "scan returned the deleted key");
    engine.close().await.unwrap();
    drop(engine);

    let engine = LsmEngine::open(io(), cfg).await.unwrap();
    assert_eq!(engine.get(b"victim").await.unwrap(), None);
    assert_eq!(
        engine.get(b"fill00-0000").await.unwrap(),
        Some(vec![9u8; 100])
    );
    engine.close().await.unwrap();
}

/// Stop a flush or a compaction at each point where a crash could leave the
/// directory in a distinct state, then reopen. Everything flushed before the
/// crash must still be there, and the engine must keep working.
#[tokio::test]
async fn reopening_after_a_crash_at_any_point_loses_nothing_flushed() {
    for point in [
        CrashPoint::FlushAfterTableWritten,
        CrashPoint::CompactionAfterOutputWritten,
        CrashPoint::CompactionAfterManifestSaved,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let base = LsmConfig {
            l0_compaction_trigger: 4,
            ..config(dir.path())
        };

        // Three clean flushes: three tables in level 0, one short of the
        // compaction trigger.
        let engine = LsmEngine::open(io(), base.clone()).await.unwrap();
        for round in 0..2 {
            for i in 0..300 {
                engine
                    .put(format!("r{round}-{i:04}").as_bytes(), &[round as u8; 80])
                    .await
                    .unwrap();
            }
            engine.flush().await.unwrap();
        }
        engine.put(b"r0-0000", b"overwritten").await.unwrap();
        engine.delete(b"r0-0001").await.unwrap();
        engine.flush().await.unwrap();
        engine.close().await.unwrap();
        drop(engine);

        // One more round with the crash armed. The flush, or the compaction
        // its table triggers, stops part-way with an error.
        let armed = LsmConfig {
            crash_point: Some(point),
            ..base.clone()
        };
        let engine = LsmEngine::open(io(), armed).await.unwrap();
        for i in 0..300 {
            engine
                .put(format!("r2-{i:04}").as_bytes(), &[2u8; 80])
                .await
                .unwrap();
        }
        let _ = engine.flush().await;
        drop(engine);

        let engine = LsmEngine::open(io(), base).await.unwrap_or_else(|e| {
            panic!("{point:?}: reopen failed: {e}");
        });
        assert_eq!(
            engine.get(b"r0-0000").await.unwrap(),
            Some(b"overwritten".to_vec()),
            "{point:?}"
        );
        assert_eq!(engine.get(b"r0-0001").await.unwrap(), None, "{point:?}");
        for round in 0..2u8 {
            for i in 2..300 {
                assert_eq!(
                    engine
                        .get(format!("r{round}-{i:04}").as_bytes())
                        .await
                        .unwrap(),
                    Some(vec![round; 80]),
                    "{point:?}: r{round}-{i:04}"
                );
            }
        }
        // And it still takes writes and flushes.
        engine.put(b"after-crash", b"ok").await.unwrap();
        engine.flush().await.unwrap();
        assert_eq!(
            engine.get(b"after-crash").await.unwrap(),
            Some(b"ok".to_vec()),
            "{point:?}"
        );
        engine.close().await.unwrap();
    }
}
