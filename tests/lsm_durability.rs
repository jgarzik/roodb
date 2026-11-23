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
use roodb::storage::traits::WriteOp;
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
                WriteOp::Put(format!("a{i:06}").into_bytes(), vec![1u8; 100]),
                WriteOp::Put(format!("m{i:06}").into_bytes(), vec![2u8; 100]),
                WriteOp::Put(format!("z{i:06}").into_bytes(), vec![3u8; 100]),
                // The marker written by the previous batch is removed by this one.
                WriteOp::Delete(format!("marker{:06}", i.saturating_sub(1)).into_bytes()),
                WriteOp::Put(format!("marker{i:06}").into_bytes(), b"last".to_vec()),
            ];
            engine.write_batch(batch).await.unwrap();
            if i % 16 == 0 {
                tokio::task::yield_now().await;
            }
            // However the two tasks happen to be scheduled, something is
            // flushed while batches are still being written.
            if i == BATCHES / 2 {
                engine.flush().await.unwrap();
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
        .write_batch(vec![WriteOp::Put(huge_key.clone(), b"v".to_vec())])
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

/// Memtables are flushed in the background once they fill; memory does not
/// grow without bound waiting for someone to call flush().
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_memtables_are_flushed_in_the_background() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = LsmConfig {
        // A 1 MiB budget gives a 512 KiB memtable.
        total_cache_bytes: 1 << 20,
        ..config(dir.path())
    };
    let engine = LsmEngine::open(io(), cfg).await.unwrap();
    // 8 MiB of writes and no explicit flush.
    for i in 0..8000 {
        engine
            .put(format!("key{i:06}").as_bytes(), &[5u8; 1000])
            .await
            .unwrap();
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while sst_files(dir.path()).is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "nothing was flushed in the background"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    for i in (0..8000).step_by(97) {
        assert_eq!(
            engine.get(format!("key{i:06}").as_bytes()).await.unwrap(),
            Some(vec![5u8; 1000])
        );
    }
    engine.close().await.unwrap();
}

/// Writers that outrun the flusher wait for it instead of piling memtables
/// up in memory, and every write still lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn writers_wait_for_the_flusher_and_lose_nothing() {
    const WRITERS: usize = 8;
    const PER_WRITER: usize = 3000;

    let dir = tempfile::tempdir().unwrap();
    let cfg = LsmConfig {
        // A 256 KiB memtable against ~48 MiB of writes: hundreds of
        // rotations, far more than the frozen-memtable limit.
        total_cache_bytes: 512 * 1024,
        ..config(dir.path())
    };
    let engine = Arc::new(LsmEngine::open(io(), cfg.clone()).await.unwrap());
    let writers: Vec<_> = (0..WRITERS)
        .map(|w| {
            let engine = engine.clone();
            tokio::spawn(async move {
                for i in 0..PER_WRITER {
                    let key = format!("w{w}-{i:06}");
                    engine.put(key.as_bytes(), &[w as u8; 2000]).await.unwrap();
                }
            })
        })
        .collect();
    tokio::time::timeout(std::time::Duration::from_secs(120), async {
        for writer in writers {
            writer.await.unwrap();
        }
    })
    .await
    .expect("writers stalled forever");

    for w in 0..WRITERS {
        for i in (0..PER_WRITER).step_by(53) {
            let key = format!("w{w}-{i:06}");
            assert_eq!(
                engine.get(key.as_bytes()).await.unwrap(),
                Some(vec![w as u8; 2000]),
                "{key}"
            );
        }
    }
    engine.close().await.unwrap();
    drop(engine);

    // Everything is on disk after close.
    let engine = LsmEngine::open(io(), cfg).await.unwrap();
    for w in 0..WRITERS {
        let last = format!("w{w}-{:06}", PER_WRITER - 1);
        assert_eq!(
            engine.get(last.as_bytes()).await.unwrap(),
            Some(vec![w as u8; 2000])
        );
    }
    engine.close().await.unwrap();
}

/// A directory with tables but no manifest has lost its manifest. Treating it
/// as new would delete every table as a leftover.
#[tokio::test]
async fn a_lost_manifest_is_not_mistaken_for_a_new_directory() {
    let dir = tempfile::tempdir().unwrap();
    let engine = LsmEngine::open(io(), config(dir.path())).await.unwrap();
    engine.put(b"k", b"v").await.unwrap();
    engine.flush().await.unwrap();
    engine.close().await.unwrap();
    drop(engine);

    let tables = sst_files(dir.path());
    assert_eq!(tables.len(), 1);
    std::fs::remove_file(dir.path().join("MANIFEST")).unwrap();

    assert!(
        LsmEngine::open(io(), config(dir.path())).await.is_err(),
        "open treated a directory with tables as empty"
    );
    assert_eq!(sst_files(dir.path()), tables, "the tables were deleted");
}

/// A damaged bloom filter would say "not here" for keys that are. It carries a
/// checksum so the damage is reported instead.
#[tokio::test]
async fn a_damaged_bloom_filter_is_detected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = LsmEngine::open(io(), config(dir.path())).await.unwrap();
    for i in 0..5000 {
        engine
            .put(format!("key{i:05}").as_bytes(), &[7u8; 32])
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    engine.close().await.unwrap();
    drop(engine);

    let path = dir.path().join(&sst_files(dir.path())[0]);
    let mut bytes = std::fs::read(&path).unwrap();
    // The bloom filter is the region just before the one-page footer; 5000
    // keys give it more than one page, so this lands inside its bits.
    let at = bytes.len() - 4096 - 3000;
    bytes[at] ^= 0xFF;
    std::fs::write(&path, &bytes).unwrap();

    assert!(
        LsmEngine::open(io(), config(dir.path())).await.is_err(),
        "a damaged bloom filter went unnoticed"
    );
}

/// Two engines on one directory would overwrite each other's manifest. The
/// second open is refused; after a close the directory can be opened again.
#[tokio::test]
async fn a_directory_can_only_be_open_once() {
    let dir = tempfile::tempdir().unwrap();
    let first = LsmEngine::open(io(), config(dir.path())).await.unwrap();
    first.put(b"k", b"v").await.unwrap();

    let second = LsmEngine::open(io(), config(dir.path())).await;
    assert!(second.is_err(), "the same directory was opened twice");

    first.close().await.unwrap();
    drop(first);
    let reopened = LsmEngine::open(io(), config(dir.path())).await.unwrap();
    assert_eq!(reopened.get(b"k").await.unwrap(), Some(b"v".to_vec()));
    reopened.close().await.unwrap();
}

/// Dropping an engine without close() lets go of the directory.
#[tokio::test]
async fn a_dropped_engine_releases_the_directory() {
    let dir = tempfile::tempdir().unwrap();
    for round in 0..5u8 {
        let engine = LsmEngine::open(io(), config(dir.path())).await.unwrap();
        engine.put(&[round], b"v").await.unwrap();
        engine.flush().await.unwrap();
        // No close().
    }
    let engine = LsmEngine::open(io(), config(dir.path())).await.unwrap();
    for round in 0..5u8 {
        assert_eq!(engine.get(&[round]).await.unwrap(), Some(b"v".to_vec()));
    }
    engine.close().await.unwrap();
}

/// The nearest key on either side of a key is found across memtables and
/// table files, skipping deleted keys, whether or not the key itself
/// exists.
#[tokio::test]
async fn nearest_keys_are_found_across_memory_and_files() {
    let dir = tempfile::tempdir().unwrap();
    let engine = LsmEngine::open(io(), config(dir.path())).await.unwrap();
    let key = |n: u32| format!("k{n:05}").into_bytes();

    // Even keys in a file; enough of them, with large values, for the file
    // to have many blocks.
    for n in (0..2000).step_by(2) {
        engine.put(&key(n), &value_of(300, n as u8)).await.unwrap();
    }
    engine.flush().await.unwrap();
    // A second file deletes a run of them; memory adds an odd key and
    // deletes one more.
    for n in (1000..1100).step_by(2) {
        engine.delete(&key(n)).await.unwrap();
    }
    engine.flush().await.unwrap();
    engine.put(&key(501), b"odd").await.unwrap();
    engine.delete(&key(1500)).await.unwrap();

    check_nearest(&engine).await;
    // The same answers once everything is in files.
    engine.flush().await.unwrap();
    check_nearest(&engine).await;
    engine.close().await.unwrap();
}

async fn check_nearest(engine: &LsmEngine) {
    let key = |n: u32| format!("k{n:05}").into_bytes();
    let low = key(0);
    let high = b"k99999".to_vec();
    // (from, expected next, expected previous)
    let cases: [(u32, Option<u32>, Option<u32>); 12] = [
        // Between two keys of the file, from a key that exists and one
        // that does not.
        (10, Some(12), Some(8)),
        (11, Some(12), Some(10)),
        // The key in memory sits between two in the file.
        (500, Some(501), Some(498)),
        (501, Some(502), Some(500)),
        (502, Some(504), Some(501)),
        // A deleted run is passed over in both directions.
        (998, Some(1100), Some(996)),
        (1100, Some(1102), Some(998)),
        (1050, Some(1100), Some(998)),
        // So is a key deleted in memory.
        (1498, Some(1502), Some(1496)),
        (1502, Some(1504), Some(1498)),
        // The ends.
        (0, Some(2), None),
        (1998, None, Some(1996)),
    ];
    for (from, next, prev) in cases {
        assert_eq!(
            engine.next_key(&key(from), &high).await.unwrap(),
            next.map(key),
            "next after {from}"
        );
        assert_eq!(
            engine.prev_key(&key(from), &low).await.unwrap(),
            prev.map(key),
            "previous before {from}"
        );
    }
    assert_eq!(engine.next_key(&key(5000), &high).await.unwrap(), None);
    assert_eq!(
        engine.prev_key(&key(5000), &low).await.unwrap(),
        Some(key(1998))
    );
    // A search stops at its bound: nothing beyond a deleted run is
    // reported when the bound lies inside the run.
    assert_eq!(engine.next_key(&key(998), &key(1050)).await.unwrap(), None);
    assert_eq!(engine.prev_key(&key(1100), &key(1050)).await.unwrap(), None);
    // The bounds: `high` is excluded, `low` included.
    assert_eq!(engine.next_key(&key(10), &key(12)).await.unwrap(), None);
    assert_eq!(
        engine.next_key(&key(10), &key(13)).await.unwrap(),
        Some(key(12))
    );
    assert_eq!(engine.prev_key(&key(12), &key(11)).await.unwrap(), None);
    assert_eq!(
        engine.prev_key(&key(12), &key(10)).await.unwrap(),
        Some(key(10))
    );
}

/// A view shows the flushed contents as they were when it was taken,
/// whatever is written, flushed and compacted afterwards, and hands them
/// out in pieces, in order, without the deleted keys.
#[tokio::test]
async fn a_view_holds_a_moment_still() {
    let dir = tempfile::tempdir().unwrap();
    let engine = LsmEngine::open(io(), config(dir.path())).await.unwrap();
    let key = |n: u32| format!("k{n:05}").into_bytes();
    // Three overlapping files: every key, then every third rewritten, then
    // every fifth deleted.
    for n in 0..3000 {
        engine.put(&key(n), &value_of(200, 1)).await.unwrap();
    }
    engine.flush().await.unwrap();
    for n in (0..3000).step_by(3) {
        engine.put(&key(n), &value_of(200, 2)).await.unwrap();
    }
    engine.flush().await.unwrap();
    for n in (0..3000).step_by(5) {
        engine.delete(&key(n)).await.unwrap();
    }
    engine.flush().await.unwrap();
    // Not flushed, so not in the view.
    engine.put(b"k99999", b"in memory").await.unwrap();

    let view = engine.snapshot_view().await.unwrap();
    let expected: Vec<(Vec<u8>, Vec<u8>)> = (0..3000)
        .filter(|n| n % 5 != 0)
        .map(|n| (key(n), value_of(200, if n % 3 == 0 { 2 } else { 1 })))
        .collect();

    // The engine moves on: everything is rewritten, flushed and compacted.
    for n in 0..3000 {
        engine.put(&key(n), b"later").await.unwrap();
    }
    engine.flush().await.unwrap();

    // Read the view in small pieces.
    let mut seen: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut after: Option<Vec<u8>> = None;
    loop {
        let (chunk, done) = view.next_chunk(after.as_deref(), 16 * 1024).await.unwrap();
        for pair in &chunk {
            assert!(
                seen.last()
                    .is_none_or(|last: &(Vec<u8>, Vec<u8>)| last.0 < pair.0),
                "out of order"
            );
        }
        if let Some(last) = chunk.last() {
            after = Some(last.0.clone());
        }
        seen.extend(chunk);
        if done {
            break;
        }
    }
    assert_eq!(seen.len(), expected.len());
    assert!(
        seen == expected,
        "the view changed, or lost or gained entries"
    );
    assert_eq!(view.get(&key(3)).await.unwrap(), Some(value_of(200, 2)));
    assert_eq!(view.get(&key(5)).await.unwrap(), None);
    assert_eq!(view.get(b"k99999").await.unwrap(), None);
    engine.close().await.unwrap();
}

/// A replacement of the whole contents takes effect at once or not at all:
/// abandoned, it leaves everything as it was and no files behind; committed,
/// the old contents are gone, memory included, and a restart agrees.
#[tokio::test]
async fn a_replacement_is_all_or_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let key = |n: u32| format!("k{n:05}").into_bytes();
    {
        let engine = Arc::new(LsmEngine::open(io(), config(dir.path())).await.unwrap());
        for n in 0..500 {
            engine.put(&key(n), b"old").await.unwrap();
        }
        engine.flush().await.unwrap();
        engine.put(b"in-memory", b"old").await.unwrap();
        let before = sst_files(dir.path());

        // Abandoned half way.
        {
            let mut ingest = engine.clone().ingest().await.unwrap();
            ingest
                .add((1000..1200).map(|n| (key(n), b"new".to_vec())).collect())
                .await
                .unwrap();
        }
        assert_eq!(
            sst_files(dir.path()),
            before,
            "an abandoned replacement left files"
        );
        assert_eq!(engine.get(&key(1)).await.unwrap(), Some(b"old".to_vec()));
        assert_eq!(engine.get(&key(1000)).await.unwrap(), None);

        // Committed.
        let mut ingest = engine.clone().ingest().await.unwrap();
        for start in (1000..2000).step_by(100) {
            ingest
                .add(
                    (start..start + 100)
                        .map(|n| (key(n), b"new".to_vec()))
                        .collect(),
                )
                .await
                .unwrap();
        }
        ingest.commit().await.unwrap();
        assert_eq!(engine.get(&key(1)).await.unwrap(), None);
        assert_eq!(engine.get(b"in-memory").await.unwrap(), None);
        assert_eq!(engine.get(&key(1500)).await.unwrap(), Some(b"new".to_vec()));
        assert_eq!(engine.scan(None, None).await.unwrap().len(), 1000);
        // It goes on working as an ordinary engine.
        engine.put(&key(1500), b"newer").await.unwrap();
        // Crash: no close.
    }
    let engine = LsmEngine::open(io(), config(dir.path())).await.unwrap();
    let all = engine.scan(None, None).await.unwrap();
    assert_eq!(all.len(), 1000);
    assert!(all.iter().all(|(_, value)| value == b"new"));
    assert_eq!(all[0].0, key(1000));
    engine.close().await.unwrap();
}

/// A view over tables that do not overlap is read one table after another,
/// and gives the same entries as a scan, also where whole tables' worth of
/// keys have been deleted.
#[tokio::test]
async fn a_view_over_tables_in_key_order_matches_a_scan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = LsmEngine::open(io(), config(dir.path())).await.unwrap();
    let key = |n: u32| format!("k{n:05}").into_bytes();
    // Five ranges, each flushed to a table of its own.
    for range in 0..5u32 {
        for n in range * 1000..range * 1000 + 400 {
            engine
                .put(&key(n), &value_of(300, range as u8))
                .await
                .unwrap();
        }
        engine.flush().await.unwrap();
    }
    // A later table deletes all of the first range, the start of the third
    // and the end of the last.
    for n in (0..400).chain(2000..2100).chain(4300..4400) {
        engine.delete(&key(n)).await.unwrap();
    }
    engine.flush().await.unwrap();

    let expected = engine.scan(None, None).await.unwrap();
    assert_eq!(expected.len(), 5 * 400 - 600);
    let view = engine.snapshot_view().await.unwrap();
    for chunk_bytes in [1, 8 * 1024, 1 << 20] {
        let mut seen: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut after: Option<Vec<u8>> = None;
        loop {
            let (chunk, done) = view
                .next_chunk(after.as_deref(), chunk_bytes)
                .await
                .unwrap();
            if let Some(last) = chunk.last() {
                after = Some(last.0.clone());
            }
            seen.extend(chunk);
            if done {
                break;
            }
        }
        assert!(
            seen == expected,
            "chunks of {chunk_bytes} bytes differ from a scan"
        );
    }
    engine.close().await.unwrap();
}

/// Compaction works through its inputs a stretch at a time. Tables larger
/// than a stretch, overlapping, with overwrites and deletes between them,
/// come out as the same data.
#[tokio::test]
async fn compaction_of_tables_larger_than_a_stretch_keeps_the_data() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = LsmConfig {
        l0_compaction_trigger: 2,
        level_base_bytes: 64 * 1024,
        ..config(dir.path())
    };
    let engine = LsmEngine::open(io(), cfg).await.unwrap();
    let mut model: std::collections::BTreeMap<Vec<u8>, Vec<u8>> = Default::default();
    for round in 0..10u32 {
        // Every round rewrites most keys, so the tables overlap throughout.
        for n in 0..3000u32 {
            let key = format!("k{n:05}").into_bytes();
            if (n + round) % 7 == 0 {
                engine.delete(&key).await.unwrap();
                model.remove(&key);
            } else if (n + round) % 3 != 0 {
                let value = value_of(900 + (n % 200) as usize, (round + n) as u8);
                engine.put(&key, &value).await.unwrap();
                model.insert(key, value);
            }
        }
        engine.flush().await.unwrap();
    }
    let expected: Vec<(Vec<u8>, Vec<u8>)> = model.into_iter().collect();
    assert!(engine.scan(None, None).await.unwrap() == expected);
    engine.close().await.unwrap();

    // And after a restart, when everything is read from the tables.
    let cfg = LsmConfig {
        l0_compaction_trigger: 2,
        level_base_bytes: 64 * 1024,
        ..config(dir.path())
    };
    let engine = LsmEngine::open(io(), cfg).await.unwrap();
    assert!(engine.scan(None, None).await.unwrap() == expected);
    engine.close().await.unwrap();
}

/// Initialisation stores its rows as the server stores rows: each under
/// the key of what it describes, with a version header. And it records
/// where the row ID counter stands, for the server to start above.
#[tokio::test]
async fn initialisation_writes_rows_the_server_reads() {
    use roodb::catalog::store::scan_rows;
    use roodb::catalog::system_tables::{SYSTEM_DATABASES, SYSTEM_GRANTS, SYSTEM_USERS};
    use roodb::executor::Datum;

    let dir = tempfile::tempdir().unwrap();
    let engine = LsmEngine::open(io(), config(dir.path())).await.unwrap();
    let storage: Arc<dyn StorageEngine> = Arc::new(engine);
    roodb::init::initialize_database(&storage, "secret")
        .await
        .unwrap();

    let first = |table: &'static str| {
        let storage = storage.clone();
        async move {
            let mut names: Vec<String> = scan_rows(storage.as_ref(), table)
                .await
                .unwrap()
                .into_iter()
                .filter_map(|stored| match stored.row.values().first() {
                    Some(Datum::String(name)) => Some(name.clone()),
                    _ => None,
                })
                .collect();
            names.sort();
            names
        }
    };
    assert_eq!(first(SYSTEM_USERS).await, ["root"]);
    assert_eq!(first(SYSTEM_GRANTS).await, ["root"]);
    // No databases but the system's own, which are not stored.
    assert!(first(SYSTEM_DATABASES).await.is_empty());

    let bound = storage
        .get(roodb::raft::ROW_ID_KEY)
        .await
        .unwrap()
        .map(|stored| u64::from_le_bytes(stored.as_slice().try_into().unwrap()))
        .expect("no row ID bound was recorded");
    assert!(bound >= 1);
    storage.close().await.unwrap();
}

// Forgotten ranges. The tests keep three tables' worth of keys side by side,
// `a:`, `b:` and `c:`, and forget `b:`, which is `[b:, b;)`.

const B_START: &[u8] = b"b:";
const B_END: &[u8] = b"b;";

fn tkey(table: char, n: u32) -> Vec<u8> {
    format!("{table}:{n:05}").into_bytes()
}

fn tvalue(table: char, n: u32) -> Vec<u8> {
    value_of(120, (table as u32 + n) as u8)
}

/// Write keys `range` of each of `tables`.
async fn put_keys(engine: &LsmEngine, tables: &[char], range: std::ops::Range<u32>) {
    for n in range {
        for &table in tables {
            engine
                .put(&tkey(table, n), &tvalue(table, n))
                .await
                .unwrap();
        }
    }
}

/// Every key of `a:` and `c:` in `range` is there, and none of `b:`.
async fn check_only_b_is_gone(engine: &LsmEngine, range: std::ops::Range<u32>, when: &str) {
    for n in range.clone() {
        assert_eq!(engine.get(&tkey('b', n)).await.unwrap(), None, "{when}");
        for table in ['a', 'c'] {
            assert_eq!(
                engine.get(&tkey(table, n)).await.unwrap(),
                Some(tvalue(table, n)),
                "{when}: {table}:{n}"
            );
        }
    }
    let in_range = engine.scan(Some(B_START), Some(B_END)).await.unwrap();
    assert!(in_range.is_empty(), "{when}: a scan found forgotten keys");
    let all = engine.scan(None, None).await.unwrap();
    assert!(
        all.iter()
            .all(|(key, _)| key.as_slice() < B_START || key.as_slice() >= B_END),
        "{when}: a scan of everything found forgotten keys"
    );
    for table in ['a', 'c'] {
        let count = all.iter().filter(|(key, _)| key[0] == table as u8).count();
        assert_eq!(count, range.len(), "{when}: keys of {table}:");
    }
}

/// What the manifest on disk says.
fn manifest_on_disk(dir: &Path) -> roodb::storage::lsm::manifest::ManifestData {
    let text = std::fs::read_to_string(dir.join("MANIFEST")).unwrap();
    serde_json::from_str(&text).unwrap()
}

/// Names of the tables the manifest on disk lists.
fn listed_tables(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = manifest_on_disk(dir)
        .levels
        .iter()
        .flat_map(|level| level.files.iter().map(|file| file.name.clone()))
        .collect();
    names.sort();
    names
}

/// Wait for the engine's background work to bring something about.
async fn wait_for(what: &str, reached: impl Fn() -> bool) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    while !reached() {
        assert!(
            std::time::Instant::now() < deadline,
            "the engine's background work never got as far as: {what}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

/// Everything a view holds, read in small pieces.
async fn read_view(view: &dyn roodb::storage::traits::SnapshotView) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut seen = Vec::new();
    let mut after: Option<Vec<u8>> = None;
    loop {
        let (chunk, done) = view.next_chunk(after.as_deref(), 8 * 1024).await.unwrap();
        if let Some(last) = chunk.last() {
            after = Some(last.0.clone());
        }
        seen.extend(chunk);
        if done {
            return seen;
        }
    }
}

/// Forget `[start, end)`, in a batch of its own.
async fn forget(engine: &LsmEngine, start: &[u8], end: &[u8]) {
    engine
        .write_batch(vec![WriteOp::forget(start, end)])
        .await
        .unwrap();
}

/// A forgotten range is gone from every read at once - keys in memory and
/// keys in tables alike - while its neighbours on both sides stay. It stays
/// gone over a restart, forgetting it again changes nothing, and the keys
/// next to it can be written and read as before.
#[tokio::test]
async fn a_forgotten_range_is_gone_and_stays_gone() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = LsmConfig {
        l0_compaction_trigger: 2,
        level_base_bytes: 16 * 1024,
        ..config(dir.path())
    };
    let engine = LsmEngine::open(io(), cfg.clone()).await.unwrap();
    // The last key before the range, and the key that ends it.
    engine.put(b"b9", b"before").await.unwrap();
    engine.put(B_END, b"end").await.unwrap();
    for round in 0..6 {
        put_keys(&engine, &['a', 'b', 'c'], round * 100..(round + 1) * 100).await;
        engine.flush().await.unwrap();
    }
    // Still in memory.
    put_keys(&engine, &['a', 'b', 'c'], 600..700).await;
    engine.put(B_START, b"first of the range").await.unwrap();

    forget(&engine, B_START, B_END).await;

    check_only_b_is_gone(&engine, 0..700, "after the drop").await;
    assert_eq!(engine.get(B_START).await.unwrap(), None);
    assert_eq!(engine.get(b"b9").await.unwrap(), Some(b"before".to_vec()));
    assert_eq!(engine.get(B_END).await.unwrap(), Some(b"end".to_vec()));
    // The nearest keys are found across the gap, from either side and from
    // inside it.
    assert_eq!(
        engine.next_key(b"b9", b"z").await.unwrap(),
        Some(B_END.to_vec())
    );
    assert_eq!(
        engine.next_key(&tkey('b', 300), b"z").await.unwrap(),
        Some(B_END.to_vec())
    );
    assert_eq!(
        engine.prev_key(B_END, b"a").await.unwrap(),
        Some(b"b9".to_vec())
    );
    assert_eq!(
        engine.prev_key(&tkey('b', 300), b"a").await.unwrap(),
        Some(b"b9".to_vec())
    );
    assert_eq!(engine.next_key(b"b9", B_END).await.unwrap(), None);
    assert_eq!(engine.prev_key(B_END, B_START).await.unwrap(), None);
    // All of that from memory: nothing of the range is on disk yet, where
    // the keys flushed earlier still are.
    assert!(manifest_on_disk(dir.path()).forgotten.is_empty());
    assert_eq!(manifest_on_disk(dir.path()).forgotten_seq, 0);
    assert_eq!(
        engine.get_durable(&tkey('b', 300)).await.unwrap(),
        Some(tvalue('b', 300))
    );

    // Again, with the same range and with ones that overlap it: harmless.
    forget(&engine, B_START, B_END).await;
    forget(&engine, b"b:00100", b"b:00200").await;
    forget(&engine, b"b:00650", B_END).await;
    // An empty range and an inverted one hold nothing.
    forget(&engine, b"a:", b"a:").await;
    forget(&engine, b"c;", b"a:").await;
    check_only_b_is_gone(&engine, 0..700, "after repeating the drop").await;

    // The ranges on both sides take writes as before.
    put_keys(&engine, &['a', 'c'], 700..800).await;
    engine.put(b"b9z", b"new neighbour").await.unwrap();
    engine.put(b"b;0", b"new neighbour").await.unwrap();
    check_only_b_is_gone(&engine, 0..800, "after writing next to it").await;
    engine.flush().await.unwrap();
    check_only_b_is_gone(&engine, 0..800, "after a flush").await;
    assert_eq!(engine.get_durable(&tkey('b', 300)).await.unwrap(), None);
    assert!(manifest_on_disk(dir.path()).forgotten_seq > 0);
    engine.close().await.unwrap();
    drop(engine);

    let engine = LsmEngine::open(io(), cfg.clone()).await.unwrap();
    check_only_b_is_gone(&engine, 0..800, "after a restart").await;
    assert_eq!(engine.get(b"b9").await.unwrap(), Some(b"before".to_vec()));
    assert_eq!(
        engine.get(b"b9z").await.unwrap(),
        Some(b"new neighbour".to_vec())
    );
    assert_eq!(engine.get(B_END).await.unwrap(), Some(b"end".to_vec()));
    assert_eq!(
        engine.get(b"b;0").await.unwrap(),
        Some(b"new neighbour".to_vec())
    );
    // Forgotten once more after the restart.
    forget(&engine, B_START, B_END).await;
    check_only_b_is_gone(&engine, 0..800, "after another drop").await;
    engine.close().await.unwrap();
    drop(engine);

    let engine = LsmEngine::open(io(), cfg).await.unwrap();
    check_only_b_is_gone(&engine, 0..800, "after a second restart").await;
    engine.close().await.unwrap();
}

/// The key under which these tests keep the position of the log entry that
/// was applied last, as the Raft state machine does: every batch writes it.
const POSITION: &[u8] = b"position";

/// Stop at each point where a crash while a forgotten range is on its way
/// to disk leaves the directory in a distinct state - the table of its
/// batch written but nothing listed; the range and the table listed and
/// nothing else done; tables unlisted but their files still there;
/// everything done - and reopen. Storage is either wholly before the batch
/// that forgot the range (the range's keys there, the earlier position) or
/// wholly after it (the keys gone, the batch's position). Then the range
/// goes or stays gone, its neighbours do not, the files end up deleted and
/// the engine goes on working.
#[tokio::test]
async fn a_crash_while_forgetting_a_range_loses_only_the_range() {
    for point in [
        Some(CrashPoint::FlushAfterTableWritten),
        Some(CrashPoint::FlushAfterRangesSaved),
        Some(CrashPoint::ForgottenTablesUnlisted),
        None,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let base = config(dir.path());

        // Two tables of nothing but `b:` keys and one with all three kinds:
        // three tables in level 0, one short of the compaction trigger.
        let engine = LsmEngine::open(io(), base.clone()).await.unwrap();
        put_keys(&engine, &['b'], 0..200).await;
        engine.flush().await.unwrap();
        put_keys(&engine, &['b'], 200..400).await;
        engine.flush().await.unwrap();
        let only_b = sst_files(dir.path());
        assert_eq!(only_b.len(), 2);
        put_keys(&engine, &['a', 'b', 'c'], 400..600).await;
        engine.put(POSITION, &[1]).await.unwrap();
        engine.flush().await.unwrap();
        engine.close().await.unwrap();
        drop(engine);
        assert_eq!(sst_files(dir.path()).len(), 3);

        let armed = LsmConfig {
            crash_point: point,
            ..base.clone()
        };
        let engine = LsmEngine::open(io(), armed).await.unwrap();
        let batch = || {
            vec![
                WriteOp::forget(B_START, B_END),
                WriteOp::Put(POSITION.to_vec(), vec![2]),
            ]
        };
        engine.write_batch(batch()).await.unwrap();
        // In memory the range is gone at once, whatever happens to the
        // flush.
        check_only_b_is_gone(&engine, 400..600, &format!("{point:?}, in memory")).await;
        let flushed = engine.flush().await;
        let range_saved = match point {
            Some(CrashPoint::FlushAfterTableWritten) => {
                assert!(flushed.is_err(), "the crash point did not stop the flush");
                // The batch's table is written and nothing lists it, nor
                // the range.
                assert!(manifest_on_disk(dir.path()).forgotten.is_empty());
                assert_eq!(listed_tables(dir.path()).len(), 3);
                assert_eq!(sst_files(dir.path()).len(), 4);
                false
            }
            Some(CrashPoint::FlushAfterRangesSaved) => {
                assert!(flushed.is_err(), "the crash point did not stop the flush");
                // One change of the manifest listed the range and the table
                // of its batch; no table has been touched.
                assert_eq!(manifest_on_disk(dir.path()).forgotten.len(), 1);
                assert_eq!(listed_tables(dir.path()).len(), 4);
                true
            }
            Some(_) => {
                assert!(flushed.is_err(), "the crash point did not stop the flush");
                wait_for("the tables of the range are off the manifest", || {
                    let listed = listed_tables(dir.path());
                    only_b.iter().all(|name| !listed.contains(name))
                })
                .await;
                // The crash came before their files were deleted.
                let on_disk = sst_files(dir.path());
                assert!(
                    only_b.iter().all(|name| on_disk.contains(name)),
                    "{point:?}"
                );
                true
            }
            None => {
                flushed.unwrap();
                wait_for("the range is retired", || {
                    manifest_on_disk(dir.path()).forgotten.is_empty()
                })
                .await;
                true
            }
        };
        // Crash: no close.
        drop(engine);

        let engine = LsmEngine::open(io(), base.clone())
            .await
            .unwrap_or_else(|e| panic!("{point:?}: reopen failed: {e}"));
        let when = format!("{point:?}, reopened");
        if range_saved {
            assert_eq!(engine.get(POSITION).await.unwrap(), Some(vec![2]), "{when}");
        } else {
            // Storage is where it was before the batch: its position, and
            // every key of the range. The log is replayed from there.
            assert_eq!(engine.get(POSITION).await.unwrap(), Some(vec![1]), "{when}");
            for n in (0..600).step_by(13) {
                assert_eq!(
                    engine.get(&tkey('b', n)).await.unwrap(),
                    Some(tvalue('b', n)),
                    "{when}"
                );
            }
            assert_eq!(sst_files(dir.path()), listed_tables(dir.path()), "{when}");
            engine.write_batch(batch()).await.unwrap();
            engine.flush().await.unwrap();
            assert_eq!(engine.get(POSITION).await.unwrap(), Some(vec![2]), "{when}");
        }
        for n in (0..400).step_by(13) {
            assert_eq!(engine.get(&tkey('b', n)).await.unwrap(), None, "{when}");
        }
        check_only_b_is_gone(&engine, 400..600, &when).await;

        // What was left undone is finished after the restart.
        wait_for("the range is retired after the restart", || {
            manifest_on_disk(dir.path()).forgotten.is_empty()
        })
        .await;
        let on_disk = sst_files(dir.path());
        assert!(
            only_b.iter().all(|name| !on_disk.contains(name)),
            "{point:?}: {on_disk:?}"
        );
        assert_eq!(on_disk, listed_tables(dir.path()), "{point:?}");
        // The range is no longer listed, so nothing hides its keys now:
        // they are gone from the tables themselves.
        check_only_b_is_gone(&engine, 400..600, &format!("{point:?}, retired")).await;

        // And it still takes writes and flushes, next to the range too.
        put_keys(&engine, &['a', 'c'], 600..700).await;
        engine.flush().await.unwrap();
        check_only_b_is_gone(&engine, 400..700, &format!("{point:?}, written")).await;
        engine.close().await.unwrap();
        drop(engine);

        let engine = LsmEngine::open(io(), base).await.unwrap();
        check_only_b_is_gone(&engine, 400..700, &format!("{point:?}, closed")).await;
        assert_eq!(engine.get(POSITION).await.unwrap(), Some(vec![2]));
        engine.close().await.unwrap();
    }
}

/// The space forgotten ranges took is given back: tables that held nothing
/// else are deleted, tables that held other keys too are rewritten without
/// them, in every level, and once nothing holds a range's keys it comes off
/// the manifest.
#[tokio::test]
async fn the_space_of_forgotten_ranges_is_given_back() {
    // Each group of keys has its own `a:`, `b:` and `c:`, so that tables of
    // different groups do not overlap and compaction leaves them spread
    // over several levels. Forgetting every group's `b:` is one range each.
    fn gkey(group: u32, table: char, n: u32) -> Vec<u8> {
        format!("{group:02}{table}:{n:04}").into_bytes()
    }
    async fn put_group(engine: &LsmEngine, group: u32, tables: &[char]) {
        for n in 0..100 {
            for &table in tables {
                engine
                    .put(&gkey(group, table, n), &tvalue(table, n))
                    .await
                    .unwrap();
            }
        }
    }
    fn table_bytes(dir: &Path) -> u64 {
        manifest_on_disk(dir)
            .levels
            .iter()
            .flat_map(|level| &level.files)
            .map(|file| file.size)
            .sum()
    }
    const MIXED: [u32; 10] = [0, 1, 2, 3, 4, 5, 6, 7, 30, 31];
    const ONLY_B: [u32; 2] = [20, 21];

    let dir = tempfile::tempdir().unwrap();
    let cfg = LsmConfig {
        l0_compaction_trigger: 4,
        // Level 2 has room for one of the compacted tables, not for two.
        level_base_bytes: 32 * 1024,
        ..config(dir.path())
    };
    let engine = LsmEngine::open(io(), cfg.clone()).await.unwrap();
    // Eight tables holding all three kinds of key, compacted down.
    for group in 0..8 {
        put_group(&engine, group, &['a', 'b', 'c']).await;
        engine.flush().await.unwrap();
    }
    let compacted = manifest_on_disk(dir.path());
    assert!(compacted.levels[0].files.is_empty());
    let deep_levels = compacted
        .levels
        .iter()
        .filter(|level| !level.files.is_empty())
        .count();
    assert!(deep_levels >= 2, "the tables are all in one level");
    // Then two tables of nothing but `b:` keys and one more with all three,
    // which stay in level 0; and some of it in memory.
    let before = sst_files(dir.path());
    for group in ONLY_B {
        put_group(&engine, group, &['b']).await;
        engine.flush().await.unwrap();
    }
    let only_b: Vec<String> = sst_files(dir.path())
        .into_iter()
        .filter(|name| !before.contains(name))
        .collect();
    assert_eq!(only_b.len(), 2);
    put_group(&engine, 30, &['a', 'b', 'c']).await;
    engine.flush().await.unwrap();
    assert_eq!(manifest_on_disk(dir.path()).levels[0].files.len(), 3);
    put_group(&engine, 31, &['a', 'b', 'c']).await;
    let bytes_before = table_bytes(dir.path());

    for group in MIXED.into_iter().chain(ONLY_B) {
        let (start, end) = (format!("{group:02}b:"), format!("{group:02}b;"));
        forget(&engine, start.as_bytes(), end.as_bytes()).await;
    }

    let expected: Vec<(Vec<u8>, Vec<u8>)> = MIXED
        .into_iter()
        .flat_map(|group| {
            ['a', 'c'].into_iter().flat_map(move |table| {
                (0..100).map(move |n| (gkey(group, table, n), tvalue(table, n)))
            })
        })
        .collect();
    assert!(
        engine.scan(None, None).await.unwrap() == expected,
        "right after the drop"
    );
    // So far the ranges are with the memtable. Its flush records them.
    assert_eq!(manifest_on_disk(dir.path()).forgotten_seq, 0);
    assert_eq!(table_bytes(dir.path()), bytes_before);
    engine.flush().await.unwrap();
    assert_eq!(
        manifest_on_disk(dir.path()).forgotten_seq,
        12,
        "the ranges were not all recorded"
    );

    wait_for("every range is retired", || {
        manifest_on_disk(dir.path()).forgotten.is_empty()
    })
    .await;
    let on_disk = sst_files(dir.path());
    assert!(
        only_b.iter().all(|name| !on_disk.contains(name)),
        "tables of nothing but forgotten keys are still there: {on_disk:?}"
    );
    assert_eq!(on_disk, listed_tables(dir.path()));
    // Less on disk than before, though a third of a memtable was added.
    let bytes_after = table_bytes(dir.path());
    assert!(
        bytes_after < bytes_before,
        "{bytes_before} bytes of tables before, {bytes_after} after"
    );
    // With the ranges off the list nothing hides their keys: that none is
    // found means none is left, in memory or in any table.
    assert!(
        engine.scan(None, None).await.unwrap() == expected,
        "once retired"
    );
    for group in MIXED.into_iter().chain(ONLY_B) {
        assert_eq!(engine.get(&gkey(group, 'b', 50)).await.unwrap(), None);
    }
    engine.close().await.unwrap();
    drop(engine);

    let engine = LsmEngine::open(io(), cfg).await.unwrap();
    assert!(manifest_on_disk(dir.path()).forgotten.is_empty());
    assert!(
        engine.scan(None, None).await.unwrap() == expected,
        "after a restart"
    );
    engine.close().await.unwrap();
}

/// Ranges are forgotten while memtables fill, are flushed and are compacted
/// all around them. However a drop falls among those - in a memtable that is
/// about to be frozen, while an older memtable's table is being written,
/// while a compaction that took the keys in is running - no forgotten key
/// comes back once its range has been retired, and no other key is lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ranges_forgotten_among_flushes_and_compactions_stay_forgotten() {
    const GROUPS: u32 = 120;
    fn gkey(group: u32, table: char, n: u32) -> Vec<u8> {
        format!("{group:03}{table}:{n:04}").into_bytes()
    }

    let dir = tempfile::tempdir().unwrap();
    let cfg = LsmConfig {
        // Small memtables and small levels: flushes and compactions the
        // whole time, in the background and from the flusher below.
        total_cache_bytes: 256 * 1024,
        l0_compaction_trigger: 2,
        level_base_bytes: 64 * 1024,
        ..config(dir.path())
    };
    let engine = Arc::new(LsmEngine::open(io(), cfg.clone()).await.unwrap());
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

    for group in 0..GROUPS {
        for n in 0..60 {
            let batch = ['a', 'b', 'c']
                .into_iter()
                .map(|table| WriteOp::Put(gkey(group, table, n), tvalue(table, n)))
                .collect();
            engine.write_batch(batch).await.unwrap();
            if n % 16 == 0 {
                tokio::task::yield_now().await;
            }
        }
        // The group before last loses its `b:`; the last one is still
        // being written around it.
        if let Some(victim) = group.checked_sub(1) {
            let (start, end) = (format!("{victim:03}b:"), format!("{victim:03}b;"));
            forget(&engine, start.as_bytes(), end.as_bytes()).await;
            assert_eq!(engine.get(&gkey(victim, 'b', 30)).await.unwrap(), None);
        }
    }
    let last = GROUPS - 1;
    forget(
        &engine,
        format!("{last:03}b:").as_bytes(),
        format!("{last:03}b;").as_bytes(),
    )
    .await;
    stop.store(true, Ordering::Relaxed);
    flusher.await.unwrap();

    let expected: Vec<(Vec<u8>, Vec<u8>)> = (0..GROUPS)
        .flat_map(|group| {
            ['a', 'c'].into_iter().flat_map(move |table| {
                (0..60).map(move |n| (gkey(group, table, n), tvalue(table, n)))
            })
        })
        .collect();
    assert!(
        engine.scan(None, None).await.unwrap() == expected,
        "with ranges still in memory or listed"
    );
    engine.flush().await.unwrap();
    wait_for("every range is retired", || {
        manifest_on_disk(dir.path()).forgotten.is_empty()
    })
    .await;
    assert_eq!(manifest_on_disk(dir.path()).forgotten_seq, GROUPS as u64);
    // Nothing hides the keys any more.
    assert!(
        engine.scan(None, None).await.unwrap() == expected,
        "with the ranges retired"
    );
    engine.close().await.unwrap();
    drop(engine);

    let engine = LsmEngine::open(io(), cfg).await.unwrap();
    assert!(manifest_on_disk(dir.path()).forgotten.is_empty());
    assert!(
        engine.scan(None, None).await.unwrap() == expected,
        "after a restart"
    );
    engine.close().await.unwrap();
}

/// A view is of what has been flushed. One taken after a forgotten range
/// was flushed does not show its keys. One taken before - before the range
/// was forgotten, or before that was flushed - goes on showing all of them,
/// also once the engine has deleted the tables that held them.
#[tokio::test]
async fn a_view_and_a_forgotten_range() {
    let dir = tempfile::tempdir().unwrap();
    // The flush that records the range stops there, so that the tables
    // still hold the range's keys when the view after it is taken.
    let armed = LsmConfig {
        crash_point: Some(CrashPoint::FlushAfterRangesSaved),
        ..config(dir.path())
    };
    let engine = LsmEngine::open(io(), armed).await.unwrap();
    put_keys(&engine, &['b'], 0..300).await;
    engine.flush().await.unwrap();
    put_keys(&engine, &['a', 'b', 'c'], 300..600).await;
    engine.flush().await.unwrap();
    let everything = engine.scan(None, None).await.unwrap();
    assert_eq!(everything.len(), 300 + 3 * 300);

    let before = engine.snapshot_view().await.unwrap();
    forget(&engine, B_START, B_END).await;
    let remaining = engine.scan(None, None).await.unwrap();
    assert_eq!(remaining.len(), 2 * 300);
    // Forgotten, but not flushed: like any write still in memory, it is
    // not in a view.
    let unflushed = engine.snapshot_view().await.unwrap();
    assert!(read_view(unflushed.as_ref()).await == everything);
    assert_eq!(
        unflushed.get(&tkey('b', 10)).await.unwrap(),
        Some(tvalue('b', 10))
    );

    assert!(engine.flush().await.is_err(), "the flush was not stopped");
    assert_eq!(manifest_on_disk(dir.path()).forgotten.len(), 1);
    let after = engine.snapshot_view().await.unwrap();
    assert!(engine.scan(None, None).await.unwrap() == remaining);

    // Read the later view at once, while the tables still hold the keys,
    // and again when they no longer do.
    assert!(read_view(after.as_ref()).await == remaining);
    assert_eq!(after.get(&tkey('b', 10)).await.unwrap(), None);
    assert_eq!(after.get(&tkey('b', 400)).await.unwrap(), None);
    assert_eq!(
        after.get(&tkey('a', 400)).await.unwrap(),
        Some(tvalue('a', 400))
    );
    // The engine stopped by the crash point is done for; another takes
    // over and clears the range's keys away. The views stay as they are.
    drop(engine);
    let engine = LsmEngine::open(io(), config(dir.path())).await.unwrap();
    wait_for("the range is retired", || {
        manifest_on_disk(dir.path()).forgotten.is_empty()
    })
    .await;
    assert!(engine.scan(None, None).await.unwrap() == remaining);
    assert!(read_view(after.as_ref()).await == remaining);
    assert!(read_view(engine.snapshot_view().await.unwrap().as_ref()).await == remaining);

    // The earlier views are the moment before the range went, whole.
    for view in [&before, &unflushed] {
        assert!(read_view(view.as_ref()).await == everything);
        assert_eq!(
            view.get(&tkey('b', 10)).await.unwrap(),
            Some(tvalue('b', 10))
        );
        assert_eq!(
            view.get(&tkey('b', 400)).await.unwrap(),
            Some(tvalue('b', 400))
        );
    }
    drop(before);
    drop(unflushed);
    drop(after);
    engine.close().await.unwrap();
    drop(engine);

    // The same with one engine throughout: the views before and after.
    let dir = tempfile::tempdir().unwrap();
    let engine = LsmEngine::open(io(), config(dir.path())).await.unwrap();
    put_keys(&engine, &['b'], 0..300).await;
    engine.flush().await.unwrap();
    put_keys(&engine, &['a', 'b', 'c'], 300..600).await;
    engine.flush().await.unwrap();
    let before = engine.snapshot_view().await.unwrap();
    forget(&engine, B_START, B_END).await;
    engine.flush().await.unwrap();
    let after = engine.snapshot_view().await.unwrap();
    wait_for("the range is retired", || {
        manifest_on_disk(dir.path()).forgotten.is_empty()
    })
    .await;
    assert!(read_view(after.as_ref()).await == remaining);
    assert!(read_view(before.as_ref()).await == everything);
    drop(before);
    drop(after);
    assert_eq!(sst_files(dir.path()), listed_tables(dir.path()));
    engine.close().await.unwrap();
}

/// Replacing the whole contents starts afresh: ranges forgotten before -
/// still in memory, or listed in the manifest - do not hide what the
/// replacement brings. A range forgotten afterwards is removed from the
/// replacement's tables like any other.
#[tokio::test]
async fn a_replacement_clears_the_forgotten_ranges() {
    /// A snapshot from a node that had not yet forgotten the range.
    async fn replace(engine: &Arc<LsmEngine>) {
        let mut ingest = engine.clone().ingest().await.unwrap();
        for table in ['a', 'b', 'c'] {
            ingest
                .add(
                    (0..300)
                        .map(|n| (tkey(table, n), b"replaced".to_vec()))
                        .collect(),
                )
                .await
                .unwrap();
        }
        ingest.commit().await.unwrap();
    }

    // The range is with a memtable when the replacement comes.
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(LsmEngine::open(io(), config(dir.path())).await.unwrap());
    put_keys(&engine, &['a', 'b', 'c'], 0..300).await;
    engine.flush().await.unwrap();
    forget(&engine, B_START, B_END).await;
    check_only_b_is_gone(&engine, 0..300, "after the drop").await;

    replace(&engine).await;
    assert!(manifest_on_disk(dir.path()).forgotten.is_empty());
    assert_eq!(
        engine.get(&tkey('b', 7)).await.unwrap(),
        Some(b"replaced".to_vec())
    );
    assert_eq!(engine.scan(None, None).await.unwrap().len(), 900);
    // Nor does the range come with a later flush.
    engine.put(b"d", b"later").await.unwrap();
    engine.flush().await.unwrap();
    assert!(manifest_on_disk(dir.path()).forgotten.is_empty());
    assert_eq!(engine.scan(None, None).await.unwrap().len(), 901);
    engine.delete(b"d").await.unwrap();

    // The log is replayed from the snapshot's position and forgets it again.
    forget(&engine, B_START, B_END).await;
    assert_eq!(engine.get(&tkey('b', 7)).await.unwrap(), None);
    assert_eq!(engine.scan(None, None).await.unwrap().len(), 600);
    engine.flush().await.unwrap();
    wait_for("the range is retired", || {
        manifest_on_disk(dir.path()).forgotten.is_empty()
    })
    .await;
    assert_eq!(engine.get(&tkey('b', 7)).await.unwrap(), None);
    let all = engine.scan(None, None).await.unwrap();
    assert_eq!(all.len(), 600);
    assert!(all.iter().all(|(_, value)| value == b"replaced"));
    engine.close().await.unwrap();
    drop(engine);

    // The range is listed in the manifest when the replacement comes: the
    // flush that recorded it was stopped before anything was cleared away.
    let dir = tempfile::tempdir().unwrap();
    let armed = LsmConfig {
        crash_point: Some(CrashPoint::FlushAfterRangesSaved),
        ..config(dir.path())
    };
    let engine = Arc::new(LsmEngine::open(io(), armed).await.unwrap());
    put_keys(&engine, &['a', 'b', 'c'], 0..300).await;
    engine.flush().await.unwrap();
    forget(&engine, B_START, B_END).await;
    assert!(engine.flush().await.is_err(), "the flush was not stopped");
    assert_eq!(manifest_on_disk(dir.path()).forgotten.len(), 1);
    check_only_b_is_gone(&engine, 0..300, "with the range listed").await;

    replace(&engine).await;
    assert!(manifest_on_disk(dir.path()).forgotten.is_empty());
    assert_eq!(
        engine.get(&tkey('b', 7)).await.unwrap(),
        Some(b"replaced".to_vec())
    );
    let all = engine.scan(None, None).await.unwrap();
    assert_eq!(all.len(), 900);
    assert!(all.iter().all(|(_, value)| value == b"replaced"));
    engine.close().await.unwrap();
}

/// What is on disk is the result of applying the log up to the position
/// stored with it: a restart replays the log from there. A forgotten range
/// must therefore become durable together with the position at which it was
/// forgotten, like every other write of that log entry - not ahead of it.
///
/// Here a row is written and flushed with position 1. A later entry
/// (position 2) is applied but not flushed, and then the entry at position
/// 3 forgets the range. The server dies - before any flush, or in the
/// middle of the one that would have made the batch durable. Storage comes
/// back at position 1: at that position the row was there, and the replay
/// of the log from position 1 reads it (a duplicate-key check, a version
/// check). Were the range already forgotten, those checks would see no row,
/// and a changeset could be accepted or refused differently from the first
/// time it was applied.
#[tokio::test]
async fn a_forgotten_range_is_not_durable_ahead_of_its_position() {
    for point in [None, Some(CrashPoint::FlushAfterTableWritten)] {
        let dir = tempfile::tempdir().unwrap();
        let base = config(dir.path());
        let row = tkey('b', 7);

        let engine = LsmEngine::open(io(), base.clone()).await.unwrap();
        engine
            .write_batch(vec![
                WriteOp::Put(row.clone(), b"as of position 1".to_vec()),
                WriteOp::Put(POSITION.to_vec(), vec![1]),
            ])
            .await
            .unwrap();
        engine.flush().await.unwrap();
        engine.close().await.unwrap();
        drop(engine);

        // Position 2 is applied and stays in memory; then the range goes,
        // with position 3.
        let armed = LsmConfig {
            crash_point: point,
            ..base.clone()
        };
        let engine = LsmEngine::open(io(), armed).await.unwrap();
        engine
            .write_batch(vec![WriteOp::Put(POSITION.to_vec(), vec![2])])
            .await
            .unwrap();
        engine
            .write_batch(vec![
                WriteOp::forget(B_START, B_END),
                WriteOp::Put(POSITION.to_vec(), vec![3]),
            ])
            .await
            .unwrap();
        // The range is gone for every reader...
        assert_eq!(engine.get(&row).await.unwrap(), None);
        assert!(engine
            .scan(Some(B_START), Some(B_END))
            .await
            .unwrap()
            .is_empty());
        assert_eq!(engine.get(POSITION).await.unwrap(), Some(vec![3]));
        // ...and nothing of that is durable: what a restart would find is
        // position 1 with its row.
        assert_eq!(engine.get_durable(POSITION).await.unwrap(), Some(vec![1]));
        assert_eq!(
            engine.get_durable(&row).await.unwrap(),
            Some(b"as of position 1".to_vec())
        );
        assert!(manifest_on_disk(dir.path()).forgotten.is_empty());
        if point.is_some() {
            // The server dies while flushing: the table is written, the
            // manifest does not list it, nor the range.
            assert!(engine.flush().await.is_err(), "the flush was not stopped");
            assert!(manifest_on_disk(dir.path()).forgotten.is_empty());
        }
        drop(engine);

        let engine = LsmEngine::open(io(), base).await.unwrap();
        let position = engine.get(POSITION).await.unwrap();
        assert_eq!(
            position,
            Some(vec![1]),
            "{point:?}: storage is back at position 1"
        );
        assert_eq!(
            engine.get(&row).await.unwrap(),
            Some(b"as of position 1".to_vec()),
            "{point:?}: at position 1 the row was there, but storage has already forgotten it"
        );
        assert_eq!(
            engine.scan(Some(B_START), Some(B_END)).await.unwrap().len(),
            1,
            "{point:?}"
        );
        engine.close().await.unwrap();
    }
}

/// The other side of the same rule: once the batch that forgot the range
/// has been flushed, a server that dies comes back at that batch's
/// position with the range forgotten - whether it died as soon as the flush
/// had recorded the range, or later.
#[tokio::test]
async fn a_forgotten_range_is_durable_with_its_position() {
    for point in [Some(CrashPoint::FlushAfterRangesSaved), None] {
        let dir = tempfile::tempdir().unwrap();
        let base = config(dir.path());
        let row = tkey('b', 7);

        let engine = LsmEngine::open(io(), base.clone()).await.unwrap();
        engine
            .write_batch(vec![
                WriteOp::Put(row.clone(), b"as of position 1".to_vec()),
                WriteOp::Put(b"b9".to_vec(), b"neighbour".to_vec()),
                WriteOp::Put(B_END.to_vec(), b"neighbour".to_vec()),
                WriteOp::Put(POSITION.to_vec(), vec![1]),
            ])
            .await
            .unwrap();
        engine.flush().await.unwrap();
        engine.close().await.unwrap();
        drop(engine);

        let armed = LsmConfig {
            crash_point: point,
            ..base.clone()
        };
        let engine = LsmEngine::open(io(), armed).await.unwrap();
        engine
            .write_batch(vec![WriteOp::Put(POSITION.to_vec(), vec![2])])
            .await
            .unwrap();
        engine
            .write_batch(vec![
                WriteOp::forget(B_START, B_END),
                WriteOp::Put(POSITION.to_vec(), vec![3]),
            ])
            .await
            .unwrap();
        let flushed = engine.flush().await;
        assert_eq!(flushed.is_err(), point.is_some(), "{point:?}");
        // Durable now, the range and the position together.
        assert_eq!(engine.get_durable(POSITION).await.unwrap(), Some(vec![3]));
        assert_eq!(engine.get_durable(&row).await.unwrap(), None);
        assert_eq!(manifest_on_disk(dir.path()).forgotten_seq, 1);
        // Crash: no close.
        drop(engine);

        let engine = LsmEngine::open(io(), base).await.unwrap();
        assert_eq!(
            engine.get(POSITION).await.unwrap(),
            Some(vec![3]),
            "{point:?}: storage is at the position of the batch that was flushed"
        );
        assert_eq!(
            engine.get(&row).await.unwrap(),
            None,
            "{point:?}: at position 3 the range is forgotten"
        );
        assert!(engine
            .scan(Some(B_START), Some(B_END))
            .await
            .unwrap()
            .is_empty());
        for neighbour in [&b"b9"[..], B_END] {
            assert_eq!(
                engine.get(neighbour).await.unwrap(),
                Some(b"neighbour".to_vec()),
                "{point:?}"
            );
        }
        wait_for("the range is retired", || {
            manifest_on_disk(dir.path()).forgotten.is_empty()
        })
        .await;
        assert_eq!(engine.get(&row).await.unwrap(), None, "{point:?}");
        engine.close().await.unwrap();
    }
}

/// A crash, and the log replayed. Entry 1 writes a row of the range and is
/// flushed. Entry 2 writes more and entry 3 forgets the range; neither is
/// flushed when the server dies. Storage comes back as entry 1 left it, the
/// row included; applying entries 2 and 3 again brings back exactly the
/// state from before the crash. Applying the last entry once more on top of
/// its own result changes nothing.
#[tokio::test]
async fn replaying_the_log_forgets_the_range_again() {
    let dir = tempfile::tempdir().unwrap();
    let base = config(dir.path());
    let entry_1 = || {
        vec![
            WriteOp::Put(tkey('a', 1), b"a1".to_vec()),
            WriteOp::Put(tkey('b', 1), b"b1".to_vec()),
            WriteOp::Put(tkey('c', 1), b"c1".to_vec()),
            WriteOp::Put(POSITION.to_vec(), vec![1]),
        ]
    };
    let entry_2 = || {
        vec![
            WriteOp::Put(tkey('a', 2), b"a2".to_vec()),
            WriteOp::Put(tkey('b', 2), b"b2".to_vec()),
            WriteOp::Delete(tkey('c', 1)),
            WriteOp::Put(POSITION.to_vec(), vec![2]),
        ]
    };
    let entry_3 = || {
        vec![
            WriteOp::forget(B_START, B_END),
            WriteOp::Put(tkey('c', 3), b"c3".to_vec()),
            WriteOp::Put(POSITION.to_vec(), vec![3]),
        ]
    };
    let after_1 = vec![
        (tkey('a', 1), b"a1".to_vec()),
        (tkey('b', 1), b"b1".to_vec()),
        (tkey('c', 1), b"c1".to_vec()),
        (POSITION.to_vec(), vec![1]),
    ];
    let after_3 = vec![
        (tkey('a', 1), b"a1".to_vec()),
        (tkey('a', 2), b"a2".to_vec()),
        (tkey('c', 3), b"c3".to_vec()),
        (POSITION.to_vec(), vec![3]),
    ];

    let engine = LsmEngine::open(io(), base.clone()).await.unwrap();
    engine.write_batch(entry_1()).await.unwrap();
    engine.flush().await.unwrap();
    engine.write_batch(entry_2()).await.unwrap();
    engine.write_batch(entry_3()).await.unwrap();
    let before_crash = engine.scan(None, None).await.unwrap();
    assert!(before_crash == after_3);
    // Crash: no close, nothing of entries 2 and 3 flushed.
    drop(engine);

    let engine = LsmEngine::open(io(), base.clone()).await.unwrap();
    assert!(
        engine.scan(None, None).await.unwrap() == after_1,
        "storage did not come back as entry 1 left it"
    );
    // The replay, from the position storage holds.
    assert_eq!(engine.get(POSITION).await.unwrap(), Some(vec![1]));
    engine.write_batch(entry_2()).await.unwrap();
    assert_eq!(
        engine.get(&tkey('b', 1)).await.unwrap(),
        Some(b"b1".to_vec())
    );
    assert_eq!(
        engine.get(&tkey('b', 2)).await.unwrap(),
        Some(b"b2".to_vec())
    );
    engine.write_batch(entry_3()).await.unwrap();
    assert!(
        engine.scan(None, None).await.unwrap() == before_crash,
        "the replay did not bring back the state before the crash"
    );

    // This time it is flushed; the server dies afterwards all the same.
    engine.flush().await.unwrap();
    assert!(engine.scan(None, None).await.unwrap() == before_crash);
    drop(engine);
    let engine = LsmEngine::open(io(), base.clone()).await.unwrap();
    assert!(
        engine.scan(None, None).await.unwrap() == before_crash,
        "the flushed state was not kept"
    );
    assert_eq!(engine.get(POSITION).await.unwrap(), Some(vec![3]));

    // The entry that forgot the range, applied once more on top of its own
    // result: nothing changes, in memory or once flushed.
    engine.write_batch(entry_3()).await.unwrap();
    assert!(engine.scan(None, None).await.unwrap() == before_crash);
    engine.flush().await.unwrap();
    wait_for("the range is retired", || {
        manifest_on_disk(dir.path()).forgotten.is_empty()
    })
    .await;
    assert!(engine.scan(None, None).await.unwrap() == before_crash);
    engine.close().await.unwrap();
    drop(engine);

    let engine = LsmEngine::open(io(), base).await.unwrap();
    assert!(engine.scan(None, None).await.unwrap() == before_crash);
    engine.close().await.unwrap();
}

/// The writes of a batch take effect in the order they are listed. A key of
/// the range written before the range is forgotten - in a table, in the
/// memtable, or earlier in the same batch - is forgotten. One written
/// afterwards, even in the same batch, is a new write and stays: in memory,
/// once flushed, once the range has been retired, and over a restart.
/// (Callers promise not to write to a forgotten range again; what happens
/// if one does is defined all the same.)
#[tokio::test]
async fn a_batch_forgets_what_came_before_and_keeps_what_comes_after() {
    let dir = tempfile::tempdir().unwrap();
    let base = config(dir.path());
    let engine = LsmEngine::open(io(), base.clone()).await.unwrap();

    // In a table, and in the memtable.
    engine.put(b"b9", b"neighbour").await.unwrap();
    engine.put(&tkey('b', 1), b"in a table").await.unwrap();
    engine.put(&tkey('b', 2), b"in a table").await.unwrap();
    engine.put(&tkey('b', 5), b"in a table").await.unwrap();
    engine.flush().await.unwrap();
    engine.put(&tkey('b', 3), b"in memory").await.unwrap();

    engine
        .write_batch(vec![
            // Put, then forgotten.
            WriteOp::Put(tkey('b', 4), b"earlier in the batch".to_vec()),
            WriteOp::Delete(tkey('b', 5)),
            WriteOp::forget(B_START, B_END),
            // Forgotten, then put: a key that was there before and a new one.
            WriteOp::Put(tkey('b', 2), b"written again".to_vec()),
            WriteOp::Put(tkey('b', 6), b"later in the batch".to_vec()),
            WriteOp::Put(POSITION.to_vec(), vec![1]),
        ])
        .await
        .unwrap();

    let in_range = vec![
        (tkey('b', 2), b"written again".to_vec()),
        (tkey('b', 6), b"later in the batch".to_vec()),
    ];
    async fn check(engine: &LsmEngine, in_range: &[(Vec<u8>, Vec<u8>)], when: &str) {
        for n in [1, 3, 4, 5] {
            assert_eq!(
                engine.get(&tkey('b', n)).await.unwrap(),
                None,
                "{when}: {n}"
            );
        }
        for (key, value) in in_range {
            assert_eq!(
                engine.get(key).await.unwrap().as_ref(),
                Some(value),
                "{when}"
            );
        }
        assert!(
            engine.scan(Some(B_START), Some(B_END)).await.unwrap() == in_range,
            "{when}"
        );
        let keys: Vec<Vec<u8>> = in_range.iter().map(|(key, _)| key.clone()).collect();
        let mut forward = vec![];
        let mut at = b"b9".to_vec();
        while let Some(next) = engine.next_key(&at, B_END).await.unwrap() {
            forward.push(next.clone());
            at = next;
        }
        assert_eq!(forward, keys, "{when}: forward");
        let mut backward = vec![];
        let mut at = B_END.to_vec();
        while let Some(prev) = engine.prev_key(&at, B_START).await.unwrap() {
            backward.push(prev.clone());
            at = prev;
        }
        backward.reverse();
        assert_eq!(backward, keys, "{when}: backward");
        assert_eq!(
            engine.get(b"b9").await.unwrap(),
            Some(b"neighbour".to_vec()),
            "{when}"
        );
    }
    check(&engine, &in_range, "in memory").await;
    // On disk it is all as before the batch.
    assert_eq!(
        engine.get_durable(&tkey('b', 2)).await.unwrap(),
        Some(b"in a table".to_vec())
    );
    assert_eq!(engine.get_durable(&tkey('b', 6)).await.unwrap(), None);

    engine.flush().await.unwrap();
    check(&engine, &in_range, "flushed").await;
    assert_eq!(
        engine.get_durable(&tkey('b', 2)).await.unwrap(),
        Some(b"written again".to_vec())
    );
    assert_eq!(engine.get_durable(&tkey('b', 1)).await.unwrap(), None);
    let view = engine.snapshot_view().await.unwrap();
    let in_view: Vec<_> = read_view(view.as_ref())
        .await
        .into_iter()
        .filter(|(key, _)| key.as_slice() >= B_START && key.as_slice() < B_END)
        .collect();
    assert!(in_view == in_range, "a view after the flush");
    drop(view);
    wait_for("the range is retired", || {
        manifest_on_disk(dir.path()).forgotten.is_empty()
    })
    .await;
    check(&engine, &in_range, "retired").await;
    // Crash: no close.
    drop(engine);

    let engine = LsmEngine::open(io(), base.clone()).await.unwrap();
    check(&engine, &in_range, "after a restart").await;

    // Across batches it is the same: a later batch's write to the range
    // stays, while the range is in memory, listed, and retired; and a
    // range forgotten again takes it.
    forget(&engine, B_START, B_END).await;
    check(&engine, &[], "forgotten again").await;
    let later = vec![(tkey('b', 7), b"a later batch".to_vec())];
    engine.put(&later[0].0, &later[0].1).await.unwrap();
    check(&engine, &later, "a later batch, in memory").await;
    engine.flush().await.unwrap();
    check(&engine, &later, "a later batch, flushed").await;
    wait_for("the range is retired again", || {
        manifest_on_disk(dir.path()).forgotten.is_empty()
    })
    .await;
    check(&engine, &later, "a later batch, retired").await;
    forget(&engine, B_START, B_END).await;
    check(&engine, &[], "forgotten a third time").await;
    engine.close().await.unwrap();
    drop(engine);

    let engine = LsmEngine::open(io(), base).await.unwrap();
    check(&engine, &[], "after the last restart").await;
    engine.close().await.unwrap();
}

/// A range forgotten in one memtable hides what older memtables and tables
/// hold, and nothing newer, while each of them is flushed in its turn - the
/// flush of an older memtable must not carry the range to disk ahead of
/// the batch that forgot it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_range_forgotten_between_memtables_goes_to_disk_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = LsmConfig {
        // Small memtables: every round below fills and freezes several.
        total_cache_bytes: 128 * 1024,
        // No compaction gets in the way of looking at the tables.
        l0_compaction_trigger: 1000,
        ..config(dir.path())
    };
    let engine = LsmEngine::open(io(), cfg.clone()).await.unwrap();
    // Position 1: rows of all three kinds, enough for several memtables
    // (frozen and flushed in the background as they fill).
    for n in 0..1500 {
        engine
            .write_batch(vec![
                WriteOp::Put(tkey('a', n), tvalue('a', n)),
                WriteOp::Put(tkey('b', n), tvalue('b', n)),
                WriteOp::Put(tkey('c', n), tvalue('c', n)),
                WriteOp::Put(POSITION.to_vec(), vec![1]),
            ])
            .await
            .unwrap();
    }
    // Position 2 forgets the range; position 3 goes on writing, into the
    // range too, enough to freeze the memtable that holds the range.
    engine
        .write_batch(vec![
            WriteOp::forget(B_START, B_END),
            WriteOp::Put(POSITION.to_vec(), vec![2]),
        ])
        .await
        .unwrap();
    for n in 1500..3000 {
        engine
            .write_batch(vec![
                WriteOp::Put(tkey('a', n), tvalue('a', n)),
                WriteOp::Put(tkey('b', n), tvalue('b', n)),
                WriteOp::Put(tkey('c', n), tvalue('c', n)),
                WriteOp::Put(POSITION.to_vec(), vec![3]),
            ])
            .await
            .unwrap();
        // At every moment, whatever is in memory and whatever is flushed:
        // the rows from before the range went are gone, those after it are
        // there.
        if n % 100 == 0 {
            assert_eq!(engine.get(&tkey('b', 700)).await.unwrap(), None);
            assert_eq!(
                engine.get(&tkey('b', n)).await.unwrap(),
                Some(tvalue('b', n))
            );
            let in_range = engine.scan(Some(B_START), Some(B_END)).await.unwrap();
            assert_eq!(in_range.len(), n as usize - 1500 + 1);
            assert_eq!(in_range[0].0, tkey('b', 1500));
            assert_eq!(
                engine.next_key(B_START, B_END).await.unwrap(),
                Some(tkey('b', 1500))
            );
            // What is durable is a position and exactly its state: the
            // range's old rows are there up to position 1 and gone from
            // position 2 on. Flushes go on between these three reads, but
            // only forwards: a row durably gone before the position is
            // read means the position is 2 or more by then, and a position
            // of 2 or more means the row is durably gone afterwards.
            let old_row = tkey('b', 0);
            let gone_before = engine.get_durable(&old_row).await.unwrap().is_none();
            let position = engine.get_durable(POSITION).await.unwrap();
            let gone_after = engine.get_durable(&old_row).await.unwrap().is_none();
            let forgot = position.is_some_and(|position| position[0] >= 2);
            assert!(
                !gone_before || forgot,
                "the range was durable ahead of its position"
            );
            assert!(
                !forgot || gone_after,
                "the position was durable ahead of its range"
            );
        }
    }
    let expected: Vec<(Vec<u8>, Vec<u8>)> = (0..3000)
        .map(|n| (tkey('a', n), tvalue('a', n)))
        .chain((1500..3000).map(|n| (tkey('b', n), tvalue('b', n))))
        .chain((0..3000).map(|n| (tkey('c', n), tvalue('c', n))))
        .chain([(POSITION.to_vec(), vec![3])])
        .collect();
    assert!(engine.scan(None, None).await.unwrap() == expected);
    engine.flush().await.unwrap();
    assert_eq!(manifest_on_disk(dir.path()).forgotten_seq, 1);
    assert!(engine.scan(None, None).await.unwrap() == expected);
    wait_for("the range is retired", || {
        manifest_on_disk(dir.path()).forgotten.is_empty()
    })
    .await;
    assert!(engine.scan(None, None).await.unwrap() == expected);
    engine.close().await.unwrap();
    drop(engine);

    let engine = LsmEngine::open(io(), cfg).await.unwrap();
    assert!(engine.scan(None, None).await.unwrap() == expected);
    engine.close().await.unwrap();
}
