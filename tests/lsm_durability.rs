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

/// Initialisation stores its rows under generated row IDs, and records how
/// far the IDs have been used: a server that starts on this storage hands
/// out IDs above them, not the same ones again.
#[tokio::test]
async fn initialisation_records_the_row_ids_it_used() {
    let dir = tempfile::tempdir().unwrap();
    let engine = LsmEngine::open(io(), config(dir.path())).await.unwrap();
    let storage: Arc<dyn StorageEngine> = Arc::new(engine);
    roodb::init::initialize_database(&storage, "secret")
        .await
        .unwrap();

    let rows = storage.scan(Some(b"t:"), Some(b"t;")).await.unwrap();
    assert!(rows.len() >= 4, "root, its grant and two databases");
    let highest = rows
        .iter()
        .map(|(key, _)| u64::from_be_bytes(key[key.len() - 8..].try_into().unwrap()))
        .max()
        .unwrap();
    let bound = storage
        .get(roodb::raft::ROW_ID_KEY)
        .await
        .unwrap()
        .map(|stored| u64::from_le_bytes(stored.as_slice().try_into().unwrap()))
        .expect("no row ID bound was recorded");
    assert!(
        bound > highest,
        "IDs up to {highest} are in use, but the recorded bound is {bound}"
    );
    storage.close().await.unwrap();
}
