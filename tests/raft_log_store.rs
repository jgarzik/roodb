//! Tests for the on-disk Raft log: what it promises openraft, and what it
//! recovers after a restart or a crash.

use std::path::Path;

use openraft::entry::RaftEntry;
use openraft::storage::{IOFlushed, RaftLogReader, RaftLogStorage};
use openraft::testing::log_id;
use openraft::type_config::TypeConfigExt;
use roodb::io::{IoBackend, IoEngine, PAGE_SIZE};
use roodb::raft::{
    ChangeSet, Command, Durability, Entry, LogId, LogStore, LogStoreConfig, RowChange, TypeConfig,
    Vote,
};

fn io() -> IoEngine {
    IoEngine::new(IoBackend::from_env().unwrap()).unwrap()
}

fn config(dir: &Path, durability: Durability) -> LogStoreConfig {
    LogStoreConfig {
        dir: dir.to_path_buf(),
        durability,
        // Small segments so that tests cross segment boundaries quickly.
        segment_bytes: 64 * PAGE_SIZE as u64,
        cache_bytes: 1 << 20,
    }
}

fn id(index: u64) -> LogId {
    log_id::<TypeConfig>(1, 1, index)
}

/// An entry whose payload identifies its index and has the given size.
fn entry_sized(index: u64, payload: usize) -> Entry {
    let value: Vec<u8> = (0..payload).map(|i| (index as usize + i) as u8).collect();
    let changes = vec![RowChange::insert(
        "t",
        format!("key{index:08}").into_bytes(),
        value,
    )];
    Entry::new_normal(
        id(index),
        Command::DataChange(ChangeSet::new_with_changes(index, changes)),
    )
}

fn entry(index: u64) -> Entry {
    entry_sized(index, 100)
}

/// Append entries and wait until the store reports them written.
async fn append(store: &mut LogStore, entries: Vec<Entry>) {
    let (tx, rx) = TypeConfig::oneshot();
    store.append(entries, IOFlushed::signal(tx)).await.unwrap();
    rx.await.unwrap().unwrap();
}

async fn read(store: &mut LogStore, range: std::ops::Range<u64>) -> Vec<Entry> {
    store.try_get_log_entries(range).await.unwrap()
}

fn segment_files(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|n| n.starts_with("log-"))
        .collect();
    names.sort();
    names
}

#[tokio::test]
async fn a_new_log_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = LogStore::open(io(), config(dir.path(), Durability::Os))
        .await
        .unwrap();
    let state = store.get_log_state().await.unwrap();
    assert_eq!(state.last_log_id, None);
    assert_eq!(state.last_purged_log_id, None);
    assert_eq!(store.read_vote().await.unwrap(), None);
    assert_eq!(store.read_committed().await.unwrap(), None);
    assert!(read(&mut store, 0..100).await.is_empty());
}

#[tokio::test]
async fn entries_are_readable_at_once_and_after_reopening() {
    for durability in [Durability::Os, Durability::Fsync] {
        let dir = tempfile::tempdir().unwrap();
        let mut store = LogStore::open(io(), config(dir.path(), durability))
            .await
            .unwrap();

        // Readable as soon as append returns, before the write completes.
        let (tx, rx) = TypeConfig::oneshot();
        store
            .append(vec![entry(0), entry(1)], IOFlushed::signal(tx))
            .await
            .unwrap();
        assert_eq!(read(&mut store, 0..2).await, vec![entry(0), entry(1)]);
        rx.await.unwrap().unwrap();

        append(&mut store, (2..50).map(entry).collect()).await;
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id, Some(id(49)));
        assert_eq!(
            read(&mut store, 10..13).await,
            vec![entry(10), entry(11), entry(12)]
        );
        assert_eq!(read(&mut store, 48..500).await, vec![entry(48), entry(49)]);

        let vote = Vote::new(3, 1);
        store.save_vote(&vote).await.unwrap();
        store.save_committed(Some(id(40))).await.unwrap();
        // The commit point travels with the next append.
        append(&mut store, vec![entry(50)]).await;

        store.close().await;
        drop(store);

        let mut store = LogStore::open(io(), config(dir.path(), durability))
            .await
            .unwrap();
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id, Some(id(50)), "{durability}");
        assert_eq!(state.last_purged_log_id, None);
        assert_eq!(store.read_vote().await.unwrap(), Some(vote));
        assert_eq!(store.read_committed().await.unwrap(), Some(id(40)));
        let all = read(&mut store, 0..51).await;
        assert_eq!(all.len(), 51);
        for (i, e) in all.iter().enumerate() {
            assert_eq!(e, &entry(i as u64), "{durability}: entry {i}");
        }

        // And it keeps going where it left off.
        append(&mut store, vec![entry(51)]).await;
        assert_eq!(read(&mut store, 51..52).await, vec![entry(51)]);
    }
}

/// The reason for upgrading openraft: commits that arrive together share a
/// write, and with fsync durability they share the sync.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commits_arriving_together_share_a_write() {
    const COMMITS: u64 = 3000;
    for durability in [Durability::Os, Durability::Fsync] {
        let dir = tempfile::tempdir().unwrap();
        let cfg = LogStoreConfig {
            segment_bytes: 16 << 20,
            ..config(dir.path(), durability)
        };
        let mut store = LogStore::open(io(), cfg).await.unwrap();
        let before = store.stats();

        // openraft hands appends over one after another without waiting for
        // each to be flushed.
        let mut waiting = Vec::new();
        for index in 0..COMMITS {
            let (tx, rx) = TypeConfig::oneshot();
            store
                .append(vec![entry(index)], IOFlushed::signal(tx))
                .await
                .unwrap();
            waiting.push(rx);
        }
        for rx in waiting {
            rx.await.unwrap().unwrap();
        }

        let stats = store.stats();
        let writes = stats.writes - before.writes;
        assert_eq!(stats.entries - before.entries, COMMITS);
        assert!(
            writes < COMMITS / 4,
            "{durability}: {writes} writes for {COMMITS} commits"
        );
        match durability {
            Durability::Os => assert_eq!(stats.syncs, before.syncs, "os: no sync per commit"),
            Durability::Fsync => assert_eq!(
                stats.syncs - before.syncs,
                writes,
                "fsync: every write of commits is synced"
            ),
        }
        assert_eq!(read(&mut store, 0..COMMITS).await.len() as u64, COMMITS);
    }
}

#[tokio::test]
async fn a_vote_is_synced_even_without_fsync_durability() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = LogStore::open(io(), config(dir.path(), Durability::Os))
        .await
        .unwrap();
    append(&mut store, vec![entry(0)]).await;
    let before = store.stats();
    store.save_vote(&Vote::new(2, 1)).await.unwrap();
    assert_eq!(store.stats().syncs - before.syncs, 1);
}

#[tokio::test]
async fn the_log_spans_segments_and_survives_reopening() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), Durability::Os);
    let mut store = LogStore::open(io(), cfg.clone()).await.unwrap();

    // ~1 KiB entries into 256 KiB segments: several segments.
    for chunk in (0..1500u64).collect::<Vec<_>>().chunks(50) {
        append(
            &mut store,
            chunk.iter().map(|i| entry_sized(*i, 1000)).collect(),
        )
        .await;
    }
    assert!(store.stats().segments >= 4);
    assert!(segment_files(dir.path()).len() >= 5);
    store.close().await;
    drop(store);

    let mut store = LogStore::open(io(), cfg).await.unwrap();
    assert_eq!(
        store.get_log_state().await.unwrap().last_log_id,
        Some(id(1499))
    );
    for index in (0..1500).step_by(37) {
        assert_eq!(
            read(&mut store, index..index + 1).await,
            vec![entry_sized(index, 1000)],
            "entry {index}"
        );
    }
}

#[tokio::test]
async fn an_entry_larger_than_a_segment_gets_a_segment_to_fit() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), Durability::Os);
    let mut store = LogStore::open(io(), cfg.clone()).await.unwrap();
    let big = 5 * cfg.segment_bytes as usize;

    append(&mut store, vec![entry(0)]).await;
    append(&mut store, vec![entry_sized(1, big)]).await;
    append(&mut store, vec![entry(2)]).await;
    assert_eq!(read(&mut store, 1..2).await, vec![entry_sized(1, big)]);
    store.close().await;
    drop(store);

    let mut store = LogStore::open(io(), cfg).await.unwrap();
    assert_eq!(
        read(&mut store, 0..3).await,
        vec![entry(0), entry_sized(1, big), entry(2)]
    );
}

#[tokio::test]
async fn entries_dropped_from_memory_are_read_back_from_disk() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = LogStoreConfig {
        cache_bytes: 16 * 1024,
        ..config(dir.path(), Durability::Os)
    };
    let mut store = LogStore::open(io(), cfg.clone()).await.unwrap();
    for chunk in (0..800u64).collect::<Vec<_>>().chunks(40) {
        append(
            &mut store,
            chunk.iter().map(|i| entry_sized(*i, 500)).collect(),
        )
        .await;
    }
    // Far more than 16 KiB has gone in; the early entries are on disk only.
    for index in [0u64, 1, 250, 799] {
        assert_eq!(
            read(&mut store, index..index + 1).await,
            vec![entry_sized(index, 500)],
            "entry {index}"
        );
    }
    assert_eq!(read(&mut store, 0..800).await.len(), 800);
    store.close().await;
    drop(store);

    // Recovery respects the budget too.
    let mut store = LogStore::open(io(), cfg).await.unwrap();
    assert_eq!(read(&mut store, 0..800).await.len(), 800);
    assert_eq!(read(&mut store, 3..4).await, vec![entry_sized(3, 500)]);
}

#[tokio::test]
async fn truncated_entries_are_replaced_and_stay_replaced() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), Durability::Os);
    let mut store = LogStore::open(io(), cfg.clone()).await.unwrap();
    append(&mut store, (0..20).map(entry).collect()).await;

    // A new leader overwrites entries 10.. with its own.
    store.truncate_after(Some(id(9))).await.unwrap();
    assert_eq!(
        store.get_log_state().await.unwrap().last_log_id,
        Some(id(9))
    );
    assert!(read(&mut store, 10..20).await.is_empty());

    let replacement = |index: u64| {
        Entry::new_normal(
            log_id::<TypeConfig>(2, 2, index),
            Command::DataChange(ChangeSet::new_with_changes(index + 1000, vec![])),
        )
    };
    append(&mut store, (10..15).map(replacement).collect()).await;
    store.close().await;
    drop(store);

    let mut store = LogStore::open(io(), cfg.clone()).await.unwrap();
    assert_eq!(
        store.get_log_state().await.unwrap().last_log_id,
        Some(log_id::<TypeConfig>(2, 2, 14))
    );
    let all = read(&mut store, 0..100).await;
    assert_eq!(all.len(), 15);
    assert_eq!(all[9], entry(9));
    assert_eq!(all[10], replacement(10));
    assert_eq!(all[14], replacement(14));

    // Truncating everything leaves an empty log that can start again at 0.
    store.truncate_after(None).await.unwrap();
    assert_eq!(store.get_log_state().await.unwrap().last_log_id, None);
    append(&mut store, vec![entry(0)]).await;
    store.close().await;
    drop(store);
    let mut store = LogStore::open(io(), cfg).await.unwrap();
    assert_eq!(read(&mut store, 0..100).await, vec![entry(0)]);
}

#[tokio::test]
async fn purging_removes_old_segments_and_keeps_the_rest() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), Durability::Os);
    let mut store = LogStore::open(io(), cfg.clone()).await.unwrap();
    let vote = Vote::new(7, 1);
    store.save_vote(&vote).await.unwrap();
    for chunk in (0..1500u64).collect::<Vec<_>>().chunks(50) {
        append(
            &mut store,
            chunk.iter().map(|i| entry_sized(*i, 1000)).collect(),
        )
        .await;
    }
    let before = segment_files(dir.path()).len();
    assert!(before >= 5);

    store.purge(id(1199)).await.unwrap();
    let state = store.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(id(1199)));
    assert_eq!(state.last_log_id, Some(id(1499)));
    assert!(read(&mut store, 0..1200).await.is_empty());
    assert_eq!(read(&mut store, 1200..1500).await.len(), 300);
    // One more write so the deletions queued behind the purge have run.
    append(&mut store, vec![entry_sized(1500, 1000)]).await;
    store.close().await;
    let after = segment_files(dir.path()).len();
    assert!(
        after < before,
        "{before} segments before purge, {after} after"
    );
    drop(store);

    // The vote was written in the first segment, which is gone; it must have
    // been carried forward.
    let mut store = LogStore::open(io(), cfg.clone()).await.unwrap();
    assert_eq!(store.read_vote().await.unwrap(), Some(vote));
    let state = store.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(id(1199)));
    assert_eq!(state.last_log_id, Some(id(1500)));
    assert_eq!(
        read(&mut store, 1200..1201).await,
        vec![entry_sized(1200, 1000)]
    );
    assert_eq!(
        read(&mut store, 1500..1501).await,
        vec![entry_sized(1500, 1000)]
    );

    // Purging everything leaves a log that continues after the purge point.
    store.purge(id(1500)).await.unwrap();
    let state = store.get_log_state().await.unwrap();
    assert_eq!(state.last_log_id, Some(id(1500)));
    append(&mut store, vec![entry(1501)]).await;
    store.close().await;
    drop(store);
    let mut store = LogStore::open(io(), cfg).await.unwrap();
    let state = store.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(id(1500)));
    assert_eq!(state.last_log_id, Some(id(1501)));
    assert_eq!(read(&mut store, 0..2000).await, vec![entry(1501)]);
}

/// A purge past the end of the log, as after installing a snapshot.
#[tokio::test]
async fn purging_beyond_the_last_entry_moves_the_log_forward() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), Durability::Os);
    let mut store = LogStore::open(io(), cfg.clone()).await.unwrap();
    append(&mut store, (0..5).map(entry).collect()).await;
    store.purge(id(99)).await.unwrap();
    let state = store.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(id(99)));
    assert_eq!(state.last_log_id, Some(id(99)));
    append(&mut store, vec![entry(100)]).await;
    store.close().await;
    drop(store);

    let mut store = LogStore::open(io(), cfg).await.unwrap();
    let state = store.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(id(99)));
    assert_eq!(state.last_log_id, Some(id(100)));
    assert_eq!(read(&mut store, 0..200).await, vec![entry(100)]);
}

/// A crash can leave the last write half done. Whatever the damage to the end
/// of the newest segment, the log reopens with a clean prefix and carries on.
#[tokio::test]
async fn a_torn_tail_is_cut_off_at_the_last_whole_record() {
    let base = tempfile::tempdir().unwrap();
    let cfg = config(base.path(), Durability::Os);
    {
        let mut store = LogStore::open(io(), cfg.clone()).await.unwrap();
        for index in 0..40 {
            append(&mut store, vec![entry_sized(index, 300)]).await;
        }
        store.close().await;
    }
    let segment = base.path().join(&segment_files(base.path())[0]);
    let pristine = std::fs::read(&segment).unwrap();
    // Records start after the one-page header; find where the data ends.
    let used = pristine.iter().rposition(|b| *b != 0).unwrap() + 1;

    for damage_at in [
        PAGE_SIZE + 1,
        PAGE_SIZE + 400,
        used / 2,
        used - 300,
        used - 5,
        used - 1,
    ] {
        for garbage in [0x00u8, 0xFF, 0x5A] {
            let dir = tempfile::tempdir().unwrap();
            let mut bytes = pristine.clone();
            for b in &mut bytes[damage_at..used + 64] {
                *b = garbage;
            }
            std::fs::write(dir.path().join(segment.file_name().unwrap()), &bytes).unwrap();

            let cfg = config(dir.path(), Durability::Os);
            let mut store = LogStore::open(io(), cfg.clone()).await.unwrap_or_else(|e| {
                panic!("damage at {damage_at} with {garbage:#x}: open failed: {e}")
            });
            let survivors = read(&mut store, 0..100).await;
            assert!(survivors.len() < 40, "damage at {damage_at} lost nothing?");
            for (i, e) in survivors.iter().enumerate() {
                assert_eq!(e, &entry_sized(i as u64, 300), "damage at {damage_at}");
            }
            let next = survivors.len() as u64;
            assert_eq!(
                store.get_log_state().await.unwrap().last_log_id,
                next.checked_sub(1).map(id)
            );

            // New entries go where the damage was, and reopening sees them.
            append(&mut store, vec![entry(next), entry(next + 1)]).await;
            store.close().await;
            drop(store);
            let mut store = LogStore::open(io(), cfg).await.unwrap();
            let all = read(&mut store, 0..100).await;
            assert_eq!(all.len() as u64, next + 2, "damage at {damage_at}");
            assert_eq!(all[next as usize], entry(next));
            assert_eq!(all[next as usize + 1], entry(next + 1));
        }
    }
}

/// Damage anywhere but the end of the newest segment is not a torn write. It
/// must be reported, not papered over by dropping everything after it.
#[tokio::test]
async fn damage_in_an_earlier_segment_fails_open() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), Durability::Os);
    {
        let mut store = LogStore::open(io(), cfg.clone()).await.unwrap();
        for chunk in (0..1000u64).collect::<Vec<_>>().chunks(50) {
            append(
                &mut store,
                chunk.iter().map(|i| entry_sized(*i, 1000)).collect(),
            )
            .await;
        }
        store.close().await;
    }
    let files = segment_files(dir.path());
    assert!(files.len() >= 3);
    let path = dir.path().join(&files[1]);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[PAGE_SIZE + 2000] ^= 0xFF;
    std::fs::write(&path, &bytes).unwrap();

    let result = LogStore::open(io(), cfg).await;
    assert!(
        result.is_err(),
        "damage in the middle of the log went unnoticed"
    );
}

/// A crash while a new segment is being started leaves a file with no valid
/// header. Nothing was ever written to it, so it is dropped.
#[tokio::test]
async fn a_segment_that_was_never_started_is_discarded() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), Durability::Os);
    {
        let mut store = LogStore::open(io(), cfg.clone()).await.unwrap();
        for chunk in (0..600u64).collect::<Vec<_>>().chunks(50) {
            append(
                &mut store,
                chunk.iter().map(|i| entry_sized(*i, 1000)).collect(),
            )
            .await;
        }
        store.close().await;
    }
    let files = segment_files(dir.path());
    let last = dir.path().join(files.last().unwrap());
    let last_len = std::fs::metadata(&last).unwrap().len();
    // Entries the last segment held are lost with it in this simulation, so
    // rebuild the scene: an all-zero file where the next segment would be.
    let next_seq = files.len() as u64 + 1;
    std::fs::write(
        dir.path().join(format!("log-{next_seq:016}.seg")),
        vec![0u8; last_len as usize],
    )
    .unwrap();

    let mut store = LogStore::open(io(), cfg.clone()).await.unwrap();
    assert_eq!(read(&mut store, 0..1000).await.len(), 600);
    append(&mut store, vec![entry(600)]).await;
    store.close().await;
    drop(store);
    let mut store = LogStore::open(io(), cfg).await.unwrap();
    assert_eq!(read(&mut store, 0..1000).await.len(), 601);
}

/// Purged segments are reused, so a new segment can start life full of an old
/// segment's records. They must not be mistaken for its own.
#[tokio::test]
async fn records_of_a_recycled_segment_are_not_replayed() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), Durability::Os);
    let mut store = LogStore::open(io(), cfg.clone()).await.unwrap();

    // Fill and purge repeatedly so segments are recycled several times over.
    let mut next = 0u64;
    for _round in 0..6 {
        for _ in 0..12 {
            let batch: Vec<Entry> = (next..next + 50).map(|i| entry_sized(i, 1000)).collect();
            next += 50;
            append(&mut store, batch).await;
        }
        store.purge(id(next - 100)).await.unwrap();
    }
    append(&mut store, vec![entry_sized(next, 1000)]).await;
    next += 1;
    store.close().await;
    drop(store);

    let mut store = LogStore::open(io(), cfg).await.unwrap();
    let state = store.get_log_state().await.unwrap();
    assert_eq!(state.last_log_id, Some(id(next - 1)));
    let purged = state.last_purged_log_id.unwrap().index();
    let live = read(&mut store, 0..next).await;
    assert_eq!(live.len() as u64, next - 1 - purged);
    for e in &live {
        assert_eq!(e, &entry_sized(e.log_id().index(), 1000));
    }
}

/// A recycled file renamed into place but not yet given its new header still
/// carries the header of the segment it used to be.
#[tokio::test]
async fn a_recycled_file_with_its_old_header_was_never_started() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), Durability::Os);
    {
        let mut store = LogStore::open(io(), cfg.clone()).await.unwrap();
        append(&mut store, (0..20).map(entry).collect()).await;
        store.close().await;
    }
    let files = segment_files(dir.path());
    assert_eq!(files.len(), 1);
    // A copy of segment 1, records and all, posing as segment 2.
    std::fs::copy(
        dir.path().join(&files[0]),
        dir.path().join(format!("log-{:016}.seg", 2)),
    )
    .unwrap();

    let mut store = LogStore::open(io(), cfg).await.unwrap();
    assert_eq!(read(&mut store, 0..100).await.len(), 20);
    assert_eq!(
        store.get_log_state().await.unwrap().last_log_id,
        Some(id(19))
    );
}

/// A header that is neither blank nor another segment's is damage.
#[tokio::test]
async fn a_garbled_header_on_the_newest_segment_fails_open() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), Durability::Os);
    {
        let mut store = LogStore::open(io(), cfg.clone()).await.unwrap();
        append(&mut store, (0..20).map(entry).collect()).await;
        store.close().await;
    }
    let path = dir.path().join(&segment_files(dir.path())[0]);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[40] ^= 0xFF;
    std::fs::write(&path, &bytes).unwrap();
    assert!(LogStore::open(io(), cfg).await.is_err());
}
