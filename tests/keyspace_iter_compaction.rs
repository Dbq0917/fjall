use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Barrier,
};

fn open(folder: &tempfile::TempDir, kv_separation: bool) -> fjall::Result<(Database, Keyspace)> {
    let db = Database::builder(folder)
        .worker_threads_unchecked(0)
        .open()?;

    let tree = db.keyspace("default", || {
        let options = KeyspaceCreateOptions::default().compaction_strategy(Arc::new(
            fjall::compaction::Leveled::default().with_l0_threshold(2),
        ));

        if kv_separation {
            options.with_kv_separation(Default::default())
        } else {
            options
        }
    })?;

    Ok((db, tree))
}

/// Writes one append-only table per round.
fn write_rounds(tree: &Keyspace, rounds: std::ops::Range<u16>) -> fjall::Result<usize> {
    let mut expected = 0;

    for round in rounds {
        for key in 0u32..10_000 {
            let key = (u64::from(round) << 32) | u64::from(key);
            tree.insert(key.to_be_bytes(), round.to_be_bytes())?;
            expected += 1;
        }
        tree.flush(true)?;
    }

    Ok(expected)
}

#[test]
fn iter_survives_compaction() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let (db, tree) = open(&folder, false)?;
    let expected = write_rounds(&tree, 0..8)?;

    let mut count = 0;
    let mut compacted = false;

    for guard in tree.iter() {
        guard.key()?;
        count += 1;

        if count == 1_000 {
            tree.compact()?;
            compacted = true;
        }
    }

    assert!(compacted);
    assert!(db.compactions_completed() > 0);
    assert_eq!(expected, count);

    Ok(())
}

#[test]
fn range_and_prefix_survive_compaction() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let (_db, tree) = open(&folder, false)?;
    let expected = write_rounds(&tree, 0..8)?;

    for make_iter in [
        |tree: &Keyspace| tree.range::<fjall::Slice, _>(..),
        |tree: &Keyspace| tree.prefix([]),
    ] {
        let mut count = 0;

        for guard in make_iter(&tree) {
            guard.key()?;
            count += 1;

            if count == 1_000 {
                tree.compact()?;
            }
        }

        assert_eq!(expected, count);
    }

    Ok(())
}

#[test]
fn blob_iter_survives_compaction() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let (_db, tree) = open(&folder, true)?;
    let expected = write_rounds(&tree, 0..8)?;

    let mut count = 0;

    for guard in tree.iter() {
        guard.into_inner()?;
        count += 1;

        if count == 1_000 {
            tree.compact()?;
        }
    }

    assert_eq!(expected, count);

    Ok(())
}

#[test]
fn background_compaction_does_not_truncate_iter() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let (_db, tree) = open(&folder, false)?;
    let expected = write_rounds(&tree, 0..16)?;

    let done = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(2));

    std::thread::scope(|s| {
        let bg_done = done.clone();
        let bg_barrier = barrier.clone();
        let bg_tree = tree.clone();

        s.spawn(move || {
            bg_barrier.wait();

            while !bg_done.load(Ordering::Relaxed) {
                bg_tree.compact().expect("compaction should not fail");
            }
        });

        let mut iter = tree.iter();
        barrier.wait();

        let mut count = 0;
        for guard in &mut iter {
            guard.into_inner()?;
            count += 1;
        }

        done.store(true, Ordering::Relaxed);
        assert_eq!(expected, count);

        Ok::<_, fjall::Error>(())
    })?;

    Ok(())
}
