use fjall::Database;
use std::io::{BufRead, BufReader, Write};
use test_log::test;

const CHILD_ENV: &str = "FJALL_SYNC_ON_COMMIT_CHILD";
const FOLDER_ENV: &str = "FJALL_SYNC_ON_COMMIT_FOLDER";

/// Writes batches and acknowledges each one on stdout, until it gets killed.
fn run_child() -> fjall::Result<()> {
    let folder = std::env::var(FOLDER_ENV).expect("folder env var should be set");

    // NOTE: `manual_journal_persist` keeps written data in process memory,
    // so only writes covered by the group commit sync can survive the kill
    let db = Database::builder(&folder)
        .manual_journal_persist(true)
        .sync_on_commit(true)
        .open()?;

    let tree = db.keyspace("default", Default::default)?;

    for i in 0_u64..1_000 {
        let mut batch = db.batch();
        batch.insert(&tree, i.to_be_bytes(), i.to_be_bytes());
        batch.commit()?;

        // Acknowledge the write to the caller
        println!("ACK {i}");
        std::io::stdout().flush()?;
    }

    Ok(())
}

#[test]
fn sync_on_commit_survives_sigkill() -> fjall::Result<()> {
    if std::env::var(CHILD_ENV).is_ok() {
        return run_child();
    }

    let folder = tempfile::tempdir()?;

    let mut child = std::process::Command::new(std::env::current_exe()?)
        .args(["sync_on_commit_survives_sigkill", "--exact", "--nocapture"])
        .env(CHILD_ENV, "1")
        .env(FOLDER_ENV, folder.path())
        .stdout(std::process::Stdio::piped())
        .spawn()?;

    // Collect acknowledged writes, then simulate a hard kill (e.g. deployment restart)
    let mut acknowledged = Vec::new();
    let stdout = child.stdout.take().expect("stdout should be piped");
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();

    while acknowledged.len() < 50 {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }

        // NOTE: The test harness writes its own output to stdout as well
        if let Some(seq) = line.trim().strip_prefix("ACK ") {
            acknowledged.push(seq.parse::<u64>().expect("should be a number"));
        }
    }

    child.kill()?;
    child.wait()?;
    drop(reader);

    assert_eq!(
        50,
        acknowledged.len(),
        "child should have acknowledged writes"
    );

    // "Restart" the process and check that no acknowledged write was lost
    let db = Database::builder(folder.path()).open()?;
    let tree = db.keyspace("default", Default::default)?;

    for i in &acknowledged {
        assert!(
            tree.contains_key(i.to_be_bytes())?,
            "acknowledged write {i} was lost after kill",
        );
    }

    Ok(())
}
