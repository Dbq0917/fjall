use std::time::Instant;

use fjall::{Database, KeyspaceCreateOptions};

fn main() -> fjall::Result<()> {
    let memtable_bytes = std::env::args()
        .nth(1)
        .map(|value| value.parse::<u64>().expect("memtable size"))
        .unwrap_or(64 * 1_024 * 1_024);
    let value_bytes = std::env::args()
        .nth(2)
        .map(|value| value.parse::<usize>().expect("value size"))
        .unwrap_or(1_000);

    let folder = tempfile::tempdir()?;
    let db = Database::builder(&folder).open()?;
    let tree = db.keyspace("default", || {
        KeyspaceCreateOptions::default().max_memtable_size(memtable_bytes)
    })?;

    let value = vec![b'x'; value_bytes];
    let mut latencies = Vec::with_capacity(100_000);

    for index in 0u64..100_000 {
        let started = Instant::now();
        tree.insert(index.to_be_bytes(), &value)?;
        latencies.push(started.elapsed());
    }

    latencies.sort_unstable();

    let percentile = |permille: usize| latencies[(latencies.len() - 1) * permille / 1_000];
    let over_timeout = latencies
        .iter()
        .filter(|latency| **latency >= std::time::Duration::from_millis(50))
        .count();

    println!(
        "memtable={memtable_bytes} value={value_bytes} p50={:?} p90={:?} p99={:?} p999={:?} max={:?} over_50ms={over_timeout}",
        percentile(500),
        percentile(900),
        percentile(990),
        percentile(999),
        latencies.last().expect("latencies"),
    );

    Ok(())
}
