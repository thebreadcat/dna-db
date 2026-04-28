//! Durable bulk ingest benchmark: full storage fsync every commit vs deferred fsync + one finalize.
//!
//! Run (from `engine/`):
//!   cargo run --release --bin bench_durable_bulk -- [RECORDS] [BATCHES] [WARMUP]
//!
//! Example: 100k rows in 20 commits (5000 per HTTP bulk-style batch):
//!   cargo run --release --bin bench_durable_bulk -- 100000 20 1

use std::path::PathBuf;
use std::time::Instant;

use dnadb_engine::transaction_durable::DurableTransactionStore;
use serde_json::{json, Value};
use uuid::Uuid;

fn usage() -> ! {
    eprintln!(
        "Usage: bench_durable_bulk [RECORDS] [BATCHES] [WARMUP]\n\
         \n\
         Multiple commits (RECORDS / BATCHES per batch) stress repeated sync_data on the\n\
         durable path. Deferred fsync skips per-batch strand file sync; compare totals.\n\
         \n\
         Defaults: RECORDS=50000 BATCHES=10 WARMUP=0\n\
         Example: cargo run --release --bin bench_durable_bulk -- 100000 20 1"
    );
    std::process::exit(2);
}

fn parse_usize(name: &str, s: &str) -> usize {
    s.parse()
        .unwrap_or_else(|_| {
            eprintln!("invalid {name}: {s}");
            usage();
        })
}

fn build_batch(start_id: u64, count: usize) -> Vec<Value> {
    (0..count)
        .map(|i| {
            let id = start_id + i as u64;
            json!({
                "id": id,
                "slug": format!("post-{id}"),
                "title": format!("Title {id}"),
            })
        })
        .collect()
}

fn run_case(
    root: &PathBuf,
    label: &str,
    total: usize,
    num_batches: usize,
    per_batch: usize,
    defer_fsync: bool,
) -> std::time::Duration {
    let t0 = Instant::now();
    let mut store =
        DurableTransactionStore::open_or_create(root, "bench", 1, Some(16 * 1024 * 1024))
            .expect("open");
    let mut next_id = 1u64;
    for _ in 0..num_batches {
        let batch = build_batch(next_id, per_batch);
        next_id += per_batch as u64;
        store
            .execute_insert_many_with_mode(batch, false, defer_fsync)
            .expect("insert_many");
    }
    if defer_fsync {
        store.sync_storage_to_disk().expect("finalize sync");
    }
    let elapsed = t0.elapsed();
    eprintln!(
        "  {label}: {:.2} ms  (defer_fsync={defer_fsync}, commits={num_batches}, rows={total}, root={})",
        elapsed.as_secs_f64() * 1000.0,
        root.display()
    );
    elapsed
}

fn main() {
    let mut args = std::env::args().skip(1);
    let total = match args.next() {
        None => 50_000usize,
        Some(s) if s == "-h" || s == "--help" => usage(),
        Some(s) => parse_usize("RECORDS", &s),
    };
    let num_batches = args
        .next()
        .map(|s| parse_usize("BATCHES", &s))
        .unwrap_or(10)
        .max(1);
    let warmup = args
        .next()
        .map(|s| parse_usize("WARMUP", &s))
        .unwrap_or(0);

    if total < num_batches {
        eprintln!("RECORDS must be >= BATCHES (need at least 1 row per batch)");
        usage();
    }
    let per_batch = total / num_batches;
    let actual_total = per_batch * num_batches;
    if actual_total != total {
        eprintln!(
            "note: using {} rows ({} * {}) so batches divide evenly; requested total was {total}",
            actual_total, per_batch, num_batches
        );
    }

    println!(
        "bench_durable_bulk: rows={actual_total} commits={num_batches} rows_per_commit={per_batch} warmup_runs={warmup}"
    );
    if std::env::var_os("DNADB_COMMIT_PROFILE").is_some() {
        eprintln!("(DNADB_COMMIT_PROFILE is set — per-commit timings go to stderr)");
    }

    for w in 0..warmup {
        let dir = std::env::temp_dir().join(format!("dnadb_bench_warmup_{}_{}", Uuid::new_v4(), w));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("mkdir");
        run_case(
            &dir,
            "warmup strict",
            actual_total,
            num_batches,
            per_batch,
            false,
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    let dir_strict = std::env::temp_dir().join(format!("dnadb_bench_strict_{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir_strict).expect("mkdir");
    let t_strict = run_case(
        &dir_strict,
        "strict (fsync every commit)",
        actual_total,
        num_batches,
        per_batch,
        false,
    );
    let _ = std::fs::remove_dir_all(&dir_strict);

    let dir_defer = std::env::temp_dir().join(format!("dnadb_bench_defer_{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir_defer).expect("mkdir");
    let t_defer = run_case(
        &dir_defer,
        "defer+finalize (flush_maps per commit, one sync at end)",
        actual_total,
        num_batches,
        per_batch,
        true,
    );
    let _ = std::fs::remove_dir_all(&dir_defer);

    let strict_ms = t_strict.as_secs_f64() * 1000.0;
    let defer_ms = t_defer.as_secs_f64() * 1000.0;
    let ratio = strict_ms / defer_ms;
    println!();
    println!("--- summary ---");
    println!("strict wall:     {strict_ms:.2} ms");
    println!("defer+finalize:  {defer_ms:.2} ms");
    if defer_ms > 0.0 {
        println!("speedup (strict / defer): {ratio:.2}x");
    }
    println!();
    if num_batches < 2 {
        println!(
            "Single commit: strict and defer both do one full storage sync for the batch (defer: flush_maps\n\
             during commit + one `sync_storage_to_disk` at end). Compare with commits>=2 to see repeated `sync_data` savings."
        );
    } else if ratio >= 1.15 {
        println!(
            "Deferred fsync is clearly faster here with {num_batches} commits — the main use case for this option.\n\
             HTTP: if the field is omitted, `defer_storage_fsync` now follows `defer_reindex` (off for one-shot bulks, on for typical CMS seeding)."
        );
    } else if ratio <= 1.05 {
        println!(
            "Difference is within ~5% — `sync_data` is not dominating (e.g. very fast disk or small batch size).\n\
             Per-request strict fsync remains a safe default for one-off bulks."
        );
    } else {
        println!(
            "Moderate win ({ratio:.2}x). Omitted `defer_storage_fsync` follows `defer_reindex` on the HTTP API."
        );
    }
}
