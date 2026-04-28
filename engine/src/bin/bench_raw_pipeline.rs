//! End-to-end **raw journal → materialize → query** benchmark (not the classic `load_bench` path).
//!
//! Run (from `engine/`, release recommended):
//!   cargo run --release --bin bench_raw_pipeline -- --scenario a
//!   cargo run --release --bin bench_raw_pipeline -- --scenario b
//!   cargo run --release --bin bench_raw_pipeline -- --scenario c
//!
//! Scenarios:
//!   **A** — Sustained ingest: ramp checkpoints 100k / 500k / 1M total raw rows, background materializer on;
//!            report ingest + mat rates, lag, time-to-visibility for the row at each checkpoint.
//!   **B** — Burst: 200k fast raw writes, then measure peak lag and time until `pending_wal_sequences == 0`.
//!   **C** — Mixed: one thread ingests, one queries, background materializer; lock-wait vs query-work split, lag, p50/p95/p99.
//!   **D** — Equilibrium sweep: repeat short runs at several `--equilibrium-pauses` (ingest spacing) to see where mean **d(lag)/dt** crosses zero (ingest ≈ materialize).
//!   **E** — **Matrix tracking**: run the same mixed **C** workload at `--matrix-target-rows` (default 100k,200k,500k) with optional `--stop-after-raw-rows` per tier; one JSON with `tiers[]` + `comparison[]` (writes/lag, read `query_work_ms_*`, search `text_search_work_ms_*`, combined latency).
//!
//! Instrumentation (A/B/C/D):
//!   - `materialize_time_series`: each materializer tick → `records_applied`, `mat_records_per_sec`, `pending_wal_sequences_after`
//!   - `lag_derivative` (raw) + optional `lag_derivative_smoothed` (trailing mean of lag′); **`lag_acceleration`** uses smoothed lag′ when `--lag-deriv-smooth-window` > 1
//!   - Burst **`burst_impulse_events`**: recovery uses **baseline-relative band** + **|lag′| < ε sustained** (`--recovery-*`); oscillation counts use **smoothed** lag′; legacy fixed threshold kept as `recovery_legacy_fixed_threshold_wal_ms`
//!   - Scenario C: `lock_wait_ms_*` vs `query_work_ms_*` (contention vs MVCC find work)
//!   - Optional **search stressor** (C/D, bench-only): in-memory **term → posting ids** updated after each raw ingest batch; `--search-enabled` + `--search-qps` runs **K id lookups** per op (no ranking). `--search-selectivity` common|rare|mixed + `--search-rare-doc-mod` / `--search-mixed-rare-query-pct`
//!   - Scenario C: `time_bucketed_traces` — per bucket: raw samples, variances, **k-means regime** on z-scored `[lock, combined, lag′, instability]` (fallback v0 if fewer than 2 nonempty buckets), **split queue** (`queue_pressure_wal_ms` vs `queue_pressure_visibility_proxy_ms`), **`state_vector`**, **lag_acceleration** (global series from lag samples)
//!   - Scenario C optional **impulse**: `--burst-period-sec` + `--burst-size` + `--burst-inject-ms` → `burst_impulse_events` (overshoot + half-life + settling + damping proxy)
//!   - Scenario D: `phase_diagram` uses **k-means** on step features when ≥2 points (`regime_calibration`); each step includes **`state_vector`**, **`lag_acceleration`**, split queue pressures, `system_regime_v0` vs `system_regime`
//!   - Scenario C temporal coherence: `regime_transition_counts`, `regime_transition_sequence`, `sticky_transition_ratio`, dwell counts
//!   - Scenario A: `visibility_under_load_ms_p50/p95/p99` over checkpoint probes
//!
//! **Coupling note:** faster `--lag-sample-ms` or heavy visibility polling increases lock contention and can skew results — treat sample intervals as part of the experiment.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use dnadb_engine::runtime::EngineRuntime;
use dnadb_engine::transaction_durable::ExecutionResult;
use dnadb_engine::wire::{MongoCommand, MongoFindCommand};
use dnadb_engine::write_pipeline::LsmWritePipeline;
use serde::Serialize;
use serde_json::{json, Map, Value};

const COLL: &str = "raw_bench";
const RAW_JOURNAL_INNER: &str = "raw";
/// Default checkpoints: 100k → 500k → 1M total raw rows (same ratios as `--phase-max-rows 1000000`).
const MILESTONES: [u64; 3] = [100_000, 500_000, 1_000_000];

fn milestone_table(phase_max: Option<u64>) -> Vec<u64> {
    match phase_max {
        Some(t) if t >= 10 => {
            let a = (t / 10).max(1);
            let b = (t / 2).max(a + 1);
            let c = t.max(b + 1);
            vec![a, b, c]
        }
        _ => MILESTONES.to_vec(),
    }
}

// `load_bench` is a second target; this is the raw → mat → query pipeline.

fn main() {
    if let Err(e) = run() {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let _ = unsafe { std::env::remove_var("DNADB_RAW_MAX_PENDING_WAL") };
    let _ = unsafe { std::env::remove_var("DNADB_RAW_CRITICAL_PENDING") };

    let args = parse_args()?;
    let _ = std::fs::remove_dir_all(&args.data_dir);
    std::fs::create_dir_all(&args.data_dir).map_err(|e| e.to_string())?;

    let out = match args.scenario {
        Scenario::A => scenario_a(&args)?,
        Scenario::B => scenario_b(&args)?,
        Scenario::C => scenario_c(&args)?,
        Scenario::D => scenario_d(&args)?,
        Scenario::E => scenario_e(&args)?,
    };
    println!("{}", serde_json::to_string_pretty(&out).map_err(|e| e.to_string())?);
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scenario {
    A,
    B,
    C,
    D,
    /// Multi-tier matrix: repeated scenario-C-style runs at configured raw row caps.
    E,
}

/// How search ops pick the term whose posting list is scanned (bench-only inverted index).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SearchSelectivity {
    /// Postings for `common` only; queries always `common`.
    Common,
    /// Sparse `rare` postings; queries always `rare`.
    Rare,
    /// Both posting lists; queries pick `rare` at `--search-mixed-rare-query-pct` rate.
    Mixed,
}

#[derive(Clone)]
struct Args {
    scenario: Scenario,
    data_dir: std::path::PathBuf,
    mmap_bytes: Option<usize>,
    /// Background materializer poll interval (active backlog path).
    mat_interval_ms: u64,
    mat_idle_ms: u64,
    /// Background materializer decode/apply batch per lock hold (`0` = adaptive/unbounded call).
    materializer_batch: usize,
    /// Max materialize decode/apply rounds per scheduler tick (bounded API call budget).
    materializer_max_batches_per_tick: usize,
    ingest_batch: usize,
    /// Scenario B burst row count.
    burst_records: u64,
    /// Scenario C duration.
    duration_sec: u64,
    ingest_pause_ms: u64,
    query_interval_ms: u64,
    /// If set (scenario A), checkpoints are at `max/10`, `max/2`, and `max` rows (defaults: 100k/500k/1M).
    phase_max_rows: Option<u64>,
    /// Wall ms between lag samples (A; lower = denser derivative, more mutex traffic).
    lag_sample_ms: u64,
    /// Scenario D: wall time per ingest-pause step.
    equilibrium_step_sec: u64,
    /// Scenario D: ingest pause values (ms) to sweep, comma-separated.
    equilibrium_pauses: Vec<u64>,
    /// Scenario C: wall-clock bucket width for `time_bucketed_traces` (0 = omit).
    trace_bucket_ms: u64,
    /// Scenario C: wall seconds between ingest impulse bursts (0 = off).
    burst_period_sec: u64,
    /// Scenario C: raw rows per impulse burst (best-effort within `burst_inject_ms`).
    burst_size: u64,
    /// Scenario C: max wall time to apply one burst (rapid inserts).
    burst_inject_ms: u64,
    /// Trailing window size for smoothing lag′ (1 = off). Used for lag″, burst oscillation, recovery |lag′|.
    lag_deriv_smooth_window: usize,
    /// Baseline-relative recovery: band half-width = max(`recovery_abs_floor_wal`, baseline * `recovery_rel_frac`).
    recovery_rel_frac: f64,
    /// Minimum WAL sequences above baseline allowed in the recovered band (when baseline is tiny).
    recovery_abs_floor_wal: f64,
    /// Sustained recovery: require |smoothed lag′| < this (WAL seq/s) over the hold window.
    recovery_eps_wal_per_s: f64,
    /// Wall ms over which lag must stay in band and |lag′| < ε (recovery + settling).
    recovery_stable_hold_ms: f64,
    /// C/D: enable bench-only search stressor (posting list + id finds).
    search_enabled: bool,
    /// Target search operations per wall second (period = 1000/qps).
    search_qps: f64,
    /// Point lookups per search op (posting tail scan).
    search_scan: usize,
    /// Doc tagging: one rare row every N ids (`common` all others). `u64::MAX` = no rare docs.
    search_rare_doc_mod: u64,
    search_selectivity: SearchSelectivity,
    /// In `Mixed`, fraction of searches targeting `rare` (0–100).
    search_mixed_rare_query_pct: u8,
    /// Scenario C/E: stop raw ingest after this many rows are durable-append complete (`next_id-1 >= N`). Main loop still runs full `duration_sec` for reads/search.
    stop_after_raw_rows: Option<u64>,
    /// Scenario E: comma-separated raw row caps (default 100000,200000,500000 if empty).
    matrix_target_rows: Vec<u64>,
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        scenario: Scenario::A,
        data_dir: std::path::PathBuf::from("/tmp/dnadb_bench_raw_pipeline"),
        mmap_bytes: Some(32 * 1024 * 1024),
        mat_interval_ms: 10,
        mat_idle_ms: 2_000,
        materializer_batch: 512,
        materializer_max_batches_per_tick: 4,
        ingest_batch: 5_000,
        burst_records: 200_000,
        duration_sec: 30,
        ingest_pause_ms: 2,
        query_interval_ms: 2,
        phase_max_rows: None,
        lag_sample_ms: 100,
        equilibrium_step_sec: 15,
        equilibrium_pauses: vec![0, 1, 2, 5, 10, 20],
        trace_bucket_ms: 500,
        burst_period_sec: 0,
        burst_size: 100_000,
        burst_inject_ms: 200,
        lag_deriv_smooth_window: 4,
        recovery_rel_frac: 0.12,
        recovery_abs_floor_wal: 2000.0,
        recovery_eps_wal_per_s: 2500.0,
        recovery_stable_hold_ms: 400.0,
        search_enabled: false,
        search_qps: 5.0,
        search_scan: 20,
        search_rare_doc_mod: 2000,
        search_selectivity: SearchSelectivity::Mixed,
        search_mixed_rare_query_pct: 12,
        stop_after_raw_rows: None,
        matrix_target_rows: Vec::new(),
    };
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--scenario" | "-s" => {
                let v = it
                    .next()
                    .ok_or_else(|| "--scenario requires a|b|c".to_string())?;
                args.scenario = match v.as_str() {
                    "a" | "A" => Scenario::A,
                    "b" | "B" => Scenario::B,
                    "c" | "C" => Scenario::C,
                    "d" | "D" => Scenario::D,
                    "e" | "E" => Scenario::E,
                    _ => return Err("use --scenario a|b|c|d|e".to_string()),
                };
            }
            "--data-dir" => {
                args.data_dir = std::path::PathBuf::from(
                    it.next()
                        .ok_or_else(|| "--data-dir needs a path".to_string())?,
                );
            }
            "--mat-interval-ms" => {
                let n: u64 = it
                    .next()
                    .ok_or_else(|| "value".to_string())?
                    .parse()
                    .map_err(|e| format!("mat-interval-ms: {e}"))?;
                args.mat_interval_ms = n.max(1);
            }
            "--mat-idle-ms" => {
                let n: u64 = it
                    .next()
                    .ok_or_else(|| "mat-idle-ms needs a number".to_string())?
                    .parse()
                    .map_err(|e| format!("mat-idle-ms: {e}"))?;
                args.mat_idle_ms = n.max(1);
            }
            "--materializer-batch" => {
                let n: usize = it
                    .next()
                    .ok_or_else(|| "--materializer-batch needs a value".to_string())?
                    .parse()
                    .map_err(|e| format!("materializer-batch: {e}"))?;
                args.materializer_batch = n;
            }
            "--materializer-max-batches-per-tick" => {
                let n: usize = it
                    .next()
                    .ok_or_else(|| "--materializer-max-batches-per-tick needs a value".to_string())?
                    .parse()
                    .map_err(|e| format!("materializer-max-batches-per-tick: {e}"))?;
                args.materializer_max_batches_per_tick = n.max(1);
            }
            "--ingest-batch" => {
                let n: usize = it
                    .next()
                    .ok_or_else(|| "ingest-batch needs a number".to_string())?
                    .parse()
                    .map_err(|e| format!("ingest-batch: {e}"))?;
                args.ingest_batch = n.max(1);
            }
            "--burst" => {
                let n: u64 = it
                    .next()
                    .ok_or_else(|| "--burst needs a number".to_string())?
                    .parse()
                    .map_err(|e| format!("burst: {e}"))?;
                args.burst_records = n.max(1);
            }
            "--duration-sec" => {
                let n: u64 = it
                    .next()
                    .ok_or_else(|| "duration-sec needs a value".to_string())?
                    .parse()
                    .map_err(|e| format!("duration-sec: {e}"))?;
                args.duration_sec = n.max(1);
            }
            "--ingest-pause-ms" => {
                let n: u64 = it
                    .next()
                    .ok_or_else(|| "ingest-pause-ms needs a value".to_string())?
                    .parse()
                    .map_err(|e| format!("ingest-pause-ms: {e}"))?;
                args.ingest_pause_ms = n;
            }
            "--query-interval-ms" => {
                let n: u64 = it
                    .next()
                    .ok_or_else(|| "query-interval-ms needs a value".to_string())?
                    .parse()
                    .map_err(|e| format!("query-interval-ms: {e}"))?;
                args.query_interval_ms = n.max(1);
            }
            "--phase-max-rows" => {
                let n: u64 = it
                    .next()
                    .ok_or_else(|| "--phase-max-rows needs a value".to_string())?
                    .parse()
                    .map_err(|e| format!("phase-max-rows: {e}"))?;
                args.phase_max_rows = Some(n.max(10));
            }
            "--lag-sample-ms" => {
                let n: u64 = it
                    .next()
                    .ok_or_else(|| "--lag-sample-ms needs a value".to_string())?
                    .parse()
                    .map_err(|e| format!("lag-sample-ms: {e}"))?;
                args.lag_sample_ms = n.max(5);
            }
            "--equilibrium-step-sec" => {
                let n: u64 = it
                    .next()
                    .ok_or_else(|| "--equilibrium-step-sec needs a value".to_string())?
                    .parse()
                    .map_err(|e| format!("equilibrium-step-sec: {e}"))?;
                args.equilibrium_step_sec = n.max(3);
            }
            "--equilibrium-pauses" => {
                let s = it
                    .next()
                    .ok_or_else(|| "--equilibrium-pauses needs comma-separated ms values".to_string())?;
                let mut v = Vec::new();
                for part in s.split(',') {
                    let p = part.trim();
                    if p.is_empty() {
                        continue;
                    }
                    let n: u64 = p
                        .parse()
                        .map_err(|e| format!("equilibrium-pauses: {e}"))?;
                    v.push(n);
                }
                if !v.is_empty() {
                    args.equilibrium_pauses = v;
                }
            }
            "--trace-bucket-ms" => {
                let n: u64 = it
                    .next()
                    .ok_or_else(|| "--trace-bucket-ms needs a value (0 disables)".to_string())?
                    .parse()
                    .map_err(|e| format!("trace-bucket-ms: {e}"))?;
                args.trace_bucket_ms = n;
            }
            "--burst-period-sec" => {
                let n: u64 = it
                    .next()
                    .ok_or_else(|| "--burst-period-sec needs a value (0 disables)".to_string())?
                    .parse()
                    .map_err(|e| format!("burst-period-sec: {e}"))?;
                args.burst_period_sec = n;
            }
            "--burst-size" => {
                let n: u64 = it
                    .next()
                    .ok_or_else(|| "--burst-size needs row count".to_string())?
                    .parse()
                    .map_err(|e| format!("burst-size: {e}"))?;
                args.burst_size = n.max(1);
            }
            "--burst-inject-ms" => {
                let n: u64 = it
                    .next()
                    .ok_or_else(|| "--burst-inject-ms needs milliseconds".to_string())?
                    .parse()
                    .map_err(|e| format!("burst-inject-ms: {e}"))?;
                args.burst_inject_ms = n.max(1);
            }
            "--search-enabled" | "--search-probe" => {
                args.search_enabled = true;
            }
            "--search-qps" => {
                let x: f64 = it
                    .next()
                    .ok_or_else(|| "--search-qps needs a number".to_string())?
                    .parse()
                    .map_err(|e| format!("search-qps: {e}"))?;
                args.search_qps = x.max(1e-6);
            }
            "--search-scan" => {
                let n: usize = it
                    .next()
                    .ok_or_else(|| "--search-scan needs K (point lookups per op)".to_string())?
                    .parse()
                    .map_err(|e| format!("search-scan: {e}"))?;
                args.search_scan = n.max(1);
            }
            "--search-selectivity" => {
                let s = it
                    .next()
                    .ok_or_else(|| "--search-selectivity needs common|rare|mixed".to_string())?;
                args.search_selectivity = match s.to_ascii_lowercase().as_str() {
                    "common" => SearchSelectivity::Common,
                    "rare" => SearchSelectivity::Rare,
                    "mixed" => SearchSelectivity::Mixed,
                    _ => return Err("use --search-selectivity common|rare|mixed".to_string()),
                };
            }
            "--search-rare-doc-mod" => {
                let n: u64 = it
                    .next()
                    .ok_or_else(|| "--search-rare-doc-mod needs N>=2 (1 rare row every N ids)".to_string())?
                    .parse()
                    .map_err(|e| format!("search-rare-doc-mod: {e}"))?;
                args.search_rare_doc_mod = n.max(2);
            }
            "--search-mixed-rare-query-pct" => {
                let n: u8 = it
                    .next()
                    .ok_or_else(|| "--search-mixed-rare-query-pct needs 0-100".to_string())?
                    .parse()
                    .map_err(|e| format!("search-mixed-rare-query-pct: {e}"))?;
                args.search_mixed_rare_query_pct = n.min(100);
            }
            "--lag-deriv-smooth-window" => {
                let n: usize = it
                    .next()
                    .ok_or_else(|| "--lag-deriv-smooth-window needs N>=1".to_string())?
                    .parse()
                    .map_err(|e| format!("lag-deriv-smooth-window: {e}"))?;
                args.lag_deriv_smooth_window = n.max(1);
            }
            "--recovery-rel-frac" => {
                let x: f64 = it
                    .next()
                    .ok_or_else(|| "--recovery-rel-frac needs a fraction".to_string())?
                    .parse()
                    .map_err(|e| format!("recovery-rel-frac: {e}"))?;
                args.recovery_rel_frac = x.max(0.0);
            }
            "--recovery-abs-floor-wal" => {
                let x: f64 = it
                    .next()
                    .ok_or_else(|| "--recovery-abs-floor-wal needs WAL sequences".to_string())?
                    .parse()
                    .map_err(|e| format!("recovery-abs-floor-wal: {e}"))?;
                args.recovery_abs_floor_wal = x.max(0.0);
            }
            "--recovery-eps-wal-per-s" => {
                let x: f64 = it
                    .next()
                    .ok_or_else(|| "--recovery-eps-wal-per-s needs a rate".to_string())?
                    .parse()
                    .map_err(|e| format!("recovery-eps-wal-per-s: {e}"))?;
                args.recovery_eps_wal_per_s = x.max(0.0);
            }
            "--recovery-stable-hold-ms" => {
                let x: f64 = it
                    .next()
                    .ok_or_else(|| "--recovery-stable-hold-ms needs milliseconds".to_string())?
                    .parse()
                    .map_err(|e| format!("recovery-stable-hold-ms: {e}"))?;
                args.recovery_stable_hold_ms = x.max(1.0);
            }
            "--stop-after-raw-rows" => {
                let n: u64 = it
                    .next()
                    .ok_or_else(|| "--stop-after-raw-rows needs N".to_string())?
                    .parse()
                    .map_err(|e| format!("stop-after-raw-rows: {e}"))?;
                args.stop_after_raw_rows = Some(n.max(1));
            }
            "--matrix-target-rows" => {
                let s = it
                    .next()
                    .ok_or_else(|| "--matrix-target-rows needs comma-separated row counts".to_string())?;
                let mut v = Vec::new();
                for part in s.split(',') {
                    let p = part.trim();
                    if p.is_empty() {
                        continue;
                    }
                    let n: u64 = p
                        .parse()
                        .map_err(|e| format!("matrix-target-rows: {e}"))?;
                    v.push(n.max(1));
                }
                if !v.is_empty() {
                    args.matrix_target_rows = v;
                }
            }
            "-h" | "--help" => {
                eprintln!(
                    "Usage: bench_raw_pipeline --scenario a|b|c|d|e [options]\n\
                     \n\
                     --data-dir PATH        (default /tmp/dnadb_bench_raw_pipeline)\n\
                     --mat-interval-ms N   (default 10) background materialize when lag>0\n\
                     --mat-idle-ms N       (default 2000) sleep when no lag (A/C/D)\n\
                     --materializer-batch N materialize decode/apply batch per lock hold (default 512; 0 = adaptive)\n\
                     --materializer-max-batches-per-tick N bounded materialize rounds per scheduler tick (default 4)\n\
                     --ingest-batch N      (default 5000)\n\
                     --burst N             scenario B total raw rows (default 200000)\n\
                     --duration-sec N      scenario C (default 30)\n\
                     --ingest-pause-ms N   scenario C pause between batches (default 2)\n\
                     --query-interval-ms N scenario C (default 2)\n\
                     --phase-max-rows N   scenario A: scale checkpoints to max/10, max/2, max (smoke); default 1e6\n\
                     --lag-sample-ms N    scenario A lag + derivative sample period (default 100)\n\
                     --equilibrium-step-sec N  scenario D seconds per pause step (default 15)\n\
                     --equilibrium-pauses MS,... scenario D e.g. 0,1,2,5,10,20\n\
                     --trace-bucket-ms N   scenario C: bucket width for time_bucketed_traces (default 500; 0 = omit)\n\
                     --burst-period-sec N scenario C: periodic ingest impulse (0=off)\n\
                     --burst-size N        scenario C: rows per impulse\n\
                     --burst-inject-ms N   scenario C: wall ms cap for applying one burst\n\
                     --lag-deriv-smooth-window N trailing mean of lag′ for lag″ / burst oscillation (default 4; 1=off)\n\
                     --recovery-rel-frac X baseline-relative band (default 0.12)\n\
                     --recovery-abs-floor-wal X min band when baseline≈0 (default 2000)\n\
                     --recovery-eps-wal-per-s X sustained |lag′| bound (default 2500)\n\
                     --recovery-stable-hold-ms X sustained window (default 400)\n\
                     --search-enabled      C/D: bench posting index + id lookup scan\n\
                     --search-probe         alias for --search-enabled\n\
                     --search-qps N         search ops per wall second (default 5)\n\
                     --search-scan K        id finds per op (default 20)\n\
                     --search-selectivity common|rare|mixed (default mixed)\n\
                     --search-rare-doc-mod N one rare doc every N ids (default 2000)\n\
                     --search-mixed-rare-query-pct P in mixed mode (default 12)\n\
                     --stop-after-raw-rows N  C/E: stop ingest after N raw rows (main loop still runs duration_sec)\n\
                     --matrix-target-rows N,... scenario E caps (default 100000,200000,500000)\n"
                );
                std::process::exit(0);
            }
            _ => return Err(format!("unknown arg: {a} (try --help)")),
        }
    }
    Ok(args)
}

fn make_row(id: u64, rare_doc_mod: u64) -> Value {
    let is_rare = rare_doc_mod < u64::MAX && rare_doc_mod >= 2 && id % rare_doc_mod == 0;
    json!({
        "id": id,
        "slug": format!("r{id}"),
        "title": format!("title {id}"),
        "body": format!("benchtok_{id} lorem ipsum dolor sit amet raw bench row"),
        "term": if is_rare { "rare" } else { "common" },
    })
}

fn make_batch(start: u64, n: usize, rare_doc_mod: u64) -> Vec<Value> {
    (0..n)
        .map(|i| make_row(start + i as u64, rare_doc_mod))
        .collect()
}

fn rare_doc_mod_from_args(args: &Args) -> u64 {
    match args.search_selectivity {
        SearchSelectivity::Common => u64::MAX,
        SearchSelectivity::Rare | SearchSelectivity::Mixed => args.search_rare_doc_mod.max(2),
    }
}

type PostingsMap = HashMap<String, VecDeque<u64>>;

fn push_posting_cap(q: &mut VecDeque<u64>, id: u64, cap: usize) {
    if q.len() >= cap {
        q.pop_front();
    }
    q.push_back(id);
}

fn register_row_range(postings: &Mutex<PostingsMap>, start: u64, n: usize, rare_doc_mod: u64) {
    let mut pm = postings.lock().expect("postings lock");
    const CAP: usize = 500_000;
    for i in 0..n {
        let id = start + i as u64;
        let qc = pm.entry("common".to_string()).or_insert_with(VecDeque::new);
        push_posting_cap(qc, id, CAP);
        if rare_doc_mod < u64::MAX && rare_doc_mod >= 2 && id % rare_doc_mod == 0 {
            let qr = pm.entry("rare".to_string()).or_insert_with(VecDeque::new);
            push_posting_cap(qr, id, CAP);
        }
    }
}

fn pick_search_term(op_idx: u64, args: &Args) -> &'static str {
    match args.search_selectivity {
        SearchSelectivity::Common => "common",
        SearchSelectivity::Rare => "rare",
        SearchSelectivity::Mixed => {
            let p = args.search_mixed_rare_query_pct.min(100) as u64;
            let h = (op_idx.wrapping_mul(1_100_351_524_5)) % 100;
            if h < p {
                "rare"
            } else {
                "common"
            }
        }
    }
}

fn search_posting_scan_ms(rt: &mut EngineRuntime, ids: &[u64]) -> Result<f64, String> {
    let t0 = Instant::now();
    for &id in ids {
        let mut m = Map::new();
        m.insert("id".to_string(), json!(id));
        rt.execute_mongo_command(MongoCommand::Find(MongoFindCommand {
            collection: COLL.to_string(),
            filter: m,
            sort: None,
            limit: Some(1),
            include_paths: vec![],
        }))
        .map_err(|e| e.to_string())?;
    }
    Ok(t0.elapsed().as_secs_f64() * 1000.0)
}

fn row_visible(rt: &mut EngineRuntime, id: u64) -> bool {
    let mut m = Map::new();
    m.insert("id".to_string(), json!(id));
    match rt.execute_mongo_command(MongoCommand::Find(MongoFindCommand {
        collection: COLL.to_string(),
        filter: m,
        sort: None,
        limit: Some(1),
        include_paths: vec![],
    })) {
        Ok(ExecutionResult::QueryRows(r)) => !r.is_empty(),
        _ => false,
    }
}

/// Returns (time to first query hit, ms) with 1ms sleep between tries, 60s cap.
fn time_to_visibility(
    rt: &Mutex<EngineRuntime>,
    probe_id: u64,
) -> Result<(bool, f64), String> {
    let t0 = Instant::now();
    let cap = Duration::from_secs(60);
    while t0.elapsed() < cap {
        {
            let mut g = rt.lock().map_err(|e| e.to_string())?;
            if row_visible(&mut g, probe_id) {
                return Ok((true, t0.elapsed().as_secs_f64() * 1000.0));
            }
        }
        thread::sleep(Duration::from_millis(1));
    }
    Ok((false, cap.as_secs_f64() * 1000.0))
}

/// One row per materializer wake: throughput of the raw→durable bridge for this process.
#[derive(Serialize, Clone)]
struct MatEvent {
    t_ms: f64,
    records_applied: u64,
    mat_records_per_sec: Option<f64>,
    duration_ms: Option<f64>,
    pending_wal_sequences_after: u64,
}

#[derive(Serialize, Clone)]
struct LagDerivSample {
    t_ms: f64,
    /// Positive = backlog growing (wal sequences/sec); negative = draining.
    d_lag_d_t_wal_sequences_per_sec: f64,
}

fn lag_derivative_series(samples: &[SustainedSample]) -> Vec<LagDerivSample> {
    samples
        .windows(2)
        .filter_map(|w| {
            let dt_s = (w[1].t_ms - w[0].t_ms) / 1000.0;
            if dt_s < 1e-9 {
                return None;
            }
            let dl = w[1].pending_wal_sequences as f64 - w[0].pending_wal_sequences as f64;
            Some(LagDerivSample {
                t_ms: w[1].t_ms,
                d_lag_d_t_wal_sequences_per_sec: dl / dt_s,
            })
        })
        .collect()
}

#[derive(Serialize, Clone)]
struct LagAccelSample {
    t_ms: f64,
    /// d²(pending_wal) / dt² in WAL sequences per second² (from successive velocity samples).
    d2_lag_dt2_wal_per_s2: f64,
}

fn lag_acceleration_series(deriv: &[LagDerivSample]) -> Vec<LagAccelSample> {
    deriv
        .windows(2)
        .filter_map(|w| {
            let dt_s = (w[1].t_ms - w[0].t_ms) / 1000.0;
            if dt_s < 1e-9 {
                return None;
            }
            let dv = w[1].d_lag_d_t_wal_sequences_per_sec - w[0].d_lag_d_t_wal_sequences_per_sec;
            Some(LagAccelSample {
                t_ms: w[1].t_ms,
                d2_lag_dt2_wal_per_s2: dv / dt_s,
            })
        })
        .collect()
}

/// Causal trailing mean of lag′ (same timestamps as raw derivative points).
fn smooth_lag_deriv_trailing(deriv: &[LagDerivSample], window: usize) -> Vec<LagDerivSample> {
    if window <= 1 || deriv.is_empty() {
        return deriv.to_vec();
    }
    let w = window.max(2);
    deriv
        .iter()
        .enumerate()
        .map(|(i, s)| {
            let lo = i.saturating_sub(w - 1);
            let vals: Vec<f64> = deriv[lo..=i]
                .iter()
                .map(|x| x.d_lag_d_t_wal_sequences_per_sec)
                .collect();
            LagDerivSample {
                t_ms: s.t_ms,
                d_lag_d_t_wal_sequences_per_sec: mean_slice(&vals),
            }
        })
        .collect()
}

fn lag_prime_pairs_from_track(track: &[&SustainedSample]) -> Vec<(f64, f64)> {
    track
        .windows(2)
        .filter_map(|w| {
            let dt_s = (w[1].t_ms - w[0].t_ms) / 1000.0;
            if dt_s < 1e-9 {
                return None;
            }
            let dv =
                w[1].pending_wal_sequences as f64 - w[0].pending_wal_sequences as f64;
            Some((w[1].t_ms, dv / dt_s))
        })
        .collect()
}

fn smooth_trailing_f64(vals: &[f64], window: usize) -> Vec<f64> {
    if window <= 1 || vals.is_empty() {
        return vals.to_vec();
    }
    let w = window.max(2);
    (0..vals.len())
        .map(|i| {
            let lo = i.saturating_sub(w - 1);
            mean_slice(&vals[lo..=i])
        })
        .collect()
}

/// First wall time after `t_burst_ms` where lag stays ≤ `ceiling` and |smoothed lag′| < ε on every segment fully inside `[t, t+hold_ms]`.
fn sustained_baseline_stable_from(
    post_lag: &[&SustainedSample],
    t_burst_ms: f64,
    baseline: u64,
    rel_frac: f64,
    abs_floor: f64,
    eps: f64,
    hold_ms: f64,
    smooth_window: usize,
) -> Option<f64> {
    if post_lag.is_empty() {
        return None;
    }
    let ceiling = baseline as f64 + (baseline as f64 * rel_frac).max(abs_floor);
    let pairs = lag_prime_pairs_from_track(post_lag);
    if pairs.is_empty() {
        return None;
    }
    let raw_v: Vec<f64> = pairs.iter().map(|(_, v)| *v).collect();
    let v_s = smooth_trailing_f64(&raw_v, smooth_window);
    let t_last = post_lag.last()?.t_ms;
    for i in 0..post_lag.len() {
        let t_a = post_lag[i].t_ms;
        if t_a < t_burst_ms {
            continue;
        }
        let t_b = t_a + hold_ms;
        if t_last + 1e-6 < t_b {
            continue;
        }
        let mut ok = true;
        for k in 0..post_lag.len() {
            let tk = post_lag[k].t_ms;
            if tk < t_a || tk > t_b {
                continue;
            }
            if post_lag[k].pending_wal_sequences as f64 > ceiling {
                ok = false;
                break;
            }
            if k >= 1 {
                let vi = k - 1;
                if vi < v_s.len() && v_s[vi].abs() >= eps {
                    ok = false;
                    break;
                }
            }
        }
        if ok {
            return Some(t_a - t_burst_ms);
        }
    }
    None
}

/// Z-score each column; `sigma[d]` is at least `1e-9`.
fn column_z_params(rows: &[[f64; 4]]) -> ([f64; 4], [f64; 4]) {
    let n = rows.len();
    let mut mu = [0.0_f64; 4];
    let mut sig = [1.0_f64; 4];
    for d in 0..4 {
        let col: Vec<f64> = (0..n).map(|i| rows[i][d]).collect();
        mu[d] = mean_slice(&col);
        let sd = population_variance(&col).sqrt().max(1e-9);
        sig[d] = sd;
    }
    (mu, sig)
}

fn to_z_row(row: &[f64; 4], mu: &[f64; 4], sig: &[f64; 4]) -> [f64; 4] {
    [
        (row[0] - mu[0]) / sig[0],
        (row[1] - mu[1]) / sig[1],
        (row[2] - mu[2]) / sig[2],
        (row[3] - mu[3]) / sig[3],
    ]
}

fn dist2(a: &[f64; 4], b: &[f64; 4]) -> f64 {
    (0..4).map(|d| (a[d] - b[d]).powi(2)).sum()
}

/// Lloyd k-means in z-space. `k` in [2, n]. Deterministic init: evenly spaced seeds.
fn kmeans_z_lloyd(rows_z: &[[f64; 4]], k: usize, max_iter: usize) -> (Vec<[f64; 4]>, Vec<usize>) {
    let n = rows_z.len();
    debug_assert!(k >= 2 && k <= n);
    let mut centroids: Vec<[f64; 4]> = (0..k)
        .map(|j| {
            let idx = j * (n - 1) / (k - 1).max(1);
            rows_z[idx]
        })
        .collect();
    let mut labels = vec![0usize; n];
    for _ in 0..max_iter {
        let mut changed = false;
        for i in 0..n {
            let mut best = 0usize;
            let mut best_d = f64::INFINITY;
            for c in 0..k {
                let d = dist2(&rows_z[i], &centroids[c]);
                if d < best_d {
                    best_d = d;
                    best = c;
                }
            }
            if labels[i] != best {
                labels[i] = best;
                changed = true;
            }
        }
        let mut counts = vec![0usize; k];
        let mut sums = vec![[0.0_f64; 4]; k];
        for i in 0..n {
            let c = labels[i];
            counts[c] += 1;
            for d in 0..4 {
                sums[c][d] += rows_z[i][d];
            }
        }
        for c in 0..k {
            if counts[c] > 0 {
                for d in 0..4 {
                    centroids[c][d] = sums[c][d] / counts[c] as f64;
                }
            }
        }
        if !changed {
            break;
        }
    }
    (centroids, labels)
}

/// Map cluster ids 0..k-1 to regime names using **original-space** severity score per cluster.
fn regime_labels_from_clusters(
    k: usize,
    labels: &[usize],
    orig: &[[f64; 4]],
    n: usize,
) -> (Vec<String>, Vec<f64>) {
    const NAMES: [&str; 4] = ["collapsing", "saturated", "backpressured", "stable"];
    let mut score = vec![0.0_f64; k];
    let mut count = vec![0usize; k];
    for i in 0..n {
        let c = labels[i];
        let o = &orig[i];
        // o = [lock, combined, lag_vel, inst_max]
        score[c] += o[2] + o[3] * 50_000.0 + o[0] * 0.01;
        count[c] += 1;
    }
    for c in 0..k {
        if count[c] > 0 {
            score[c] /= count[c] as f64;
        }
    }
    let mut order: Vec<usize> = (0..k).collect();
    order.sort_by(|&a, &b| score[b].partial_cmp(&score[a]).unwrap());
    let mut cluster_to_regime: Vec<String> = (0..k).map(|_| "stable".to_string()).collect();
    for (rank, &cid) in order.iter().enumerate() {
        cluster_to_regime[cid] = NAMES[rank.min(3)].to_string();
    }
    (cluster_to_regime, score)
}

#[derive(Serialize)]
struct RegimeCalibrationMeta {
    method: &'static str,
    k: usize,
    feature_names: [&'static str; 4],
    /// Centroids in z-scored feature space (same order as `feature_names`).
    centroids_z: Vec<Vec<f64>>,
    cluster_to_regime: Vec<String>,
    cluster_severity_score: Vec<f64>,
}

fn percentile_of(mut v: Vec<f64>, q: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let i = ((v.len() as f64 - 1.0) * q).round() as usize;
    v[i.min(v.len() - 1)]
}

fn mean_slice(v: &[f64]) -> f64 {
    if v.is_empty() {
        0.0
    } else {
        v.iter().sum::<f64>() / v.len() as f64
    }
}

fn phase_split_pair<T: Copy>(
    xs: &[T],
    ts_ms: &[f64],
    ingest_stop_ms: Option<f64>,
) -> (Vec<T>, Vec<T>) {
    let mut ingest = Vec::new();
    let mut drain = Vec::new();
    for (i, &x) in xs.iter().enumerate() {
        let t = ts_ms.get(i).copied().unwrap_or(0.0);
        if let Some(stop_ms) = ingest_stop_ms {
            if t <= stop_ms {
                ingest.push(x);
            } else {
                drain.push(x);
            }
        } else {
            ingest.push(x);
        }
    }
    (ingest, drain)
}

/// Population variance (divide by N); 0 for len < 2.
fn population_variance(xs: &[f64]) -> f64 {
    let n = xs.len();
    if n < 2 {
        return 0.0;
    }
    let m = mean_slice(xs);
    xs.iter().map(|x| (x - m).powi(2)).sum::<f64>() / n as f64
}

fn instability_index(variance: f64, mean_abs: f64) -> f64 {
    let d = mean_abs.max(1e-6);
    variance / d
}

/// Heuristic v0: same labels for scenario C buckets and D sweep points (interpretable, not formal SLO).
fn classify_regime_d(
    mean_d_lag_wal_per_s: f64,
    max_lag: u64,
    final_lag: u64,
    _mat_mean_rps: f64,
) -> &'static str {
    let ml = max_lag as f64;
    let fl = final_lag as f64;
    if mean_d_lag_wal_per_s > 55_000.0 || (mean_d_lag_wal_per_s > 22_000.0 && ml > 100_000.0) {
        return "collapsing";
    }
    if fl > 35_000.0 && mean_d_lag_wal_per_s > 6_000.0 {
        return "saturated";
    }
    if mean_d_lag_wal_per_s > 2_000.0 && fl > 12_000.0 {
        return "backpressured";
    }
    if mean_d_lag_wal_per_s.abs() < 10_000.0 && fl < 18_000.0 {
        return "stable";
    }
    if mean_d_lag_wal_per_s > 0.0 {
        "backpressured"
    } else {
        "stable"
    }
}

/// Scenario C time-bucket regime from lock/work/lag dynamics (v0 thresholds; tune with real runs).
fn classify_regime_c(
    lock_mean: f64,
    combined_mean: f64,
    query_work_mean: f64,
    probe_rate: f64,
    lag_slope_wal_per_s: f64,
    inst_max: f64,
    prev_lock_mean: Option<f64>,
    lock_p75: f64,
    lock_med: f64,
    work_p75: f64,
    combined_p75: f64,
) -> &'static str {
    const SLOPE_COLLAPSE: f64 = 42_000.0;
    const SLOPE_STABLE: f64 = 18_000.0;
    if lag_slope_wal_per_s > SLOPE_COLLAPSE
        || (inst_max > 14.0 && lag_slope_wal_per_s > 10_000.0)
        || (inst_max > 22.0 && lag_slope_wal_per_s > 4_000.0)
    {
        return "collapsing";
    }
    let high_lock = lock_mean > lock_p75 * 0.92;
    let high_combined = combined_mean > combined_p75 * 0.92;
    let high_work = probe_rate > 0.15 && query_work_mean > work_p75 * 0.85;
    if high_lock && high_combined && (high_work || work_p75 < 1e-3) {
        return "saturated";
    }
    let rising = prev_lock_mean
        .map(|p| lock_mean > p * 1.12)
        .unwrap_or(false);
    if lock_mean > lock_med * 1.05
        && rising
        && lag_slope_wal_per_s < SLOPE_COLLAPSE * 0.55
        && combined_mean <= combined_p75 * 1.35
    {
        return "backpressured";
    }
    if lock_mean <= lock_p75 * 0.88 && lag_slope_wal_per_s.abs() < SLOPE_STABLE {
        return "stable";
    }
    if lock_mean > lock_med && lag_slope_wal_per_s < SLOPE_COLLAPSE * 0.45 {
        "backpressured"
    } else if high_combined {
        "saturated"
    } else {
        "stable"
    }
}

/// One client iteration in scenario C (wall time + latency decomposition + backlog).
#[derive(Clone)]
struct CTracePoint {
    t_ms: f64,
    lock_wait_ms: f64,
    /// Total wall ms spent while holding the runtime mutex this iteration.
    lock_hold_ms: f64,
    /// Sub-slice of hold time for `raw_journal_pending_sequences`.
    lag_check_ms: f64,
    /// `None` when `max_id < 1` (no MVCC probe this iteration).
    query_work_ms: Option<f64>,
    /// `None` when search probe off or skipped (throttled) this iteration.
    text_search_work_ms: Option<f64>,
    combined_client_ms: f64,
    pending_wal_sequences: u64,
}

/// Canonical per-bucket view for comparing runs / future classifiers.
#[derive(Serialize, Clone)]
struct SystemStateVector {
    lock_pressure: f64,
    execution_pressure: f64,
    lag_mean: f64,
    lag_velocity_wal_per_s: f64,
    lag_acceleration_wal_per_s2: f64,
    instability_index: f64,
    /// Write-side: estimated drain time for mean bucket WAL depth at `mat_rps_mean_used`.
    queue_pressure_wal_ms: f64,
    /// Read-side proxy: MVCC path latency scaled by relative backlog depth (not engine truth).
    queue_pressure_visibility_proxy_ms: f64,
    /// Mean `$regex` find work in the bucket (0 if no probes).
    text_search_work_ms: f64,
    regime: String,
    regime_cluster_id: Option<u8>,
}

#[derive(Serialize)]
struct TraceBucketAnalyzed {
    bucket_start_ms: u64,
    lock_wait_samples: Vec<f64>,
    query_work_samples: Vec<Option<f64>>,
    text_search_work_samples: Vec<Option<f64>>,
    combined_latency_samples: Vec<f64>,
    lock_wait_mean: f64,
    combined_latency_mean: f64,
    query_work_mean: f64,
    text_search_work_mean: f64,
    query_probe_rate: f64,
    text_search_probe_rate: f64,
    lag_mean: f64,
    /// WAL sequences / sec from first→last sample in this wall-time bucket.
    lag_derivative_wal_sequences_per_sec: f64,
    /// Phase velocity change vs previous bucket (uniform `bucket_width_ms` spacing).
    lag_acceleration_wal_per_s2: f64,
    lock_wait_variance: f64,
    combined_latency_variance: f64,
    lag_variance: f64,
    instability_index_lock: f64,
    instability_index_combined: f64,
    instability_index_lag: f64,
    instability_index_max: f64,
    queue_pressure_wal_ms: f64,
    queue_pressure_visibility_proxy_ms: f64,
    regime_cluster_id: Option<u8>,
    state_vector: SystemStateVector,
    system_regime: String,
}

#[derive(Serialize)]
struct TimeBucketedTraces {
    bucket_width_ms: u64,
    regime_classifier: String,
    mat_rps_mean_used: f64,
    regime_calibration: Option<RegimeCalibrationMeta>,
    regime_transition_counts: HashMap<String, u64>,
    regime_dwell_buckets: HashMap<String, u64>,
    regime_transition_sequence: Vec<String>,
    sticky_transition_ratio: f64,
    buckets: Vec<TraceBucketAnalyzed>,
}

fn empty_state_vector() -> SystemStateVector {
    SystemStateVector {
        lock_pressure: 0.0,
        execution_pressure: 0.0,
        lag_mean: 0.0,
        lag_velocity_wal_per_s: 0.0,
        lag_acceleration_wal_per_s2: 0.0,
        instability_index: 0.0,
        queue_pressure_wal_ms: 0.0,
        queue_pressure_visibility_proxy_ms: 0.0,
        text_search_work_ms: 0.0,
        regime: "stable".to_string(),
        regime_cluster_id: None,
    }
}

fn last_wal_lag_before_t(t_ms: f64, lag_track: &[SustainedSample]) -> u64 {
    lag_track
        .iter()
        .filter(|s| s.t_ms <= t_ms)
        .last()
        .map(|s| s.pending_wal_sequences)
        .unwrap_or(0)
}

/// Per burst impulse: overshoot + recovery curve characteristics.
fn analyze_burst_impulse_responses(
    burst_t_ms: &[f64],
    lag_track: &[SustainedSample],
    trace: &[CTracePoint],
    args: &Args,
) -> Vec<Value> {
    let sw = args.lag_deriv_smooth_window.max(1);
    let rel = args.recovery_rel_frac;
    let floor_w = args.recovery_abs_floor_wal;
    let eps = args.recovery_eps_wal_per_s;
    let hold = args.recovery_stable_hold_ms;
    burst_t_ms
        .iter()
        .map(|&t_b| {
            let baseline = last_wal_lag_before_t(t_b, lag_track);
            let mut peak_lag = baseline;
            for s in lag_track.iter().filter(|s| s.t_ms >= t_b) {
                peak_lag = peak_lag.max(s.pending_wal_sequences);
            }
            let mut peak_lock = 0.0_f64;
            for p in trace.iter().filter(|p| p.t_ms >= t_b) {
                peak_lock = peak_lock.max(p.lock_wait_ms);
            }
            let post_lag: Vec<&SustainedSample> = lag_track.iter().filter(|s| s.t_ms >= t_b).collect();
            let legacy_target = baseline as f64 * 1.12 + 2000.0;
            let mut recovery_legacy_ms: Option<f64> = None;
            for s in &post_lag {
                if s.pending_wal_sequences as f64 <= legacy_target {
                    recovery_legacy_ms = Some(s.t_ms - t_b);
                    break;
                }
            }
            let recovery_sustained_ms = sustained_baseline_stable_from(
                &post_lag,
                t_b,
                baseline,
                rel,
                floor_w,
                eps,
                hold,
                sw,
            );
            let overshoot = peak_lag.saturating_sub(baseline);
            let half_target = baseline as f64 + overshoot as f64 * 0.5;
            let mut half_life_ms: Option<f64> = None;
            for s in &post_lag {
                if s.pending_wal_sequences as f64 <= half_target {
                    half_life_ms = Some(s.t_ms - t_b);
                    break;
                }
            }
            let settle_legacy_target = baseline as f64 * 1.08 + 1500.0;
            let mut settling_legacy_ms: Option<f64> = None;
            if post_lag.len() >= 4 {
                for i in 0..(post_lag.len() - 3) {
                    if post_lag[i..=i + 3]
                        .iter()
                        .all(|s| s.pending_wal_sequences as f64 <= settle_legacy_target)
                    {
                        settling_legacy_ms = Some(post_lag[i].t_ms - t_b);
                        break;
                    }
                }
            }
            let settling_sustained_ms = recovery_sustained_ms;
            let pairs = lag_prime_pairs_from_track(&post_lag);
            let raw_v: Vec<f64> = pairs.iter().map(|(_, v)| *v).collect();
            let v_smooth = smooth_trailing_f64(&raw_v, sw);
            let mut slope_sign_changes_smoothed: u64 = 0;
            let mut prev_sign = 0i8;
            for &v in &v_smooth {
                let sign = if v > 1e-9 {
                    1
                } else if v < -1e-9 {
                    -1
                } else {
                    0
                };
                if sign != 0 && prev_sign != 0 && sign != prev_sign {
                    slope_sign_changes_smoothed = slope_sign_changes_smoothed.saturating_add(1);
                }
                if sign != 0 {
                    prev_sign = sign;
                }
            }
            let mut slope_sign_changes_raw: u64 = 0;
            prev_sign = 0;
            for w in post_lag.windows(2) {
                let dl = w[1].pending_wal_sequences as i64 - w[0].pending_wal_sequences as i64;
                let sign = if dl > 0 {
                    1
                } else if dl < 0 {
                    -1
                } else {
                    0
                };
                if sign != 0 && prev_sign != 0 && sign != prev_sign {
                    slope_sign_changes_raw = slope_sign_changes_raw.saturating_add(1);
                }
                if sign != 0 {
                    prev_sign = sign;
                }
            }
            let mut local_peaks: Vec<u64> = Vec::new();
            if post_lag.len() >= 3 {
                for w in post_lag.windows(3) {
                    let a = w[0].pending_wal_sequences;
                    let b = w[1].pending_wal_sequences;
                    let c = w[2].pending_wal_sequences;
                    if b >= a && b >= c {
                        local_peaks.push(b);
                    }
                }
            }
            let damping_ratio_proxy = if local_peaks.len() >= 2 && local_peaks[0] > 0 {
                local_peaks[1] as f64 / local_peaks[0] as f64
            } else {
                0.0
            };
            let ceiling = baseline as f64 + (baseline as f64 * rel).max(floor_w);
            json!({
                "burst_t_ms": t_b,
                "baseline_wal_sequences": baseline,
                "peak_lag_after": peak_lag,
                "peak_lock_wait_ms_after": peak_lock,
                "overshoot_wal_sequences": overshoot,
                "recovery_sustained_baseline_stable_ms": recovery_sustained_ms,
                "recovery_legacy_fixed_threshold_wal_ms": recovery_legacy_ms,
                "recovery_ceiling_wal_sequences": ceiling,
                "half_life_wal_ms": half_life_ms,
                "settling_time_sustained_baseline_stable_ms": settling_sustained_ms,
                "settling_time_legacy_three_samples_wal_ms": settling_legacy_ms,
                "lag_slope_sign_changes_smoothed_after_burst": slope_sign_changes_smoothed,
                "lag_slope_sign_changes_raw_after_burst": slope_sign_changes_raw,
                "damping_ratio_proxy": damping_ratio_proxy,
            })
        })
        .collect()
}

fn build_time_bucketed_traces(
    bucket_ms: u64,
    points: &[CTracePoint],
    mat_rps_mean: f64,
) -> Option<TimeBucketedTraces> {
    if bucket_ms == 0 || points.is_empty() {
        return None;
    }
    let max_bi = points
        .iter()
        .map(|p| (p.t_ms / bucket_ms as f64).floor() as usize)
        .max()
        .unwrap_or(0);
    let mut acc: Vec<Vec<CTracePoint>> = (0..=max_bi).map(|_| Vec::new()).collect();
    for p in points {
        let bi = (p.t_ms / bucket_ms as f64).floor() as usize;
        if let Some(v) = acc.get_mut(bi) {
            v.push(p.clone());
        }
    }

    let all_lock: Vec<f64> = points.iter().map(|p| p.lock_wait_ms).collect();
    let all_combined: Vec<f64> = points.iter().map(|p| p.combined_client_ms).collect();
    let all_work: Vec<f64> = points.iter().filter_map(|p| p.query_work_ms).collect();
    let lock_p75 = percentile_of(all_lock.clone(), 0.75);
    let lock_med = percentile_of(all_lock.clone(), 0.5);
    let combined_p75 = percentile_of(all_combined.clone(), 0.75);
    let work_p75 = if all_work.is_empty() {
        0.0
    } else {
        percentile_of(all_work.clone(), 0.75)
    };

    let mat_safe = mat_rps_mean.max(1.0);
    let dt_bucket_s = (bucket_ms as f64 / 1000.0).max(1e-9);

    let mut lock_means: Vec<f64> = Vec::new();
    let mut partial: Vec<TraceBucketAnalyzed> = Vec::new();

    for (i, entries) in acc.into_iter().enumerate() {
        let bucket_start_ms = i as u64 * bucket_ms;
        if entries.is_empty() {
            partial.push(TraceBucketAnalyzed {
                bucket_start_ms,
                lock_wait_samples: vec![],
                query_work_samples: vec![],
                text_search_work_samples: vec![],
                combined_latency_samples: vec![],
                lock_wait_mean: 0.0,
                combined_latency_mean: 0.0,
                query_work_mean: 0.0,
                text_search_work_mean: 0.0,
                query_probe_rate: 0.0,
                text_search_probe_rate: 0.0,
                lag_mean: 0.0,
                lag_derivative_wal_sequences_per_sec: 0.0,
                lag_acceleration_wal_per_s2: 0.0,
                lock_wait_variance: 0.0,
                combined_latency_variance: 0.0,
                lag_variance: 0.0,
                instability_index_lock: 0.0,
                instability_index_combined: 0.0,
                instability_index_lag: 0.0,
                instability_index_max: 0.0,
                queue_pressure_wal_ms: 0.0,
                queue_pressure_visibility_proxy_ms: 0.0,
                regime_cluster_id: None,
                state_vector: empty_state_vector(),
                system_regime: "stable".to_string(),
            });
            lock_means.push(0.0);
            continue;
        }

        let lock_wait_samples: Vec<f64> = entries.iter().map(|e| e.lock_wait_ms).collect();
        let query_work_samples: Vec<Option<f64>> =
            entries.iter().map(|e| e.query_work_ms).collect();
        let text_search_work_samples: Vec<Option<f64>> =
            entries.iter().map(|e| e.text_search_work_ms).collect();
        let combined_latency_samples: Vec<f64> =
            entries.iter().map(|e| e.combined_client_ms).collect();
        let lag_f: Vec<f64> = entries
            .iter()
            .map(|e| e.pending_wal_sequences as f64)
            .collect();

        let lock_wait_mean = mean_slice(&lock_wait_samples);
        let combined_latency_mean = mean_slice(&combined_latency_samples);
        let lag_mean = mean_slice(&lag_f);
        let n_probe = query_work_samples.iter().filter(|o| o.is_some()).count();
        let probe_rate = n_probe as f64 / query_work_samples.len().max(1) as f64;
        let work_vals: Vec<f64> = query_work_samples.iter().copied().flatten().collect();
        let query_work_mean = if work_vals.is_empty() {
            0.0
        } else {
            mean_slice(&work_vals)
        };
        let n_ts = text_search_work_samples.iter().filter(|o| o.is_some()).count();
        let text_search_probe_rate =
            n_ts as f64 / text_search_work_samples.len().max(1) as f64;
        let ts_vals: Vec<f64> = text_search_work_samples.iter().copied().flatten().collect();
        let text_search_work_mean = if ts_vals.is_empty() {
            0.0
        } else {
            mean_slice(&ts_vals)
        };

        let first = entries.first().unwrap();
        let last = entries.last().unwrap();
        let dt_s = ((last.t_ms - first.t_ms) / 1000.0).max(1e-9);
        let lag_derivative_wal_sequences_per_sec =
            (last.pending_wal_sequences as f64 - first.pending_wal_sequences as f64) / dt_s;

        let lock_wait_variance = population_variance(&lock_wait_samples);
        let combined_latency_variance = population_variance(&combined_latency_samples);
        let lag_variance = population_variance(&lag_f);

        let instability_index_lock =
            instability_index(lock_wait_variance, lock_wait_mean.abs());
        let instability_index_combined =
            instability_index(combined_latency_variance, combined_latency_mean.abs());
        let instability_index_lag = instability_index(lag_variance, lag_mean.abs().max(1.0));
        let instability_index_max = instability_index_lock
            .max(instability_index_combined)
            .max(instability_index_lag);

        let queue_pressure_wal_ms = lag_mean / mat_safe * 1000.0;
        let vis_work = query_work_mean + text_search_work_mean;
        let queue_pressure_visibility_proxy_ms = if probe_rate > 1e-9 || text_search_probe_rate > 1e-9
        {
            vis_work * (lag_mean / (lag_mean + 8000.0)).min(1.0)
        } else {
            0.0
        };

        partial.push(TraceBucketAnalyzed {
            bucket_start_ms,
            lock_wait_samples,
            query_work_samples,
            text_search_work_samples,
            combined_latency_samples,
            lock_wait_mean,
            combined_latency_mean,
            query_work_mean,
            text_search_work_mean,
            query_probe_rate: probe_rate,
            text_search_probe_rate,
            lag_mean,
            lag_derivative_wal_sequences_per_sec,
            lag_acceleration_wal_per_s2: 0.0,
            lock_wait_variance,
            combined_latency_variance,
            lag_variance,
            instability_index_lock,
            instability_index_combined,
            instability_index_lag,
            instability_index_max,
            queue_pressure_wal_ms,
            queue_pressure_visibility_proxy_ms,
            regime_cluster_id: None,
            state_vector: empty_state_vector(),
            system_regime: String::new(),
        });
        lock_means.push(lock_wait_mean);
    }

    let nb = partial.len();
    for i in 1..nb {
        if partial[i].lock_wait_samples.is_empty() || partial[i - 1].lock_wait_samples.is_empty() {
            continue;
        }
        let dv = partial[i].lag_derivative_wal_sequences_per_sec
            - partial[i - 1].lag_derivative_wal_sequences_per_sec;
        partial[i].lag_acceleration_wal_per_s2 = dv / dt_bucket_s;
    }

    let idx_nonempty: Vec<usize> = (0..nb)
        .filter(|&i| !partial[i].lock_wait_samples.is_empty())
        .collect();

    let (regime_calibration, regime_classifier) = if idx_nonempty.len() >= 2 {
        let n = idx_nonempty.len();
        let k = n.min(4).max(2);
        let mut orig: Vec<[f64; 4]> = Vec::with_capacity(n);
        for &bi in &idx_nonempty {
            let b = &partial[bi];
            orig.push([
                b.lock_wait_mean,
                b.combined_latency_mean,
                b.lag_derivative_wal_sequences_per_sec,
                b.instability_index_max,
            ]);
        }
        let (mu, sig) = column_z_params(&orig);
        let rows_z: Vec<[f64; 4]> = orig.iter().map(|row| to_z_row(row, &mu, &sig)).collect();
        let (centroids_z, labels) = kmeans_z_lloyd(&rows_z, k, 40);
        let (cluster_to_regime, cluster_severity_score) =
            regime_labels_from_clusters(k, &labels, &orig, n);
        for j in 0..n {
            let bi = idx_nonempty[j];
            partial[bi].system_regime = cluster_to_regime[labels[j]].clone();
            partial[bi].regime_cluster_id = Some(labels[j] as u8);
        }
        for i in 0..nb {
            if partial[i].lock_wait_samples.is_empty() {
                partial[i].system_regime = "stable".to_string();
            }
        }
        let cal = RegimeCalibrationMeta {
            method: "kmeans_lloyd_zscore",
            k,
            feature_names: [
                "lock_wait_mean",
                "combined_latency_mean",
                "lag_derivative_wal_per_s",
                "instability_index_max",
            ],
            centroids_z: centroids_z
                .iter()
                .map(|c| c.to_vec())
                .collect(),
            cluster_to_regime: cluster_to_regime.clone(),
            cluster_severity_score,
        };
        (
            Some(cal),
            format!("c_bucket_kmeans_k{k}_zscore"),
        )
    } else {
        for &i in &idx_nonempty {
            let prev = i.checked_sub(1).map(|j| lock_means[j]);
            let b = &mut partial[i];
            let regime = classify_regime_c(
                b.lock_wait_mean,
                b.combined_latency_mean,
                b.query_work_mean,
                b.query_probe_rate,
                b.lag_derivative_wal_sequences_per_sec,
                b.instability_index_max,
                prev,
                lock_p75,
                lock_med,
                work_p75,
                combined_p75,
            );
            b.system_regime = regime.to_string();
        }
        for i in 0..nb {
            if partial[i].lock_wait_samples.is_empty() {
                partial[i].system_regime = "stable".to_string();
            }
        }
        (None, "c_bucket_v0_fallback".to_string())
    };

    for b in &mut partial {
        b.state_vector = SystemStateVector {
            lock_pressure: b.lock_wait_mean,
            execution_pressure: b.query_work_mean,
            lag_mean: b.lag_mean,
            lag_velocity_wal_per_s: b.lag_derivative_wal_sequences_per_sec,
            lag_acceleration_wal_per_s2: b.lag_acceleration_wal_per_s2,
            instability_index: b.instability_index_max,
            queue_pressure_wal_ms: b.queue_pressure_wal_ms,
            queue_pressure_visibility_proxy_ms: b.queue_pressure_visibility_proxy_ms,
            text_search_work_ms: b.text_search_work_mean,
            regime: b.system_regime.clone(),
            regime_cluster_id: b.regime_cluster_id,
        };
    }

    let mut regime_transition_counts: HashMap<String, u64> = HashMap::new();
    let mut regime_dwell_buckets: HashMap<String, u64> = HashMap::new();
    let mut regime_transition_sequence: Vec<String> = Vec::new();
    let seq: Vec<String> = partial
        .iter()
        .filter(|b| !b.lock_wait_samples.is_empty())
        .map(|b| b.system_regime.clone())
        .collect();
    for s in &seq {
        let e = regime_dwell_buckets.entry(s.clone()).or_insert(0);
        *e = e.saturating_add(1);
    }
    let mut sticky = 0u64;
    let mut total_transitions = 0u64;
    for w in seq.windows(2) {
        let key = format!("{}->{}", w[0], w[1]);
        let e = regime_transition_counts.entry(key.clone()).or_insert(0);
        *e = e.saturating_add(1);
        regime_transition_sequence.push(key);
        total_transitions = total_transitions.saturating_add(1);
        if w[0] == w[1] {
            sticky = sticky.saturating_add(1);
        }
    }
    let sticky_transition_ratio = if total_transitions == 0 {
        1.0
    } else {
        sticky as f64 / total_transitions as f64
    };

    Some(TimeBucketedTraces {
        bucket_width_ms: bucket_ms,
        regime_classifier,
        mat_rps_mean_used: mat_rps_mean,
        regime_calibration,
        regime_transition_counts,
        regime_dwell_buckets,
        regime_transition_sequence,
        sticky_transition_ratio,
        buckets: partial,
    })
}

fn start_background_materializer(
    rt: Arc<Mutex<EngineRuntime>>,
    stop: Arc<AtomicBool>,
    data_dir: PathBuf,
    mat_interval_ms: u64,
    mat_idle_ms: u64,
    materializer_batch: usize,
    materializer_max_batches_per_tick: usize,
    mat_log: Arc<Mutex<Vec<MatEvent>>>,
    bench_t0: Instant,
) {
    thread::spawn(move || {
        let coll = COLL.to_string();
        let mut last_applied_seq: u64 = 0;
        let jr = data_dir.join("raw_journal").join(&coll);
        let mut pipe = match LsmWritePipeline::open_or_create(&jr, RAW_JOURNAL_INNER, 8_192) {
            Ok(p) => p,
            Err(_) => return,
        };
        let trace_mat_lock = std::env::var_os("DNADB_BENCH_MAT_LOCK_TRACE").is_some();
        while !stop.load(Ordering::Relaxed) {
            let mut pending_after = 0u64;
            let mut applied_any = false;
            let rounds = materializer_max_batches_per_tick.max(1);
            for _ in 0..rounds {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                let bs = materializer_batch.max(1);
                let decoded = match pipe.decode_raw_wal_from_sequence(
                    last_applied_seq.saturating_add(1),
                    bs,
                ) {
                    Ok(v) => v,
                    Err(_) => break,
                };
                if decoded.is_empty() {
                    break;
                }
                let mut docs: Vec<Value> = Vec::with_capacity(decoded.len());
                let mut max_seq = last_applied_seq;
                for d in &decoded {
                    if let Ok(v) = serde_json::from_slice::<Value>(&d.payload_json) {
                        docs.push(v);
                        max_seq = max_seq.max(d.wal_sequence);
                    }
                }
                let (applied, apply_hold_ms) = {
                    let mut g = match rt.lock() {
                        Ok(x) => x,
                        Err(_) => return,
                    };
                    let hold_t0 = Instant::now();
                    let t_ms = bench_t0.elapsed().as_secs_f64() * 1000.0;
                    let applied = g
                        .apply_materialized_docs_direct(
                            &coll,
                            docs,
                        )
                        .unwrap_or(0);
                    if let Ok(mut log) = mat_log.lock() {
                        let hw = pipe.raw_wal_high_water_sequence().unwrap_or(0);
                        let pa = hw.saturating_sub(max_seq);
                        let dur = hold_t0.elapsed().as_secs_f64() * 1000.0;
                        let mps = if dur > 0.0 {
                            Some((applied as f64) / (dur / 1000.0))
                        } else {
                            None
                        };
                        log.push(MatEvent {
                            t_ms,
                            records_applied: applied as u64,
                            mat_records_per_sec: mps,
                            duration_ms: Some(dur),
                            pending_wal_sequences_after: pa,
                        });
                    }
                    (
                        applied as u64,
                        hold_t0.elapsed().as_secs_f64() * 1000.0,
                    )
                };
                if applied > 0 {
                    last_applied_seq = max_seq;
                }
                if trace_mat_lock && apply_hold_ms > 50.0 {
                    eprintln!("bench_mat_lock_trace: apply_hold_ms={apply_hold_ms:.3}");
                }
                pending_after = pipe
                    .raw_wal_high_water_sequence()
                    .unwrap_or(last_applied_seq)
                    .saturating_sub(last_applied_seq);
                if applied == 0 {
                    break;
                }
                applied_any = true;
                thread::yield_now();
            }
            if applied_any {
                // One sync per scheduler tick (best-effort) so we don't force lock queuing
                // when the runtime lock is already contended by ingest/query work.
                let sync_t0 = Instant::now();
                if let Ok(mut g) = rt.try_lock() {
                    let _ = g.sync_collection_storage(&coll);
                }
                let sync_hold_ms = sync_t0.elapsed().as_secs_f64() * 1000.0;
                if trace_mat_lock && sync_hold_ms > 50.0 {
                    eprintln!("bench_mat_lock_trace: sync_hold_ms={sync_hold_ms:.3}");
                }
            }
            let sleep_ms = if pending_after == 0 || !applied_any {
                mat_idle_ms
            } else {
                mat_interval_ms
            };
            thread::sleep(Duration::from_millis(sleep_ms));
        }
    });
}

#[derive(Serialize)]
struct ScenarioOut {
    scenario: String,
    #[serde(flatten)]
    payload: HashMap<String, serde_json::Value>,
}

fn resolve_vis_probes(
    rt: &Arc<Mutex<EngineRuntime>>,
    vis_pending: &mut Vec<(u64, Instant)>,
    checkpoint_out: &mut Vec<Checkpoint>,
) {
    vis_pending.retain(|(probe_id, t0)| {
        match rt.lock() {
            Ok(mut g) => {
                if row_visible(&mut g, *probe_id) {
                    let ms = t0.elapsed().as_secs_f64() * 1000.0;
                    if let Some(cp) = checkpoint_out.iter_mut().find(|c| c.total_raw_rows == *probe_id)
                    {
                        cp.visibility_under_load_ms = Some(ms);
                    }
                    false
                } else {
                    true
                }
            }
            Err(_) => true,
        }
    });
}

fn scenario_a(args: &Args) -> Result<ScenarioOut, String> {
    let milestones = milestone_table(args.phase_max_rows);
    let target = *milestones
        .last()
        .ok_or_else(|| "milestones empty".to_string())?;
    let goal_milestone = target;
    let t_start = Instant::now();
    let rare_mod = rare_doc_mod_from_args(args);

    let rt = Arc::new(Mutex::new(EngineRuntime::open(
        &args.data_dir,
        args.mmap_bytes,
    )));
    let stop = Arc::new(AtomicBool::new(false));
    let mat_log = Arc::new(Mutex::new(Vec::<MatEvent>::new()));
    start_background_materializer(
        rt.clone(),
        stop.clone(),
        args.data_dir.clone(),
        args.mat_interval_ms,
        args.mat_idle_ms,
        args.materializer_batch,
        args.materializer_max_batches_per_tick,
        mat_log.clone(),
        t_start,
    );
    let total_written = Arc::new(AtomicU64::new(0));
    let next_id = Arc::new(AtomicU64::new(1));
    let rt_ingest = rt.clone();
    let w_st = total_written.clone();
    let w_ni = next_id.clone();
    let batch = args.ingest_batch;
    let ingest = thread::spawn(move || -> Result<(), String> {
        while w_st.load(Ordering::Relaxed) < target {
            let start = w_ni.load(Ordering::Relaxed);
            let remain = target - w_st.load(Ordering::Relaxed);
            if remain == 0 {
                break;
            }
            let chunk = (batch as u64).min(remain) as usize;
            let mut g = rt_ingest.lock().map_err(|e| e.to_string())?;
            g.execute_raw_segment_bulk_insert(COLL, make_batch(start, chunk, rare_mod))
                .map_err(|e| e.to_string())?;
            w_ni.fetch_add(chunk as u64, Ordering::Relaxed);
            w_st.fetch_add(chunk as u64, Ordering::Relaxed);
        }
        Ok(())
    });

    let mut samples: Vec<SustainedSample> = Vec::new();
    let mut global_max_lag = 0u64;
    let mut checkpoint_out: Vec<Checkpoint> = Vec::new();
    let mut vis_pending: Vec<(u64, Instant)> = Vec::new();
    let mut last_milestone_done: u64 = 0;
    let mut window_lag_max: u64 = 0;
    let mut t_window_start = t_start;
    let mut rows_at_window_start: u64 = 0;

    // Ingest may finish before we've observed every crossing from `lag_samples` — keep sampling until the last checkpoint row count is recorded.
    while total_written.load(Ordering::Relaxed) < target
        || !ingest.is_finished()
        || last_milestone_done < goal_milestone
        || !vis_pending.is_empty()
    {
        let n = total_written.load(Ordering::Relaxed);
        if let Ok(mut g) = rt.try_lock() {
            let lag = g.raw_journal_pending_sequences(COLL).unwrap_or(0);
            global_max_lag = global_max_lag.max(lag);
            window_lag_max = window_lag_max.max(lag);
            samples.push(SustainedSample {
                t_ms: t_start.elapsed().as_secs_f64() * 1000.0,
                total_raw_rows: n,
                pending_wal_sequences: lag,
            });
        }
        // At most one milestone per sampler tick so window_secs stays meaningful if ingest runs ahead of this loop.
        for &m in &milestones {
            if last_milestone_done < m && n >= m {
                let window_sec = t_window_start.elapsed().as_secs_f64().max(f64::EPSILON);
                let window_rows = m.saturating_sub(rows_at_window_start);
                let ingest_rps_window = window_rows as f64 / window_sec;
                checkpoint_out.push(Checkpoint {
                    total_raw_rows: m,
                    window_rows,
                    max_lag_sampled_in_window: window_lag_max,
                    visibility_under_load_ms: None,
                    window_ingest_rows_per_sec: ingest_rps_window,
                });
                vis_pending.push((m, Instant::now()));
                last_milestone_done = m;
                t_window_start = Instant::now();
                rows_at_window_start = m;
                window_lag_max = 0;
                break;
            }
        }

        resolve_vis_probes(&rt, &mut vis_pending, &mut checkpoint_out);

        thread::sleep(Duration::from_millis(args.lag_sample_ms.max(5)));
    }
    let _ = ingest.join().map_err(|e| format!("ingest join: {e:?}"))??;

    let settle_deadline = Instant::now() + Duration::from_secs(120);
    while !vis_pending.is_empty() && Instant::now() < settle_deadline {
        resolve_vis_probes(&rt, &mut vis_pending, &mut checkpoint_out);
        thread::sleep(Duration::from_millis(5));
    }

    stop.store(true, Ordering::Relaxed);
    thread::sleep(Duration::from_millis(args.mat_idle_ms + 100));

    let last_probe_id = *milestones.last().expect("milestones");
    let (probe_visible, settle_probe_ms) = time_to_visibility(&rt, last_probe_id)?;

    let total_sec = t_start.elapsed().as_secs_f64();
    let mut m: HashMap<String, serde_json::Value> = HashMap::new();
    m.insert(
        "pipeline".to_string(),
        json!("raw_segment -> materialize -> query"),
    );
    m.insert("target_total_rows".to_string(), json!(target));
    m.insert(
        "checkpoint_rows".to_string(),
        json!(milestones),
    );
    m.insert("ingest_batch".to_string(), json!(args.ingest_batch));
    m.insert("mat_interval_ms".to_string(), json!(args.mat_interval_ms));
    m.insert("mat_idle_ms".to_string(), json!(args.mat_idle_ms));
    m.insert("materializer_batch".to_string(), json!(args.materializer_batch));
    m.insert(
        "materializer_max_batches_per_tick".to_string(),
        json!(args.materializer_max_batches_per_tick),
    );
    m.insert("total_wall_sec".to_string(), json!(total_sec));
    m.insert(
        "overall_ingest_rows_per_sec".to_string(),
        json!(target as f64 / total_sec),
    );
    m.insert("max_lag_pending_wal_sampled".to_string(), json!(global_max_lag));
    m.insert("lag_samples".to_string(), json!(samples));
    let lag_derivative = lag_derivative_series(&samples);
    let lag_deriv_smooth =
        smooth_lag_deriv_trailing(&lag_derivative, args.lag_deriv_smooth_window.max(1));
    let d_lag_vals: Vec<f64> = lag_derivative
        .iter()
        .map(|d| d.d_lag_d_t_wal_sequences_per_sec)
        .collect();
    let d_lag_smooth_vals: Vec<f64> = lag_deriv_smooth
        .iter()
        .map(|d| d.d_lag_d_t_wal_sequences_per_sec)
        .collect();
    m.insert("lag_derivative".to_string(), json!(lag_derivative));
    m.insert("lag_derivative_smoothed".to_string(), json!(lag_deriv_smooth));
    m.insert(
        "lag_acceleration".to_string(),
        json!(lag_acceleration_series(&lag_deriv_smooth)),
    );
    m.insert(
        "mean_d_lag_d_t_wal_sequences_per_sec".to_string(),
        json!(mean_slice(&d_lag_smooth_vals)),
    );
    m.insert(
        "mean_d_lag_d_t_wal_sequences_per_sec_raw".to_string(),
        json!(mean_slice(&d_lag_vals)),
    );
    m.insert(
        "lag_deriv_smooth_window".to_string(),
        json!(args.lag_deriv_smooth_window.max(1)),
    );
    let mat_events: Vec<MatEvent> = mat_log
        .lock()
        .map_err(|e| e.to_string())?
        .clone();
    let mat_rps: Vec<f64> = mat_events
        .iter()
        .filter_map(|e| e.mat_records_per_sec)
        .collect();
    m.insert("materialize_time_series".to_string(), json!(mat_events));
    m.insert(
        "mat_nonzero_tick_mean_records_per_sec".to_string(),
        json!(mean_slice(&mat_rps)),
    );
    m.insert("lag_sample_ms".to_string(), json!(args.lag_sample_ms));
    let vis_ms: Vec<f64> = checkpoint_out
        .iter()
        .filter_map(|c| c.visibility_under_load_ms)
        .collect();
    m.insert(
        "visibility_under_load_ms_p50".to_string(),
        json!(percentile_of(vis_ms.clone(), 0.5)),
    );
    m.insert(
        "visibility_under_load_ms_p95".to_string(),
        json!(percentile_of(vis_ms.clone(), 0.95)),
    );
    m.insert(
        "visibility_under_load_ms_p99".to_string(),
        json!(percentile_of(vis_ms, 0.99)),
    );
    m.insert("checkpoints".to_string(), json!(checkpoint_out));
    m.insert(
        "final_probe_last_checkpoint_row_visible_after_stop".to_string(),
        json!(probe_visible),
    );
    m.insert(
        "final_probe_last_checkpoint_ms_after_mat_idle".to_string(),
        json!(settle_probe_ms),
    );
    m.insert(
        "final_probe_row_id".to_string(),
        json!(last_probe_id),
    );
    Ok(ScenarioOut {
        scenario: "A_sustained".to_string(),
        payload: m,
    })
}

#[derive(Serialize)]
struct SustainedSample {
    t_ms: f64,
    total_raw_rows: u64,
    pending_wal_sequences: u64,
}

#[derive(Serialize)]
struct Checkpoint {
    total_raw_rows: u64,
    window_rows: u64,
    max_lag_sampled_in_window: u64,
    /// First successful MVCC `find` for row id `total_raw_rows` while ingest + materializer still running (`None` until observed).
    visibility_under_load_ms: Option<f64>,
    window_ingest_rows_per_sec: f64,
}

fn scenario_b(args: &Args) -> Result<ScenarioOut, String> {
    let t0 = Instant::now();
    let rare_mod = rare_doc_mod_from_args(args);
    let rt = Arc::new(Mutex::new(EngineRuntime::open(
        &args.data_dir,
        args.mmap_bytes,
    )));
    let stop = Arc::new(AtomicBool::new(false));
    let mat_log = Arc::new(Mutex::new(Vec::<MatEvent>::new()));
    start_background_materializer(
        rt.clone(),
        stop.clone(),
        args.data_dir.clone(),
        args.mat_interval_ms,
        args.mat_idle_ms,
        args.materializer_batch,
        args.materializer_max_batches_per_tick,
        mat_log.clone(),
        t0,
    );

    let n = args.burst_records;
    let t_burst0 = Instant::now();
    let mut id = 1u64;
    let mut burst_peak_lag: u64 = 0;
    let mut max_lag = 0u64;
    let batch = args.ingest_batch;
    while id <= n {
        let take = (n - id + 1).min(batch as u64) as usize;
        {
            let mut g = rt.lock().map_err(|e| e.to_string())?;
            g.execute_raw_segment_bulk_insert(COLL, make_batch(id, take, rare_mod))
                .map_err(|e| e.to_string())?;
        }
        id += take as u64;
        if let Ok(mut g) = rt.try_lock() {
            if let Ok(l) = g.raw_journal_pending_sequences(COLL) {
                max_lag = max_lag.max(l);
                burst_peak_lag = burst_peak_lag.max(l);
            }
        }
    }
    let burst_sec = t_burst0.elapsed().as_secs_f64();
    let t_drain0 = Instant::now();
    let (recovered, drain_sec) = loop {
        let lag = {
            let mut g = rt.lock().map_err(|e| e.to_string())?;
            g.raw_journal_pending_sequences(COLL).map_err(|e| e.to_string())?
        };
        if lag == 0 {
            break (true, t_drain0.elapsed().as_secs_f64());
        }
        max_lag = max_lag.max(lag);
        thread::sleep(Duration::from_millis(2));
        if t_drain0.elapsed() > Duration::from_secs(300) {
            break (false, 300.0);
        }
    };
    stop.store(true, Ordering::Relaxed);
    let mat_events: Vec<MatEvent> = mat_log
        .lock()
        .map_err(|e| e.to_string())?
        .clone();
    let mat_rps: Vec<f64> = mat_events
        .iter()
        .filter_map(|e| e.mat_records_per_sec)
        .collect();
    let mut m: HashMap<String, serde_json::Value> = HashMap::new();
    m.insert("pipeline".to_string(), json!("raw_segment -> materialize -> query"));
    m.insert("burst_raw_rows".to_string(), json!(n));
    m.insert("ingest_batch".to_string(), json!(args.ingest_batch));
    m.insert("burst_ingest_sec".to_string(), json!(burst_sec));
    m.insert("burst_ingest_rows_per_sec".to_string(), json!(n as f64 / burst_sec));
    m.insert("burst_peak_lag_pending_wal".to_string(), json!(burst_peak_lag));
    m.insert("max_lag_pending_wal_incl_drain".to_string(), json!(max_lag));
    m.insert("drain_to_zero_sec".to_string(), json!(drain_sec));
    m.insert("drain_reached_zero".to_string(), json!(recovered));
    m.insert("mat_interval_ms".to_string(), json!(args.mat_interval_ms));
    m.insert("materializer_batch".to_string(), json!(args.materializer_batch));
    m.insert(
        "materializer_max_batches_per_tick".to_string(),
        json!(args.materializer_max_batches_per_tick),
    );
    m.insert("materialize_time_series".to_string(), json!(mat_events));
    m.insert(
        "mat_nonzero_tick_mean_records_per_sec".to_string(),
        json!(mean_slice(&mat_rps)),
    );
    Ok(ScenarioOut {
        scenario: "B_burst".to_string(),
        payload: m,
    })
}

/// Mixed ingest + MVCC read probe + optional search (scenario **C**); also used by **E** per tier.
fn scenario_mixed_workload(args: &Args) -> Result<HashMap<String, Value>, String> {
    let t0 = Instant::now();
    let rare_mod = rare_doc_mod_from_args(args);
    let postings: Arc<Mutex<PostingsMap>> = Arc::new(Mutex::new(HashMap::new()));
    let rt = Arc::new(Mutex::new(EngineRuntime::open(
        &args.data_dir,
        args.mmap_bytes,
    )));
    let stop = Arc::new(AtomicBool::new(false));
    let ingest_done = Arc::new(AtomicBool::new(false));
    let ingest_stop_ms_micros = Arc::new(AtomicU64::new(0));
    let ingest_cap = args.stop_after_raw_rows;
    let ingest_done_ing = ingest_done.clone();
    let ingest_stop_ms_ing = ingest_stop_ms_micros.clone();
    let mat_log = Arc::new(Mutex::new(Vec::<MatEvent>::new()));
    start_background_materializer(
        rt.clone(),
        stop.clone(),
        args.data_dir.clone(),
        args.mat_interval_ms,
        args.mat_idle_ms,
        args.materializer_batch,
        args.materializer_max_batches_per_tick,
        mat_log.clone(),
        t0,
    );

    let next_id = Arc::new(AtomicU64::new(1));
    let ni = next_id.clone();
    let st = stop.clone();
    let b = args.ingest_batch;
    let ib = rt.clone();
    let pause = args.ingest_pause_ms;
    let burst_times = Arc::new(Mutex::new(Vec::<f64>::new()));
    let burst_times_ing = burst_times.clone();
    let burst_period = args.burst_period_sec;
    let burst_sz = args.burst_size;
    let burst_ms = args.burst_inject_ms.max(1);
    let bench_t0_ing = t0;
    let postings_ing = postings.clone();
    let search_en_ing = args.search_enabled;
    let _ing = thread::spawn(move || {
        let mut next_burst = if burst_period > 0 {
            bench_t0_ing + Duration::from_secs(burst_period)
        } else {
            bench_t0_ing
        };
        while !st.load(Ordering::Relaxed) && !ingest_done_ing.load(Ordering::Relaxed) {
            if burst_period > 0 && Instant::now() >= next_burst {
                let t_ms = bench_t0_ing.elapsed().as_secs_f64() * 1000.0;
                if let Ok(mut g) = burst_times_ing.lock() {
                    g.push(t_ms);
                }
                let deadline = Instant::now() + Duration::from_millis(burst_ms);
                let mut remain = burst_sz;
                while remain > 0
                    && Instant::now() < deadline
                    && !st.load(Ordering::Relaxed)
                    && !ingest_done_ing.load(Ordering::Relaxed)
                {
                    let start = ni.load(Ordering::Relaxed);
                    let chunk = (b as u64).min(remain) as usize;
                    {
                        let mut g = ib.lock().expect("ingest burst lock");
                        if g
                            .execute_raw_segment_bulk_insert(COLL, make_batch(start, chunk, rare_mod))
                            .is_err()
                        {
                            break;
                        }
                    }
                    ni.fetch_add(chunk as u64, Ordering::Relaxed);
                    if search_en_ing {
                        register_row_range(&postings_ing, start, chunk, rare_mod);
                    }
                    if ingest_cap.is_some_and(|c| ni.load(Ordering::Relaxed).saturating_sub(1) >= c) {
                        ingest_done_ing.store(true, Ordering::Relaxed);
                        let us =
                            (bench_t0_ing.elapsed().as_secs_f64() * 1_000_000.0).round() as u64;
                        ingest_stop_ms_ing.store(us, Ordering::Relaxed);
                        break;
                    }
                    remain = remain.saturating_sub(chunk as u64);
                }
                next_burst += Duration::from_secs(burst_period);
            }
            if ingest_done_ing.load(Ordering::Relaxed) {
                break;
            }
            let start = ni.load(Ordering::Relaxed);
            {
                let mut g = ib.lock().expect("ingest lock");
                let _ = g
                    .execute_raw_segment_bulk_insert(COLL, make_batch(start, b, rare_mod))
                    .map_err(|e| e.to_string());
            }
            ni.fetch_add(b as u64, Ordering::Relaxed);
            if search_en_ing {
                register_row_range(&postings_ing, start, b, rare_mod);
            }
            if ingest_cap.is_some_and(|c| ni.load(Ordering::Relaxed).saturating_sub(1) >= c) {
                ingest_done_ing.store(true, Ordering::Relaxed);
                let us =
                    (bench_t0_ing.elapsed().as_secs_f64() * 1_000_000.0).round() as u64;
                ingest_stop_ms_ing.store(us, Ordering::Relaxed);
            }
            if pause > 0 {
                thread::sleep(Duration::from_millis(pause));
            }
        }
    });

    let mut lags: Vec<u64> = Vec::new();
    let mut lag_track: Vec<SustainedSample> = Vec::new();
    let mut trace_points: Vec<CTracePoint> = Vec::new();
    let mut high_lag_queries: u64 = 0;
    const LAG_SPIKE: u64 = 10_000;
    let t_end = Instant::now() + Duration::from_secs(args.duration_sec);
    let q_ms = args.query_interval_ms;
    let search_period_ms = 1000.0 / args.search_qps.max(1e-9);
    let mut last_search_ms = -1.0e9_f64;
    let mut search_fire_count: u64 = 0;
    while Instant::now() < t_end {
        let max_id = next_id.load(Ordering::Relaxed).saturating_sub(1);
        let t_ms = t0.elapsed().as_secs_f64() * 1000.0;
        let mut ids_op: Option<Vec<u64>> = None;
        if args.search_enabled
            && max_id >= 1
            && (t_ms - last_search_ms) >= search_period_ms
        {
            let term = pick_search_term(search_fire_count, args);
            search_fire_count = search_fire_count.saturating_add(1);
            ids_op = postings
                .lock()
                .expect("postings")
                .get(term)
                .map(|q| {
                    q.iter()
                        .rev()
                        .take(args.search_scan)
                        .copied()
                        .collect::<Vec<_>>()
                });
            last_search_ms = t_ms;
        }
        let t_acquire = Instant::now();
        let mut g = rt.lock().map_err(|e| e.to_string())?;
        let wait_ms = t_acquire.elapsed().as_secs_f64() * 1000.0;
        let t_lag = Instant::now();
        let lag = g.raw_journal_pending_sequences(COLL).unwrap_or(0);
        let lag_check_ms = t_lag.elapsed().as_secs_f64() * 1000.0;
        lags.push(lag);
        lag_track.push(SustainedSample {
            t_ms,
            total_raw_rows: max_id,
            pending_wal_sequences: lag,
        });
        let mut q_work: Option<f64> = None;
        let mut ts_work: Option<f64> = None;
        let mut combined = wait_ms;
        if max_id >= 1 {
            let probe = (max_id / 2).max(1);
            let tq = Instant::now();
            let _ = row_visible(&mut g, probe);
            let qw = tq.elapsed().as_secs_f64() * 1000.0;
            q_work = Some(qw);
            combined = wait_ms + qw;
            if let Some(ids) = ids_op {
                if ids.is_empty() {
                    ts_work = Some(0.0);
                } else if let Ok(ms) = search_posting_scan_ms(&mut g, &ids) {
                    ts_work = Some(ms);
                    combined += ms;
                }
            }
            if lag > LAG_SPIKE {
                high_lag_queries = high_lag_queries.saturating_add(1);
            }
        }
        trace_points.push(CTracePoint {
            t_ms,
            lock_wait_ms: wait_ms,
            lock_hold_ms: t_acquire.elapsed().as_secs_f64() * 1000.0,
            lag_check_ms,
            query_work_ms: q_work,
            text_search_work_ms: ts_work,
            combined_client_ms: combined,
            pending_wal_sequences: lag,
        });
        drop(g);
        thread::sleep(Duration::from_millis(q_ms));
    }
    stop.store(true, Ordering::Relaxed);
    let _ = _ing.join();
    thread::sleep(Duration::from_millis(200));
    let ingest_stop_ms = if ingest_done.load(Ordering::Relaxed) {
        let us = ingest_stop_ms_micros.load(Ordering::Relaxed);
        if us > 0 {
            Some(us as f64 / 1000.0)
        } else {
            None
        }
    } else {
        None
    };

    let lock_wait_ms: Vec<f64> = trace_points.iter().map(|p| p.lock_wait_ms).collect();
    let lock_hold_ms: Vec<f64> = trace_points.iter().map(|p| p.lock_hold_ms).collect();
    let lag_check_ms: Vec<f64> = trace_points.iter().map(|p| p.lag_check_ms).collect();
    let query_work_ms: Vec<f64> = trace_points
        .iter()
        .filter_map(|p| p.query_work_ms)
        .collect();
    let text_search_work_ms: Vec<f64> = trace_points
        .iter()
        .filter_map(|p| p.text_search_work_ms)
        .collect();
    let combined_client_ms: Vec<f64> = trace_points
        .iter()
        .map(|p| p.combined_client_ms)
        .collect();
    let trace_t_ms: Vec<f64> = trace_points.iter().map(|p| p.t_ms).collect();
    let lag_series_f64: Vec<f64> = trace_points
        .iter()
        .map(|p| p.pending_wal_sequences as f64)
        .collect();

    let end_p50 = percentile_of(combined_client_ms.clone(), 0.50);
    let end_p95 = percentile_of(combined_client_ms.clone(), 0.95);
    let end_p99 = percentile_of(combined_client_ms.clone(), 0.99);
    let lag_deriv_c = lag_derivative_series(&lag_track);
    let d_lag_vals: Vec<f64> = lag_deriv_c
        .iter()
        .map(|d| d.d_lag_d_t_wal_sequences_per_sec)
        .collect();
    let mat_events: Vec<MatEvent> = mat_log
        .lock()
        .map_err(|e| e.to_string())?
        .clone();
    let mat_rps: Vec<f64> = mat_events
        .iter()
        .filter_map(|e| e.mat_records_per_sec)
        .collect();
    let mat_rps_mean = mean_slice(&mat_rps);
    let lag_max = lags.iter().copied().max().unwrap_or(0);
    let lag_min = lags.iter().copied().min().unwrap_or(0);
    let lag_mean = if lags.is_empty() {
        0.0
    } else {
        lags.iter().map(|&x| x as f64).sum::<f64>() / lags.len() as f64
    };

    let mut m: HashMap<String, serde_json::Value> = HashMap::new();
    m.insert("pipeline".to_string(), json!("raw_segment -> materialize + concurrent query"));
    m.insert("duration_sec".to_string(), json!(args.duration_sec));
    m.insert("ingest_batch".to_string(), json!(args.ingest_batch));
    m.insert("ingest_pause_ms".to_string(), json!(args.ingest_pause_ms));
    m.insert("query_interval_ms".to_string(), json!(args.query_interval_ms));
    m.insert("search_enabled".to_string(), json!(args.search_enabled));
    m.insert("search_qps".to_string(), json!(args.search_qps));
    m.insert("search_scan".to_string(), json!(args.search_scan));
    m.insert(
        "search_selectivity".to_string(),
        json!(match args.search_selectivity {
            SearchSelectivity::Common => "common",
            SearchSelectivity::Rare => "rare",
            SearchSelectivity::Mixed => "mixed",
        }),
    );
    m.insert(
        "search_rare_doc_mod".to_string(),
        json!(args.search_rare_doc_mod),
    );
    m.insert(
        "search_mixed_rare_query_pct".to_string(),
        json!(args.search_mixed_rare_query_pct),
    );
    m.insert(
        "lag_deriv_smooth_window".to_string(),
        json!(args.lag_deriv_smooth_window.max(1)),
    );
    m.insert("recovery_rel_frac".to_string(), json!(args.recovery_rel_frac));
    m.insert(
        "recovery_abs_floor_wal".to_string(),
        json!(args.recovery_abs_floor_wal),
    );
    m.insert(
        "recovery_eps_wal_per_s".to_string(),
        json!(args.recovery_eps_wal_per_s),
    );
    m.insert(
        "recovery_stable_hold_ms".to_string(),
        json!(args.recovery_stable_hold_ms),
    );
    m.insert(
        "text_search_path_samples".to_string(),
        json!(text_search_work_ms.len()),
    );
    m.insert(
        "text_search_work_ms_p50".to_string(),
        json!(percentile_of(text_search_work_ms.clone(), 0.5)),
    );
    m.insert(
        "text_search_work_ms_p95".to_string(),
        json!(percentile_of(text_search_work_ms.clone(), 0.95)),
    );
    m.insert(
        "text_search_work_ms_p99".to_string(),
        json!(percentile_of(text_search_work_ms.clone(), 0.99)),
    );
    m.insert(
        "end_to_end_client_ms_p50".to_string(),
        json!(end_p50),
    );
    m.insert(
        "end_to_end_client_ms_p95".to_string(),
        json!(end_p95),
    );
    m.insert(
        "end_to_end_client_ms_p99".to_string(),
        json!(end_p99),
    );
    let (ing_combined, drain_combined) =
        phase_split_pair(&combined_client_ms, &trace_t_ms, ingest_stop_ms);
    m.insert(
        "end_to_end_client_ms_p99_ingest".to_string(),
        json!(percentile_of(ing_combined.clone(), 0.99)),
    );
    m.insert(
        "end_to_end_client_ms_p99_drain".to_string(),
        json!(percentile_of(drain_combined.clone(), 0.99)),
    );
    m.insert(
        "lock_wait_ms_p50".to_string(),
        json!(percentile_of(lock_wait_ms.clone(), 0.5)),
    );
    m.insert(
        "lock_wait_ms_p95".to_string(),
        json!(percentile_of(lock_wait_ms.clone(), 0.95)),
    );
    m.insert(
        "lock_wait_ms_p99".to_string(),
        json!(percentile_of(lock_wait_ms.clone(), 0.99)),
    );
    m.insert(
        "rt_lock_hold_ms_p50".to_string(),
        json!(percentile_of(lock_hold_ms.clone(), 0.5)),
    );
    m.insert(
        "rt_lock_hold_ms_p95".to_string(),
        json!(percentile_of(lock_hold_ms.clone(), 0.95)),
    );
    m.insert(
        "rt_lock_hold_ms_p99".to_string(),
        json!(percentile_of(lock_hold_ms.clone(), 0.99)),
    );
    m.insert(
        "lag_check_ms_p99".to_string(),
        json!(percentile_of(lag_check_ms.clone(), 0.99)),
    );
    let (ing_lock, drain_lock) = phase_split_pair(&lock_wait_ms, &trace_t_ms, ingest_stop_ms);
    let (ing_hold, drain_hold) = phase_split_pair(&lock_hold_ms, &trace_t_ms, ingest_stop_ms);
    m.insert(
        "lock_wait_ms_p99_ingest".to_string(),
        json!(percentile_of(ing_lock.clone(), 0.99)),
    );
    m.insert(
        "lock_wait_ms_p99_drain".to_string(),
        json!(percentile_of(drain_lock.clone(), 0.99)),
    );
    m.insert(
        "rt_lock_hold_ms_p99_ingest".to_string(),
        json!(percentile_of(ing_hold.clone(), 0.99)),
    );
    m.insert(
        "rt_lock_hold_ms_p99_drain".to_string(),
        json!(percentile_of(drain_hold.clone(), 0.99)),
    );
    m.insert(
        "query_work_ms_p50".to_string(),
        json!(percentile_of(query_work_ms.clone(), 0.5)),
    );
    m.insert(
        "query_work_ms_p95".to_string(),
        json!(percentile_of(query_work_ms.clone(), 0.95)),
    );
    m.insert(
        "query_work_ms_p99".to_string(),
        json!(percentile_of(query_work_ms.clone(), 0.99)),
    );
    m.insert("query_path_samples".to_string(), json!(query_work_ms.len()));
    m.insert("iteration_samples".to_string(), json!(combined_client_ms.len()));
    let (ing_lag, drain_lag) = phase_split_pair(&lag_series_f64, &trace_t_ms, ingest_stop_ms);
    m.insert(
        "lag_p99_ingest".to_string(),
        json!(percentile_of(ing_lag.clone(), 0.99)),
    );
    m.insert(
        "lag_p99_drain".to_string(),
        json!(percentile_of(drain_lag.clone(), 0.99)),
    );
    m.insert("lag_mean_ingest".to_string(), json!(mean_slice(&ing_lag)));
    m.insert("lag_mean_drain".to_string(), json!(mean_slice(&drain_lag)));
    m.insert(
        "lag_max_ingest".to_string(),
        json!(ing_lag.iter().copied().fold(0.0_f64, f64::max)),
    );
    m.insert(
        "lag_max_drain".to_string(),
        json!(drain_lag.iter().copied().fold(0.0_f64, f64::max)),
    );
    m.insert(
        "queries_with_pending_wal_over_10k".to_string(),
        json!(high_lag_queries),
    );
    let lag_deriv_smooth_c =
        smooth_lag_deriv_trailing(&lag_deriv_c, args.lag_deriv_smooth_window.max(1));
    let d_lag_smooth_vals: Vec<f64> = lag_deriv_smooth_c
        .iter()
        .map(|d| d.d_lag_d_t_wal_sequences_per_sec)
        .collect();
    m.insert("lag_derivative".to_string(), json!(lag_deriv_c));
    m.insert(
        "lag_derivative_smoothed".to_string(),
        json!(lag_deriv_smooth_c),
    );
    let lag_accel_c = lag_acceleration_series(&lag_deriv_smooth_c);
    m.insert("lag_acceleration".to_string(), json!(lag_accel_c));
    m.insert(
        "mean_d_lag_d_t_wal_sequences_per_sec".to_string(),
        json!(mean_slice(&d_lag_smooth_vals)),
    );
    m.insert(
        "mean_d_lag_d_t_wal_sequences_per_sec_raw".to_string(),
        json!(mean_slice(&d_lag_vals)),
    );
    let deriv_t_ms: Vec<f64> = lag_deriv_smooth_c.iter().map(|d| d.t_ms).collect();
    let (ing_d, drain_d) = phase_split_pair(&d_lag_smooth_vals, &deriv_t_ms, ingest_stop_ms);
    m.insert(
        "mean_d_lag_d_t_wal_sequences_per_sec_ingest".to_string(),
        json!(mean_slice(&ing_d)),
    );
    m.insert(
        "mean_d_lag_d_t_wal_sequences_per_sec_drain".to_string(),
        json!(mean_slice(&drain_d)),
    );
    m.insert(
        "lag_derivative_p99_abs_ingest".to_string(),
        json!(percentile_of(
            ing_d.iter().map(|x| x.abs()).collect::<Vec<_>>(),
            0.99
        )),
    );
    m.insert(
        "lag_derivative_p99_abs_drain".to_string(),
        json!(percentile_of(
            drain_d.iter().map(|x| x.abs()).collect::<Vec<_>>(),
            0.99
        )),
    );
    m.insert("materialize_time_series".to_string(), json!(mat_events));
    m.insert(
        "mat_nonzero_tick_mean_records_per_sec".to_string(),
        json!(mat_rps_mean),
    );
    m.insert("lag_samples_count".to_string(), json!(lags.len()));
    m.insert("lag_min".to_string(), json!(lag_min));
    m.insert("lag_max".to_string(), json!(lag_max));
    m.insert("lag_mean".to_string(), json!(lag_mean));
    m.insert(
        "rows_ingested_end".to_string(),
        json!(next_id.load(Ordering::Relaxed).saturating_sub(1)),
    );
    m.insert("lag_sample_ms".to_string(), json!(args.lag_sample_ms));
    m.insert("trace_bucket_ms".to_string(), json!(args.trace_bucket_ms));
    m.insert("materializer_batch".to_string(), json!(args.materializer_batch));
    m.insert(
        "materializer_max_batches_per_tick".to_string(),
        json!(args.materializer_max_batches_per_tick),
    );
    m.insert("burst_period_sec".to_string(), json!(args.burst_period_sec));
    m.insert("burst_size".to_string(), json!(args.burst_size));
    m.insert("burst_inject_ms".to_string(), json!(args.burst_inject_ms));
    let burst_ts: Vec<f64> = burst_times
        .lock()
        .map_err(|e| e.to_string())?
        .clone();
    m.insert("burst_timestamps_bench_ms".to_string(), json!(burst_ts));
    m.insert(
        "burst_impulse_events".to_string(),
        json!(analyze_burst_impulse_responses(
            &burst_ts,
            &lag_track,
            &trace_points,
            args
        )),
    );
    if let Some(tb) = build_time_bucketed_traces(
        args.trace_bucket_ms,
        &trace_points,
        mat_rps_mean,
    ) {
        m.insert(
            "time_bucketed_traces".to_string(),
            serde_json::to_value(&tb).map_err(|e| e.to_string())?,
        );
    }
    if let Some(cap) = args.stop_after_raw_rows {
        m.insert("stop_after_raw_rows".to_string(), json!(cap));
    }
    m.insert("ingest_stopped".to_string(), json!(ingest_done.load(Ordering::Relaxed)));
    m.insert("ingest_stop_t_ms".to_string(), json!(ingest_stop_ms));
    Ok(m)
}

fn scenario_c(args: &Args) -> Result<ScenarioOut, String> {
    let m = scenario_mixed_workload(args)?;
    Ok(ScenarioOut {
        scenario: "C_mixed".to_string(),
        payload: m,
    })
}

fn matrix_comparison_row(target_raw_rows: u64, m: &HashMap<String, Value>) -> Value {
    let pick = |k: &str| m.get(k).cloned().unwrap_or(Value::Null);
    json!({
        "target_raw_rows": target_raw_rows,
        "rows_ingested_end": pick("rows_ingested_end"),
        "lag_max": pick("lag_max"),
        "lag_mean": pick("lag_mean"),
        "mean_d_lag_d_t_wal_sequences_per_sec": pick("mean_d_lag_d_t_wal_sequences_per_sec"),
        "mat_nonzero_tick_mean_records_per_sec": pick("mat_nonzero_tick_mean_records_per_sec"),
        "query_work_ms_p50": pick("query_work_ms_p50"),
        "query_work_ms_p99": pick("query_work_ms_p99"),
        "text_search_work_ms_p50": pick("text_search_work_ms_p50"),
        "text_search_work_ms_p99": pick("text_search_work_ms_p99"),
        "text_search_path_samples": pick("text_search_path_samples"),
        "end_to_end_client_ms_p99": pick("end_to_end_client_ms_p99"),
        "end_to_end_client_ms_p99_ingest": pick("end_to_end_client_ms_p99_ingest"),
        "end_to_end_client_ms_p99_drain": pick("end_to_end_client_ms_p99_drain"),
        "lock_wait_ms_p99": pick("lock_wait_ms_p99"),
        "lock_wait_ms_p99_ingest": pick("lock_wait_ms_p99_ingest"),
        "lock_wait_ms_p99_drain": pick("lock_wait_ms_p99_drain"),
        "rt_lock_hold_ms_p99": pick("rt_lock_hold_ms_p99"),
        "rt_lock_hold_ms_p99_ingest": pick("rt_lock_hold_ms_p99_ingest"),
        "rt_lock_hold_ms_p99_drain": pick("rt_lock_hold_ms_p99_drain"),
        "lag_check_ms_p99": pick("lag_check_ms_p99"),
        "lag_p99_ingest": pick("lag_p99_ingest"),
        "lag_p99_drain": pick("lag_p99_drain"),
        "mean_d_lag_d_t_wal_sequences_per_sec_ingest": pick("mean_d_lag_d_t_wal_sequences_per_sec_ingest"),
        "mean_d_lag_d_t_wal_sequences_per_sec_drain": pick("mean_d_lag_d_t_wal_sequences_per_sec_drain"),
    })
}

/// Run **scenario C**-style tracking at each `--matrix-target-rows` cap (default 100k, 200k, 500k) for apples-to-apples write/read/search curves.
fn scenario_e(args: &Args) -> Result<ScenarioOut, String> {
    let targets = if args.matrix_target_rows.is_empty() {
        vec![100_000u64, 200_000, 500_000]
    } else {
        args.matrix_target_rows.clone()
    };
    let mut tiers: Vec<Value> = Vec::new();
    let mut comparison: Vec<Value> = Vec::new();
    for (idx, &r) in targets.iter().enumerate() {
        let sub = args
            .data_dir
            .join(format!("matrix_tier_{idx}_{r}raw"));
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).map_err(|e| e.to_string())?;
        let mut tier_args = args.clone();
        tier_args.data_dir = sub;
        tier_args.stop_after_raw_rows = Some(r);
        let m = scenario_mixed_workload(&tier_args)?;
        comparison.push(matrix_comparison_row(r, &m));
        tiers.push(json!({
            "tier_index": idx,
            "target_raw_rows": r,
            "tracking": m,
        }));
    }
    let mut payload: HashMap<String, Value> = HashMap::new();
    payload.insert(
        "pipeline".to_string(),
        json!("matrix_tracking: repeated mixed raw→mat→read+search workloads"),
    );
    payload.insert("matrix_target_rows".to_string(), json!(&targets));
    payload.insert("comparison".to_string(), json!(comparison));
    payload.insert("tiers".to_string(), json!(tiers));
    payload.insert(
        "note".to_string(),
        json!("Each tier uses its own data subdirectory. Ingest stops at target_raw_rows; sampling continues for full --duration-sec. Enable --search-enabled to populate search metrics."),
    );
    Ok(ScenarioOut {
        scenario: "E_matrix_tracking".to_string(),
        payload,
    })
}

/// Ingest-only steady load at each `equilibrium_pauses` value; mean **d(lag)/dt** shows where backlog
/// growth crosses zero (sustained ingest ≈ materialize) for this mat/ingest configuration.
fn scenario_d(args: &Args) -> Result<ScenarioOut, String> {
    let mut step_maps: Vec<HashMap<String, Value>> = Vec::new();
    let mut d_features: Vec<[f64; 4]> = Vec::new();
    let mut means: Vec<f64> = Vec::new();
    let mut pauses_out: Vec<u64> = Vec::new();

    for (step_idx, &ingest_pause_ms) in args.equilibrium_pauses.iter().enumerate() {
        let step_dir = args
            .data_dir
            .join(format!("equilibrium_step_{step_idx}_{ingest_pause_ms}ms"));
        let _ = std::fs::remove_dir_all(&step_dir);
        std::fs::create_dir_all(&step_dir).map_err(|e| e.to_string())?;

        let t0 = Instant::now();
        let rare_mod = rare_doc_mod_from_args(args);
        let postings: Arc<Mutex<PostingsMap>> = Arc::new(Mutex::new(HashMap::new()));
        let rt = Arc::new(Mutex::new(EngineRuntime::open(
            &step_dir,
            args.mmap_bytes,
        )));
        let stop = Arc::new(AtomicBool::new(false));
        let mat_log = Arc::new(Mutex::new(Vec::<MatEvent>::new()));
        start_background_materializer(
            rt.clone(),
            stop.clone(),
            step_dir.clone(),
            args.mat_interval_ms,
            args.mat_idle_ms,
            args.materializer_batch,
            args.materializer_max_batches_per_tick,
            mat_log.clone(),
            t0,
        );

        let next_id = Arc::new(AtomicU64::new(1));
        let ni = next_id.clone();
        let st = stop.clone();
        let b = args.ingest_batch;
        let ib = rt.clone();
        let postings_ing = postings.clone();
        let search_en_ing = args.search_enabled;
        let _ing = thread::spawn(move || {
            while !st.load(Ordering::Relaxed) {
                let start = ni.load(Ordering::Relaxed);
                {
                    let mut g = match ib.lock() {
                        Ok(x) => x,
                        Err(_) => break,
                    };
                    if g
                        .execute_raw_segment_bulk_insert(COLL, make_batch(start, b, rare_mod))
                        .is_err()
                    {
                        break;
                    }
                }
                ni.fetch_add(b as u64, Ordering::Relaxed);
                if search_en_ing {
                    register_row_range(&postings_ing, start, b, rare_mod);
                }
                thread::sleep(Duration::from_millis(ingest_pause_ms));
            }
        });

        let t_end = t0 + Duration::from_secs(args.equilibrium_step_sec);
        let mut samples: Vec<SustainedSample> = Vec::new();
        let mut max_lag: u64 = 0;
        let search_period_ms = 1000.0 / args.search_qps.max(1e-9);
        let mut last_search_ms = -1.0e9_f64;
        let mut search_fire_count: u64 = 0;
        let mut text_search_samples: Vec<f64> = Vec::new();
        while Instant::now() < t_end {
            let t_ms = t0.elapsed().as_secs_f64() * 1000.0;
            let mut ids_op: Option<Vec<u64>> = None;
            if args.search_enabled {
                let n0 = next_id.load(Ordering::Relaxed).saturating_sub(1);
                if n0 >= 1 && (t_ms - last_search_ms) >= search_period_ms {
                    let term = pick_search_term(search_fire_count, args);
                    search_fire_count = search_fire_count.saturating_add(1);
                    ids_op = postings
                        .lock()
                        .expect("postings")
                        .get(term)
                        .map(|q| {
                            q.iter()
                                .rev()
                                .take(args.search_scan)
                                .copied()
                                .collect::<Vec<_>>()
                        });
                    last_search_ms = t_ms;
                }
            }
            if let Ok(mut g) = rt.lock() {
                let n = next_id.load(Ordering::Relaxed).saturating_sub(1);
                let lag = g.raw_journal_pending_sequences(COLL).unwrap_or(0);
                if let Some(ref ids) = ids_op {
                    if ids.is_empty() {
                        text_search_samples.push(0.0);
                    } else if let Ok(ms) = search_posting_scan_ms(&mut g, ids) {
                        text_search_samples.push(ms);
                    }
                }
                max_lag = max_lag.max(lag);
                samples.push(SustainedSample {
                    t_ms,
                    total_raw_rows: n,
                    pending_wal_sequences: lag,
                });
            }
            thread::sleep(Duration::from_millis(args.lag_sample_ms.max(5)));
        }

        stop.store(true, Ordering::Relaxed);
        let _ = _ing.join();
        thread::sleep(Duration::from_millis(200));

        let final_lag = {
            let mut g = rt.lock().map_err(|e| e.to_string())?;
            g.raw_journal_pending_sequences(COLL).unwrap_or(0)
        };
        let rows_ingested = next_id.load(Ordering::Relaxed).saturating_sub(1);

        let lag_deriv = lag_derivative_series(&samples);
        let d_lag_vals: Vec<f64> = lag_deriv
            .iter()
            .map(|d| d.d_lag_d_t_wal_sequences_per_sec)
            .collect();
        let lag_deriv_smooth =
            smooth_lag_deriv_trailing(&lag_deriv, args.lag_deriv_smooth_window.max(1));
        let d_lag_smooth_vals: Vec<f64> = lag_deriv_smooth
            .iter()
            .map(|d| d.d_lag_d_t_wal_sequences_per_sec)
            .collect();
        let mean_d = mean_slice(&d_lag_smooth_vals);
        let mean_d_raw = mean_slice(&d_lag_vals);

        let mat_events: Vec<MatEvent> = mat_log
            .lock()
            .map_err(|e| e.to_string())?
            .clone();
        let mat_rps: Vec<f64> = mat_events
            .iter()
            .filter_map(|e| e.mat_records_per_sec)
            .collect();
        let mat_mean = mean_slice(&mat_rps);
        let ingest_rps =
            rows_ingested as f64 / (args.equilibrium_step_sec.max(1) as f64);
        let regime_v0 = classify_regime_d(mean_d, max_lag, final_lag, mat_mean);
        let queue_pressure_wal_ms = final_lag as f64 / mat_mean.max(1.0) * 1000.0;
        let queue_pressure_visibility_proxy_ms = mean_d.max(0.0)
            * (final_lag as f64 / (mat_mean.max(1.0) + 500.0))
            * 0.05
            + (max_lag.saturating_sub(final_lag) as f64) * 3.0;
        let lag_accel_d = lag_acceleration_series(&lag_deriv_smooth);

        let mut step: HashMap<String, Value> = HashMap::new();
        step.insert("ingest_pause_ms".to_string(), json!(ingest_pause_ms));
        step.insert("step_index".to_string(), json!(step_idx));
        step.insert(
            "step_wall_sec".to_string(),
            json!(args.equilibrium_step_sec),
        );
        step.insert("lag_sample_count".to_string(), json!(samples.len()));
        step.insert("max_lag_pending_wal".to_string(), json!(max_lag));
        step.insert("final_lag_pending_wal".to_string(), json!(final_lag));
        step.insert("rows_ingested".to_string(), json!(rows_ingested));
        step.insert(
            "mean_d_lag_d_t_wal_sequences_per_sec".to_string(),
            json!(mean_d),
        );
        step.insert(
            "mean_d_lag_d_t_wal_sequences_per_sec_raw".to_string(),
            json!(mean_d_raw),
        );
        step.insert("lag_derivative".to_string(), json!(lag_deriv));
        step.insert(
            "lag_derivative_smoothed".to_string(),
            json!(lag_deriv_smooth),
        );
        step.insert("lag_acceleration".to_string(), json!(lag_accel_d));
        step.insert("materialize_time_series".to_string(), json!(mat_events));
        step.insert(
            "mat_nonzero_tick_mean_records_per_sec".to_string(),
            json!(mat_mean),
        );
        step.insert("lag_sample_ms".to_string(), json!(args.lag_sample_ms));
        step.insert("ingest_rows_per_sec".to_string(), json!(ingest_rps));
        step.insert("system_regime_v0".to_string(), json!(regime_v0));
        step.insert(
            "queue_pressure_wal_ms".to_string(),
            json!(queue_pressure_wal_ms),
        );
        step.insert(
            "queue_pressure_visibility_proxy_ms".to_string(),
            json!(queue_pressure_visibility_proxy_ms),
        );
        let ts_mean = mean_slice(&text_search_samples);
        let ts_p50 = percentile_of(text_search_samples.clone(), 0.5);
        let ts_p95 = percentile_of(text_search_samples.clone(), 0.95);
        let ts_p99 = percentile_of(text_search_samples.clone(), 0.99);
        step.insert(
            "text_search_sample_count".to_string(),
            json!(text_search_samples.len()),
        );
        step.insert("text_search_work_ms_mean".to_string(), json!(ts_mean));
        step.insert("text_search_work_ms_p50".to_string(), json!(ts_p50));
        step.insert("text_search_work_ms_p95".to_string(), json!(ts_p95));
        step.insert("text_search_work_ms_p99".to_string(), json!(ts_p99));

        d_features.push([ingest_rps, mean_d, max_lag as f64, mat_mean]);
        means.push(mean_d);
        pauses_out.push(ingest_pause_ms);
        step_maps.push(step);
    }

    let mut phase_classifier = "d_step_v0_fallback".to_string();
    let mut regime_calibration_d: Option<RegimeCalibrationMeta> = None;
    if d_features.len() >= 2 {
        let n = d_features.len();
        let k = n.min(4).max(2);
        let (mu, sig) = column_z_params(&d_features);
        let rows_z: Vec<[f64; 4]> = d_features.iter().map(|r| to_z_row(r, &mu, &sig)).collect();
        let (centroids_z, labels) = kmeans_z_lloyd(&rows_z, k, 40);
        let (cluster_to_regime, cluster_severity_score) =
            regime_labels_from_clusters(k, &labels, &d_features, n);
        for i in 0..n {
            step_maps[i].insert(
                "system_regime".to_string(),
                json!(cluster_to_regime[labels[i]].clone()),
            );
            step_maps[i].insert("regime_cluster_id".to_string(), json!(labels[i]));
        }
        regime_calibration_d = Some(RegimeCalibrationMeta {
            method: "kmeans_lloyd_zscore",
            k,
            feature_names: [
                "ingest_rows_per_sec",
                "mean_d_lag_d_t_wal_sequences_per_sec",
                "max_lag_pending_wal",
                "mat_nonzero_tick_mean_records_per_sec",
            ],
            centroids_z: centroids_z.iter().map(|c| c.to_vec()).collect(),
            cluster_to_regime: cluster_to_regime.clone(),
            cluster_severity_score,
        });
        phase_classifier = format!("d_step_kmeans_k{k}_zscore");
    } else {
        for sm in &mut step_maps {
            if let Some(v0) = sm.get("system_regime_v0").cloned() {
                sm.insert("system_regime".to_string(), v0);
            }
        }
    }

    for sm in &mut step_maps {
        let ingest_rps = sm
            .get("ingest_rows_per_sec")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let mean_dv = sm
            .get("mean_d_lag_d_t_wal_sequences_per_sec")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let max_l = sm
            .get("max_lag_pending_wal")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let fin = sm
            .get("final_lag_pending_wal")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let matm = sm
            .get("mat_nonzero_tick_mean_records_per_sec")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let regime_s = sm
            .get("system_regime")
            .and_then(|v| v.as_str())
            .unwrap_or("stable")
            .to_string();
        let q_wal = sm
            .get("queue_pressure_wal_ms")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let q_vis = sm
            .get("queue_pressure_visibility_proxy_ms")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let mean_accel = sm
            .get("lag_acceleration")
            .and_then(|v| v.as_array())
            .map(|a| {
                let vals: Vec<f64> = a
                    .iter()
                    .filter_map(|x| x.get("d2_lag_dt2_wal_per_s2").and_then(|y| y.as_f64()))
                    .collect();
                mean_slice(&vals)
            })
            .unwrap_or(0.0);
        let inst = (max_l as f64 - fin as f64).max(0.0);
        let text_search_mean = sm
            .get("text_search_work_ms_mean")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        sm.insert(
            "state_vector".to_string(),
            json!({
                "lock_pressure": ingest_rps * 1e-3,
                "execution_pressure": matm,
                "lag_mean": fin as f64,
                "lag_velocity_wal_per_s": mean_dv,
                "lag_acceleration_wal_per_s2_mean_sampled": mean_accel,
                "instability_index": inst,
                "queue_pressure_wal_ms": q_wal,
                "queue_pressure_visibility_proxy_ms": q_vis,
                "text_search_work_ms_mean": text_search_mean,
                "regime": regime_s,
            }),
        );
    }

    let sweep: Vec<Value> = step_maps.iter().map(|s| json!(s)).collect();
    let phase_points: Vec<Value> = step_maps
        .iter()
        .map(|s| {
            json!({
                "ingest_pause_ms": s.get("ingest_pause_ms"),
                "ingest_rows_per_sec": s.get("ingest_rows_per_sec"),
                "mean_d_lag_d_t_wal_sequences_per_sec": s.get("mean_d_lag_d_t_wal_sequences_per_sec"),
                "max_lag_pending_wal": s.get("max_lag_pending_wal"),
                "final_lag_pending_wal": s.get("final_lag_pending_wal"),
                "mat_nonzero_tick_mean_records_per_sec": s.get("mat_nonzero_tick_mean_records_per_sec"),
                "queue_pressure_wal_ms": s.get("queue_pressure_wal_ms"),
                "queue_pressure_visibility_proxy_ms": s.get("queue_pressure_visibility_proxy_ms"),
                "text_search_work_ms_mean": s.get("text_search_work_ms_mean"),
                "text_search_sample_count": s.get("text_search_sample_count"),
                "system_regime_v0": s.get("system_regime_v0"),
                "system_regime": s.get("system_regime"),
                "regime_cluster_id": s.get("regime_cluster_id"),
                "state_vector": s.get("state_vector"),
            })
        })
        .collect();

    let mut crossing_note = String::new();
    for i in 0..means.len().saturating_sub(1) {
        let a = means[i];
        let b = means[i + 1];
        if a * b < 0.0 {
            crossing_note = format!(
                "mean d(lag)/dt crosses zero between ingest_pause_ms={} and {} ({} → {} wal_seq/s)",
                pauses_out[i], pauses_out[i + 1], a, b
            );
            break;
        }
    }
    if crossing_note.is_empty() && !means.is_empty() {
        crossing_note = "no sign change in mean d(lag)/dt across sweep; widen --equilibrium-pauses or increase --equilibrium-step-sec".to_string();
    }

    let by_pause: Vec<serde_json::Value> = means
        .iter()
        .zip(pauses_out.iter())
        .map(|(m, p)| {
            json!({
                "ingest_pause_ms": p,
                "mean_d_lag_d_t_wal_sequences_per_sec": m
            })
        })
        .collect();

    let mut m: HashMap<String, serde_json::Value> = HashMap::new();
    m.insert(
        "pipeline".to_string(),
        json!("raw_segment -> materialize (ingest-only equilibrium sweep)"),
    );
    m.insert(
        "equilibrium_pauses_ms".to_string(),
        json!(&args.equilibrium_pauses),
    );
    m.insert(
        "equilibrium_step_sec".to_string(),
        json!(args.equilibrium_step_sec),
    );
    m.insert("ingest_batch".to_string(), json!(args.ingest_batch));
    m.insert("mat_interval_ms".to_string(), json!(args.mat_interval_ms));
    m.insert("mat_idle_ms".to_string(), json!(args.mat_idle_ms));
    m.insert("materializer_batch".to_string(), json!(args.materializer_batch));
    m.insert(
        "materializer_max_batches_per_tick".to_string(),
        json!(args.materializer_max_batches_per_tick),
    );
    m.insert("lag_sample_ms".to_string(), json!(args.lag_sample_ms));
    m.insert("search_enabled".to_string(), json!(args.search_enabled));
    m.insert("search_qps".to_string(), json!(args.search_qps));
    m.insert("search_scan".to_string(), json!(args.search_scan));
    m.insert(
        "search_selectivity".to_string(),
        json!(match args.search_selectivity {
            SearchSelectivity::Common => "common",
            SearchSelectivity::Rare => "rare",
            SearchSelectivity::Mixed => "mixed",
        }),
    );
    m.insert(
        "search_rare_doc_mod".to_string(),
        json!(args.search_rare_doc_mod),
    );
    m.insert(
        "search_mixed_rare_query_pct".to_string(),
        json!(args.search_mixed_rare_query_pct),
    );
    m.insert(
        "lag_deriv_smooth_window".to_string(),
        json!(args.lag_deriv_smooth_window.max(1)),
    );
    m.insert("recovery_rel_frac".to_string(), json!(args.recovery_rel_frac));
    m.insert(
        "recovery_abs_floor_wal".to_string(),
        json!(args.recovery_abs_floor_wal),
    );
    m.insert(
        "recovery_eps_wal_per_s".to_string(),
        json!(args.recovery_eps_wal_per_s),
    );
    m.insert(
        "recovery_stable_hold_ms".to_string(),
        json!(args.recovery_stable_hold_ms),
    );
    m.insert("equilibrium_sweep".to_string(), json!(sweep));
    m.insert("mean_d_lag_d_t_by_pause".to_string(), json!(by_pause));
    m.insert("equilibrium_crossing_hint".to_string(), json!(crossing_note));
    m.insert(
        "phase_diagram".to_string(),
        json!({
            "regime_classifier": phase_classifier,
            "axes": { "x": "ingest_pause_ms", "y": "ingest_rows_per_sec", "label": "system_regime" },
            "regime_calibration": regime_calibration_d,
            "points": phase_points
        }),
    );

    Ok(ScenarioOut {
        scenario: "D_equilibrium".to_string(),
        payload: m,
    })
}
