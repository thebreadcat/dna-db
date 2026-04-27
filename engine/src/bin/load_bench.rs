use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Read;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use dnadb_engine::codec::{BincodeStrandCodec, StrandCodec, STRAND_FORMAT_MAGIC};
use dnadb_engine::encoding::encode_bytes_to_codons;
use dnadb_engine::group_commit::{GroupCommit, GroupCommitPolicy};
use dnadb_engine::model::Strand;
use dnadb_engine::processor::{encode_complement_blob, strand_from_wal_payload};
use dnadb_engine::query::fetch::fetch_one;
use dnadb_engine::query::{
    project_rows, should_skip_block, should_skip_block_bloom_only, should_skip_segment,
    should_skip_segment_bloom_only, Clause, GlobalIndexCatalog, GuidePattern, ScanConfig,
    SegmentBlockMeta, SegmentMeta,
};
use dnadb_engine::storage::CollectionStorage;
use dnadb_engine::wal::Wal;
use rayon::prelude::*;
use rayon::ThreadPool;
use rayon::ThreadPoolBuilder;
use serde::Serialize;

struct ShardedWal {
    shards: Vec<Mutex<Wal>>,
    next_sequence: AtomicU64,
}

impl ShardedWal {
    fn open_or_create(root: &std::path::Path, collection: &str, shard_count: usize) -> Result<Self, String> {
        let mut shards = Vec::with_capacity(shard_count.max(1));
        let mut next = 1u64;
        for shard_idx in 0..shard_count.max(1) {
            let shard_name = format!("{collection}_shard_{shard_idx:02}");
            let wal =
                Wal::open_or_create(root, &shard_name).map_err(|e| format!("open shard wal: {e}"))?;
            next = next.max(wal.next_sequence());
            shards.push(Mutex::new(wal));
        }
        Ok(Self {
            shards,
            next_sequence: AtomicU64::new(next),
        })
    }

    fn append_to_shard(&self, shard_idx: usize, payload: &[u8]) -> Result<u64, String> {
        let seq = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        let shard = shard_idx % self.shards.len().max(1);
        let mut wal = self.shards[shard]
            .lock()
            .map_err(|e| format!("sharded wal lock: {e}"))?;
        wal.append(payload)
            .map_err(|e| format!("sharded wal append: {e}"))?;
        Ok(seq)
    }

    fn sync_all(&self) -> Result<(), String> {
        for shard in &self.shards {
            let mut wal = shard
                .lock()
                .map_err(|e| format!("sharded wal lock sync: {e}"))?;
            wal.sync().map_err(|e| format!("sharded wal sync: {e}"))?;
        }
        Ok(())
    }

    fn sync_shard(&self, shard_idx: usize) -> Result<(), String> {
        let shard = shard_idx % self.shards.len().max(1);
        let mut wal = self.shards[shard]
            .lock()
            .map_err(|e| format!("sharded wal lock sync shard: {e}"))?;
        wal.sync()
            .map_err(|e| format!("sharded wal sync shard: {e}"))?;
        Ok(())
    }

    fn read_all_entries_len(&self) -> Result<usize, String> {
        let mut total = 0usize;
        for shard in &self.shards {
            let mut wal = shard
                .lock()
                .map_err(|e| format!("sharded wal lock replay: {e}"))?;
            total += wal
                .read_all_entries()
                .map_err(|e| format!("sharded wal replay: {e}"))?
                .len();
        }
        Ok(total)
    }
}

#[derive(Debug, Clone)]
struct Config {
    records: usize,
    read_sample: usize,
    data_dir: PathBuf,
    mmap_bytes: usize,
    mode: BenchMode,
    batch_size: usize,
    /// Parallel encoder worker count for ingestion (1 = single-threaded prep path).
    threads: usize,
    /// Number of records prepared per chunk before ordered WAL/materialization commit.
    prep_batch: usize,
    /// Use true concurrent writer threads (each thread appends + materializes its own records).
    /// WAL/storage locks are taken once per `--prep-batch` records per thread (not per record).
    concurrent_writers: bool,
    /// Number of WAL shards for concurrent-writer mode. `1` = legacy single WAL mutex.
    wal_shards: usize,
    /// If set, milliseconds between WAL+storage fsync groups. If unset, mode defaults apply
    /// (`strict` 10ms, `balanced` 50ms, `fast` unused). `Some(0)` disables the timer (batch only).
    wal_interval_ms: Option<u64>,
    /// If true, perform durability syncs on a background timer instead of blocking write path.
    async_sync: bool,
    /// Max staleness interval for async sync worker when `--async-sync` is enabled.
    async_sync_interval_ms: u64,
    /// Async WAL sync interval when `--async-sync` is enabled.
    async_sync_wal_interval_ms: u64,
    /// Async storage sync interval when `--async-sync` is enabled.
    async_sync_storage_interval_ms: u64,
    /// Record threshold to trigger background durability sync when `--async-sync` is enabled.
    async_sync_max_records: u64,
    phase: BenchPhase,
    /// When true, skip building a full `Vec<Strand>` in memory; stream the strand file once,
    /// decode each frame, project `_payload` only, and report that time as `decode_seconds`.
    projection_pipeline_only: bool,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum BenchMode {
    Strict,
    Balanced,
    Fast,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum BenchPhase {
    Full,
    Ingest,
    Verify,
    Query,
}

impl BenchPhase {
    fn parse(s: &str) -> Result<Self, String> {
        match s {
            "full" => Ok(Self::Full),
            "ingest" => Ok(Self::Ingest),
            "verify" => Ok(Self::Verify),
            "query" => Ok(Self::Query),
            _ => Err(format!("invalid --phase `{s}` (expected full|ingest|verify|query)")),
        }
    }
}

impl BenchMode {
    fn parse(s: &str) -> Result<Self, String> {
        match s {
            "strict" => Ok(Self::Strict),
            "balanced" => Ok(Self::Balanced),
            "fast" => Ok(Self::Fast),
            _ => Err(format!(
                "invalid --mode `{s}` (expected strict|balanced|fast)"
            )),
        }
    }

    /// Harness defaults: **batch-only** group commit so slow disks (e.g. network sync folders)
    /// are not forced into per-record fsync by a short timer. Use `--wal-interval-ms 10` on
    /// fast local storage for a PostgreSQL-style max staleness bound.
    fn default_wal_interval_ms(self) -> u64 {
        match self {
            Self::Strict | Self::Balanced | Self::Fast => 0,
        }
    }
}

#[derive(Debug, Serialize)]
struct BenchReport {
    phase: BenchPhase,
    mode: BenchMode,
    batch_size: usize,
    threads: usize,
    prep_batch: usize,
    concurrent_writers: bool,
    wal_shards: usize,
    /// Effective WAL group-commit interval (ms); 0 means timer disabled (batch-only triggers).
    wal_interval_ms: u64,
    records: usize,
    write_seconds: f64,
    write_records_per_sec: f64,
    decode_seconds: f64,
    decode_records_per_sec: f64,
    projected_decode_seconds: f64,
    projected_rows: usize,
    point_read_sample: usize,
    point_read_seconds: f64,
    point_reads_per_sec: f64,
    query_seconds: f64,
    query_found: bool,
    full_scan_seconds: f64,
    full_scan_hits: usize,
    /// Full parallel scan with an exact `_payload` match that never hits (still visits every strand).
    negative_exact_scan_seconds: f64,
    negative_exact_scan_hits: usize,
    /// `query_seconds * 1e6 / records` — comparable across 100k vs 1M (lower is better).
    query_s_per_million_strands: f64,
    full_scan_s_per_million_strands: f64,
    negative_exact_s_per_million_strands: f64,
    decode_s_per_million_strands: f64,
    /// True when `--projection-pipeline-only` ran the streaming read path (no full vector materialization).
    projection_pipeline_only: bool,
    lock_wait_ms_total: f64,
    lock_hold_ms_total: f64,
    sync_events: u64,
    segment_count: usize,
    block_count: usize,
    segment_dictionary_entries: usize,
    block_dictionary_entries: usize,
    segment_dictionary_coverage_pct: f64,
    block_dictionary_coverage_pct: f64,
    query_skipped_segments: usize,
    query_skipped_blocks: usize,
    query_skipped_segments_dict_extra: usize,
    query_skipped_blocks_dict_extra: usize,
    full_scan_skipped_segments: usize,
    full_scan_skipped_blocks: usize,
    full_scan_skipped_segments_dict_extra: usize,
    full_scan_skipped_blocks_dict_extra: usize,
    negative_exact_skipped_segments: usize,
    negative_exact_skipped_blocks: usize,
    negative_exact_skipped_segments_dict_extra: usize,
    negative_exact_skipped_blocks_dict_extra: usize,
    wal_entries: usize,
    decoded_strands: usize,
    ingest_prep_seconds: f64,
    ingest_encode_seconds: f64,
    ingest_wal_append_seconds: f64,
    ingest_storage_append_seconds: f64,
    ingest_storage_sync_seconds: f64,
    ingest_wal_sync_seconds: f64,
    ingest_wal_appends: u64,
    ingest_fsync_events: u64,
    storage_strands_grow_events: u64,
    storage_complement_grow_events: u64,
    storage_meta_grow_events: u64,
    storage_total_grow_events: u64,
    strands_map_capacity_bytes: usize,
    complement_map_capacity_bytes: usize,
    meta_map_capacity_bytes: usize,
    async_sync: bool,
    async_sync_interval_ms: u64,
    async_wal_sync_events: u64,
    async_storage_sync_events: u64,
    async_sync_max_records: u64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = parse_args()?;
    fs::create_dir_all(&cfg.data_dir)?;

    let wal_interval_ms = cfg
        .wal_interval_ms
        .unwrap_or_else(|| cfg.mode.default_wal_interval_ms());

    let collection_name = "bench";
    let collection_id = 1u32;
    let codec = BincodeStrandCodec;

    let wal = Wal::open_or_create(&cfg.data_dir, collection_name)?;
    let storage =
        CollectionStorage::open_or_create(&cfg.data_dir, collection_name, Some(cfg.mmap_bytes))?;

    let max_interval = match cfg.mode {
        BenchMode::Fast => None,
        BenchMode::Strict | BenchMode::Balanced => {
            if wal_interval_ms == 0 {
                None
            } else {
                Some(Duration::from_millis(wal_interval_ms))
            }
        }
    };
    let group_commit_opt = match cfg.mode {
        BenchMode::Strict | BenchMode::Balanced => Some(GroupCommit::new(GroupCommitPolicy {
            max_batch: cfg.batch_size.max(1),
            max_interval,
        })),
        BenchMode::Fast => None,
    };

    let encode_pool = if cfg.threads > 1 {
        Some(ThreadPoolBuilder::new().num_threads(cfg.threads).build()?)
    } else {
        None
    };

    let lock_wait_ns = Arc::new(AtomicU64::new(0));
    let lock_hold_ns = Arc::new(AtomicU64::new(0));
    let sync_events = Arc::new(AtomicU64::new(0));
    let async_wal_sync_events = Arc::new(AtomicU64::new(0));
    let async_storage_sync_events = Arc::new(AtomicU64::new(0));
    let prep_ns = Arc::new(AtomicU64::new(0));
    let encode_ns = Arc::new(AtomicU64::new(0));
    let wal_append_ns = Arc::new(AtomicU64::new(0));
    let storage_append_ns = Arc::new(AtomicU64::new(0));
    let storage_sync_ns = Arc::new(AtomicU64::new(0));
    let wal_sync_ns = Arc::new(AtomicU64::new(0));
    let wal_appends = Arc::new(AtomicU64::new(0));
    let wal = Arc::new(Mutex::new(wal));
    let use_sharded_wal = cfg.concurrent_writers && cfg.threads > 1 && cfg.wal_shards > 1;
    let sharded_wal = if use_sharded_wal {
        Some(Arc::new(ShardedWal::open_or_create(
            &cfg.data_dir,
            collection_name,
            cfg.wal_shards,
        )?))
    } else {
        None
    };
    let storage = Arc::new(Mutex::new(storage));
    let group_commit = Arc::new(Mutex::new(group_commit_opt));
    let async_pending_records = Arc::new(AtomicU64::new(0));
    let async_stop = Arc::new(AtomicBool::new(false));

    let run_write = matches!(cfg.phase, BenchPhase::Full | BenchPhase::Ingest);
    let run_projection_stream = cfg.projection_pipeline_only
        && matches!(
            cfg.phase,
            BenchPhase::Full | BenchPhase::Verify | BenchPhase::Query
        );
    let run_full_vector_decode =
        matches!(cfg.phase, BenchPhase::Full | BenchPhase::Verify | BenchPhase::Query)
            && !cfg.projection_pipeline_only;
    let run_query_suite =
        matches!(cfg.phase, BenchPhase::Full | BenchPhase::Query) && !cfg.projection_pipeline_only;

    let mut async_sync_worker = None;
    if run_write && cfg.async_sync {
        let storage = Arc::clone(&storage);
        let wal = Arc::clone(&wal);
        let sharded_wal = sharded_wal.as_ref().map(Arc::clone);
        let async_pending_records = Arc::clone(&async_pending_records);
        let async_stop = Arc::clone(&async_stop);
        let sync_events = Arc::clone(&sync_events);
        let async_wal_sync_events = Arc::clone(&async_wal_sync_events);
        let async_storage_sync_events = Arc::clone(&async_storage_sync_events);
        let storage_sync_ns = Arc::clone(&storage_sync_ns);
        let wal_sync_ns = Arc::clone(&wal_sync_ns);
        let wal_interval = Duration::from_millis(
            cfg.async_sync_wal_interval_ms
                .max(cfg.async_sync_interval_ms)
                .max(1),
        );
        let storage_interval = Duration::from_millis(
            cfg.async_sync_storage_interval_ms
                .max(cfg.async_sync_interval_ms)
                .max(1),
        );
        let max_records = cfg.async_sync_max_records.max(1);
        async_sync_worker = Some(thread::spawn(move || -> Result<(), String> {
            let mut last_wal_sync = Instant::now();
            let mut last_storage_sync = Instant::now();
            while !async_stop.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_millis(1));
                let pending = async_pending_records.load(Ordering::Relaxed);
                if pending == 0 {
                    continue;
                }
                let due_to_count = pending >= max_records;
                let due_wal = due_to_count || last_wal_sync.elapsed() >= wal_interval;
                let due_storage = due_to_count || last_storage_sync.elapsed() >= storage_interval;
                if !(due_wal || due_storage) {
                    continue;
                }
                if due_storage {
                    let mut storage_guard = storage
                        .lock()
                        .map_err(|e| format!("async storage lock sync: {e}"))?;
                    let storage_sync_start = Instant::now();
                    storage_guard
                        .flush_maps()
                        .map_err(|e| format!("async flush maps: {e}"))?;
                    storage_guard
                        .sync_files()
                        .map_err(|e| format!("async sync files: {e}"))?;
                    storage_sync_ns.fetch_add(
                        storage_sync_start.elapsed().as_nanos() as u64,
                        Ordering::Relaxed,
                    );
                    drop(storage_guard);
                    async_storage_sync_events.fetch_add(1, Ordering::Relaxed);
                    last_storage_sync = Instant::now();
                }
                if due_wal {
                    let wal_sync_start = Instant::now();
                    if let Some(sw) = sharded_wal.as_ref() {
                        sw.sync_all().map_err(|e| format!("async sharded wal sync: {e}"))?;
                    } else {
                        let mut wal_guard =
                            wal.lock().map_err(|e| format!("async wal lock sync: {e}"))?;
                        wal_guard.sync().map_err(|e| format!("async wal sync: {e}"))?;
                    }
                    wal_sync_ns.fetch_add(
                        wal_sync_start.elapsed().as_nanos() as u64,
                        Ordering::Relaxed,
                    );
                    async_wal_sync_events.fetch_add(1, Ordering::Relaxed);
                    last_wal_sync = Instant::now();
                }
                sync_events.fetch_add(1, Ordering::Relaxed);
                if due_storage || due_to_count {
                    async_pending_records.store(0, Ordering::Relaxed);
                }
            }
            if async_pending_records.load(Ordering::Relaxed) > 0 {
                let mut storage_guard = storage
                    .lock()
                    .map_err(|e| format!("async final storage lock sync: {e}"))?;
                let storage_sync_start = Instant::now();
                storage_guard
                    .flush_maps()
                    .map_err(|e| format!("async final flush maps: {e}"))?;
                storage_guard
                    .sync_files()
                    .map_err(|e| format!("async final sync files: {e}"))?;
                storage_sync_ns.fetch_add(
                    storage_sync_start.elapsed().as_nanos() as u64,
                    Ordering::Relaxed,
                );
                drop(storage_guard);
                async_storage_sync_events.fetch_add(1, Ordering::Relaxed);
                let wal_sync_start = Instant::now();
                if let Some(sw) = sharded_wal.as_ref() {
                    sw.sync_all()
                        .map_err(|e| format!("async final sharded wal sync: {e}"))?;
                } else {
                    let mut wal_guard = wal
                        .lock()
                        .map_err(|e| format!("async final wal lock sync: {e}"))?;
                    wal_guard
                        .sync()
                        .map_err(|e| format!("async final wal sync: {e}"))?;
                }
                wal_sync_ns.fetch_add(
                    wal_sync_start.elapsed().as_nanos() as u64,
                    Ordering::Relaxed,
                );
                async_wal_sync_events.fetch_add(1, Ordering::Relaxed);
                sync_events.fetch_add(1, Ordering::Relaxed);
                async_pending_records.store(0, Ordering::Relaxed);
            }
            Ok(())
        }));
    }

    let write_start = Instant::now();
    if run_write && cfg.concurrent_writers && cfg.threads > 1 {
        let total_records = cfg.records;
        let writer_threads = cfg.threads;
        let mut handles = Vec::with_capacity(cfg.threads);
        for tid in 0..cfg.threads {
            let wal = Arc::clone(&wal);
            let sharded_wal = sharded_wal.as_ref().map(Arc::clone);
            let storage = Arc::clone(&storage);
            let group_commit = Arc::clone(&group_commit);
            let lock_wait_ns = Arc::clone(&lock_wait_ns);
            let lock_hold_ns = Arc::clone(&lock_hold_ns);
            let sync_events = Arc::clone(&sync_events);
            let encode_ns = Arc::clone(&encode_ns);
            let wal_append_ns = Arc::clone(&wal_append_ns);
            let storage_append_ns = Arc::clone(&storage_append_ns);
            let storage_sync_ns = Arc::clone(&storage_sync_ns);
            let wal_sync_ns = Arc::clone(&wal_sync_ns);
            let wal_appends = Arc::clone(&wal_appends);
            let async_pending_records = Arc::clone(&async_pending_records);
            let micro = cfg.prep_batch.max(1);
            let async_sync = cfg.async_sync;
            let handle = thread::spawn(move || -> Result<(), String> {
                // One WAL/storage/group-commit critical section per chunk (size `--prep-batch`)
                // instead of per record — cuts mutex churn across concurrent writer threads.
                let record_indices: Vec<usize> =
                    (tid..total_records).step_by(writer_threads).collect();
                for chunk in record_indices.chunks(micro) {
                    let shard = tid % writer_threads.max(1);
                    let wal_append_start = Instant::now();
                    let mut seq_payload = Vec::with_capacity(chunk.len());
                    for &record_idx in chunk {
                        let payload = make_payload(record_idx);
                        let seq = if let Some(sw) = sharded_wal.as_ref() {
                            sw.append_to_shard(shard, payload.as_bytes())?
                        } else {
                            let wait_start = Instant::now();
                            let mut wal_guard =
                                wal.lock().map_err(|e| format!("wal lock append: {e}"))?;
                            let waited = wait_start.elapsed();
                            let hold_start = Instant::now();
                            let seq = wal_guard.append(payload.as_bytes()).map_err(|e| e.to_string())?;
                            lock_wait_ns.fetch_add(waited.as_nanos() as u64, Ordering::Relaxed);
                            lock_hold_ns
                                .fetch_add(hold_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                            seq
                        };
                        seq_payload.push((seq, payload));
                    }
                    wal_append_ns.fetch_add(
                        wal_append_start.elapsed().as_nanos() as u64,
                        Ordering::Relaxed,
                    );
                    wal_appends.fetch_add(chunk.len() as u64, Ordering::Relaxed);

                    // Encode full chunk with no WAL or storage locks held.
                    let encode_start = Instant::now();
                    let mut encoded = Vec::with_capacity(chunk.len());
                    for (seq, payload) in seq_payload {
                        let strand =
                            strand_from_wal_payload(collection_id, seq, payload.as_bytes());
                        let strand_bytes = codec
                            .encode_strand(&strand)
                            .map_err(|e| format!("encode strand: {e}"))?;
                        let complement_blob = encode_complement_blob(&strand.complement)
                            .map_err(|e| format!("encode complement: {e}"))?;
                        encoded.push((seq, strand_bytes, complement_blob));
                    }
                    encode_ns.fetch_add(
                        encode_start.elapsed().as_nanos() as u64,
                        Ordering::Relaxed,
                    );

                    let wait_start = Instant::now();
                    let mut storage_guard =
                        storage.lock().map_err(|e| format!("storage lock: {e}"))?;
                    let waited = wait_start.elapsed();
                    let hold_start = Instant::now();
                    let storage_append_start = Instant::now();
                    for (seq, strand_bytes, complement_blob) in encoded {
                        storage_guard
                            .append_strands(&strand_bytes)
                            .map_err(|e| format!("append strands: {e}"))?;
                        storage_guard
                            .append_complement(&complement_blob)
                            .map_err(|e| format!("append complement: {e}"))?;
                        storage_guard.record_materialized_sequence(seq);
                    }
                    storage_append_ns.fetch_add(
                        storage_append_start.elapsed().as_nanos() as u64,
                        Ordering::Relaxed,
                    );
                    drop(storage_guard);
                    lock_wait_ns.fetch_add(waited.as_nanos() as u64, Ordering::Relaxed);
                    lock_hold_ns.fetch_add(hold_start.elapsed().as_nanos() as u64, Ordering::Relaxed);

                    if async_sync {
                        async_pending_records.fetch_add(chunk.len() as u64, Ordering::Relaxed);
                    } else {
                        let mut gc_guard = group_commit
                            .lock()
                            .map_err(|e| format!("group commit lock: {e}"))?;
                        let mut sync_now = false;
                        for _ in 0..chunk.len() {
                            if gc_guard
                                .as_mut()
                                .map(|g| g.after_record())
                                .unwrap_or(false)
                            {
                                sync_now = true;
                            }
                        }
                        drop(gc_guard);
                        if sync_now {
                            let mut storage_guard =
                                storage.lock().map_err(|e| format!("storage lock sync: {e}"))?;
                            let storage_sync_start = Instant::now();
                            storage_guard
                                .flush_maps()
                                .map_err(|e| format!("flush maps: {e}"))?;
                            storage_guard
                                .sync_files()
                                .map_err(|e| format!("sync files: {e}"))?;
                            storage_sync_ns.fetch_add(
                                storage_sync_start.elapsed().as_nanos() as u64,
                                Ordering::Relaxed,
                            );
                            drop(storage_guard);
                            let wal_sync_start = Instant::now();
                            if let Some(sw) = sharded_wal.as_ref() {
                                // In sharded mode, sync only the writer's shard for this commit tick.
                                // Full cross-shard sync remains at final checkpoint.
                                sw.sync_shard(shard)
                                    .map_err(|e| format!("sharded wal sync shard: {e}"))?;
                            } else {
                                let mut wal_guard =
                                    wal.lock().map_err(|e| format!("wal lock sync: {e}"))?;
                                wal_guard.sync().map_err(|e| format!("wal sync: {e}"))?;
                            }
                            wal_sync_ns.fetch_add(
                                wal_sync_start.elapsed().as_nanos() as u64,
                                Ordering::Relaxed,
                            );
                            sync_events.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                Ok(())
            });
            handles.push(handle);
        }
        for h in handles {
            h.join()
                .map_err(|e| format!("writer thread panicked: {e:?}"))?
                .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
        }
    } else if run_write {
        let chunk_size = cfg.prep_batch.max(1);
        let mut next_idx = 0usize;
        while next_idx < cfg.records {
            let chunk_end = (next_idx + chunk_size).min(cfg.records);
            let start_seq = {
                let wal = wal.lock().map_err(|e| format!("wal lock next sequence: {e}"))?;
                wal.next_sequence()
            };
            let prep_start = Instant::now();
            let prepared = prepare_records_chunk(
                next_idx,
                chunk_end,
                collection_id,
                start_seq,
                encode_pool.as_ref(),
                &codec,
            )
            .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
            prep_ns.fetch_add(prep_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
            for rec in prepared {
                let mut wal_guard = wal.lock().map_err(|e| format!("wal lock append: {e}"))?;
                let wal_append_start = Instant::now();
                let seq = wal_guard.append(rec.payload.as_bytes())?;
                wal_append_ns.fetch_add(
                    wal_append_start.elapsed().as_nanos() as u64,
                    Ordering::Relaxed,
                );
                wal_appends.fetch_add(1, Ordering::Relaxed);
                debug_assert_eq!(seq, rec.sequence);
                drop(wal_guard);

                let mut storage_guard = storage
                    .lock()
                    .map_err(|e| format!("storage lock append: {e}"))?;
                let storage_append_start = Instant::now();
                storage_guard.append_strands(&rec.strand_bytes)?;
                storage_guard.append_complement(&rec.complement_blob)?;
                storage_guard.record_materialized_sequence(rec.sequence);
                storage_append_ns.fetch_add(
                    storage_append_start.elapsed().as_nanos() as u64,
                    Ordering::Relaxed,
                );
                drop(storage_guard);

                if cfg.async_sync {
                    async_pending_records.fetch_add(1, Ordering::Relaxed);
                } else {
                    let mut gc_guard = group_commit
                        .lock()
                        .map_err(|e| format!("group commit lock: {e}"))?;
                    let sync_now = gc_guard
                        .as_mut()
                        .map(|g| g.after_record())
                        .unwrap_or(false);
                    drop(gc_guard);
                    if sync_now {
                        let mut storage_guard = storage
                            .lock()
                            .map_err(|e| format!("storage lock sync: {e}"))?;
                        let storage_sync_start = Instant::now();
                        storage_guard.flush_maps()?;
                        storage_guard.sync_files()?;
                        storage_sync_ns.fetch_add(
                            storage_sync_start.elapsed().as_nanos() as u64,
                            Ordering::Relaxed,
                        );
                        drop(storage_guard);
                        let mut wal_guard = wal.lock().map_err(|e| format!("wal lock sync: {e}"))?;
                        let wal_sync_start = Instant::now();
                        wal_guard.sync()?;
                        wal_sync_ns.fetch_add(
                            wal_sync_start.elapsed().as_nanos() as u64,
                            Ordering::Relaxed,
                        );
                        drop(wal_guard);
                        sync_events.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            next_idx = chunk_end;
        }
    }
    if run_write {
        if cfg.async_sync {
            async_stop.store(true, Ordering::Relaxed);
            if let Some(handle) = async_sync_worker.take() {
                handle
                    .join()
                    .map_err(|e| format!("async sync thread panicked: {e:?}"))?
                    .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
            }
        }
        {
            let mut storage = storage
                .lock()
                .map_err(|e| format!("storage lock final: {e}"))?;
            let storage_sync_start = Instant::now();
            storage.flush_maps()?;
            storage.sync_files()?;
            storage_sync_ns.fetch_add(
                storage_sync_start.elapsed().as_nanos() as u64,
                Ordering::Relaxed,
            );
        }
        {
            let wal_sync_start = Instant::now();
            if let Some(sw) = sharded_wal.as_ref() {
                sw.sync_all()
                    .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
            } else {
                let mut wal = wal.lock().map_err(|e| format!("wal lock final: {e}"))?;
                wal.sync()?;
            }
            wal_sync_ns.fetch_add(
                wal_sync_start.elapsed().as_nanos() as u64,
                Ordering::Relaxed,
            );
        }
    }
    let write_elapsed = if run_write {
        write_start.elapsed()
    } else {
        Duration::from_secs(0)
    };

    let wal_entries = if let Some(sw) = sharded_wal.as_ref() {
        sw.read_all_entries_len()
            .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?
    } else {
        let mut wal = wal.lock().map_err(|e| format!("wal lock replay: {e}"))?;
        wal.read_all_entries()?.len()
    };

    let mut storage_reader =
        CollectionStorage::open_or_create(&cfg.data_dir, collection_name, Some(cfg.mmap_bytes))?;
    let (strands, decode_elapsed) = if run_projection_stream {
        storage_reader.flush()?;
        let expected = if run_write { cfg.records } else { wal_entries };
        let elapsed = projection_pipeline_stream_decode(&storage_reader, expected)?;
        (Vec::new(), elapsed)
    } else if run_full_vector_decode {
        let decode_start = Instant::now();
        {
            storage_reader.flush()?;
            let decoded = decode_all_strands(&storage_reader)?;
            let elapsed = decode_start.elapsed();
            let expected = if run_write { cfg.records } else { wal_entries };
            if decoded.len() != expected {
                return Err(
                    format!("decoded {} strands but expected {}", decoded.len(), expected).into(),
                );
            }
            (decoded, elapsed)
        }
    } else {
        (Vec::new(), Duration::from_secs(0))
    };
    let mut projected_decode_elapsed = Duration::from_secs(0);
    let mut projected_rows = 0usize;

    let (point_elapsed, sample) = if run_query_suite {
        let point_start = Instant::now();
        let index = index_by_signature(&strands);
        let sample = cfg.read_sample.min(cfg.records).max(1);
        for i in 0..sample {
            let id = (i * 9973) % cfg.records;
            let sig = (id as u64 + 1).to_le_bytes();
            if !index.contains_key(&sig) {
                return Err(format!("missing signature for sampled record {id}").into());
            }
        }
        (point_start.elapsed(), sample)
    } else {
        (Duration::from_secs(0), 0)
    };

    let mut query_elapsed = Duration::from_secs(0);
    let mut found = false;
    let mut full_scan_elapsed = Duration::from_secs(0);
    let mut hits_len = 0usize;
    let mut negative_exact_scan_elapsed = Duration::from_secs(0);
    let mut neg_hits_len = 0usize;
    let mut segment_count = 0usize;
    let mut block_count = 0usize;
    let mut segment_dictionary_entries = 0usize;
    let mut block_dictionary_entries = 0usize;
    let mut segment_dictionary_coverage_pct = 0.0f64;
    let mut block_dictionary_coverage_pct = 0.0f64;
    let mut query_skipped_segments = 0usize;
    let mut query_skipped_blocks = 0usize;
    let mut query_skipped_segments_dict_extra = 0usize;
    let mut query_skipped_blocks_dict_extra = 0usize;
    let mut full_scan_skipped_segments = 0usize;
    let mut full_scan_skipped_blocks = 0usize;
    let mut full_scan_skipped_segments_dict_extra = 0usize;
    let mut full_scan_skipped_blocks_dict_extra = 0usize;
    let mut negative_exact_skipped_segments = 0usize;
    let mut negative_exact_skipped_blocks = 0usize;
    let mut negative_exact_skipped_segments_dict_extra = 0usize;
    let mut negative_exact_skipped_blocks_dict_extra = 0usize;

    if run_query_suite {
        let (segment_ranges, segment_metas, block_ranges, block_metas) =
            build_segment_and_block_metadata(&strands, 4096, 512, &["_payload"]);
        segment_count = segment_metas.len();
        block_count = block_metas.iter().map(|v| v.len()).sum();
        segment_dictionary_entries = segment_metas
            .iter()
            .map(|m| m.dictionary_entry_count())
            .sum();
        block_dictionary_entries = block_metas
            .iter()
            .flatten()
            .map(|m| m.dictionary_entry_count())
            .sum();
        if !segment_metas.is_empty() {
            segment_dictionary_coverage_pct =
                segment_metas.iter().map(|m| m.dictionary_coverage_ratio()).sum::<f64>()
                    / segment_metas.len() as f64
                    * 100.0;
        }
        if block_count > 0 {
            block_dictionary_coverage_pct = block_metas
                .iter()
                .flatten()
                .map(|m| m.dictionary_coverage_ratio())
                .sum::<f64>()
                / block_count as f64
                * 100.0;
        }
        let catalog = GlobalIndexCatalog::build(&strands, &["_payload"], &["_payload"]);
        catalog.save_to_root(&cfg.data_dir, collection_name)?;
        let loaded_catalog = GlobalIndexCatalog::load_from_root(&cfg.data_dir, collection_name)?;

        let target_id = cfg.records / 2;
        let target_payload = make_payload(target_id).into_bytes();
        let pattern = GuidePattern {
            collection: "bench".to_string(),
            clauses: vec![Clause::ExactMatch {
                field_intron: "_payload".to_string(),
                operand_wire: target_payload.clone(),
                operand_codons: encode_bytes_to_codons(&target_payload),
            }],
            includes: vec![],
            overlay: None,
            order_by: None,
            limit: Some(1),
        };

        let query_cfg = ScanConfig {
            collection_id: Some(collection_id),
            segment_metas: Some(segment_metas.clone()),
            segment_ranges: Some(segment_ranges.clone()),
            segment_block_metas: Some(block_metas.clone()),
            segment_block_ranges: Some(block_ranges.clone()),
            indexed_fields: Some(["_payload".to_string()].into_iter().collect()),
            global_hash_indexes: Some(loaded_catalog.hash_indexes.clone()),
            global_range_indexes: Some(loaded_catalog.range_indexes.clone()),
            ..ScanConfig::default()
        };
        query_skipped_segments = segment_metas
            .iter()
            .filter(|m| should_skip_segment(m, &pattern))
            .count();
        let query_skipped_segments_bloom_only = segment_metas
            .iter()
            .filter(|m| should_skip_segment_bloom_only(m, &pattern))
            .count();
        query_skipped_segments_dict_extra =
            query_skipped_segments.saturating_sub(query_skipped_segments_bloom_only);
        query_skipped_blocks = block_metas
            .iter()
            .flatten()
            .filter(|m| should_skip_block(m, &pattern))
            .count();
        let query_skipped_blocks_bloom_only = block_metas
            .iter()
            .flatten()
            .filter(|m| should_skip_block_bloom_only(m, &pattern))
            .count();
        query_skipped_blocks_dict_extra =
            query_skipped_blocks.saturating_sub(query_skipped_blocks_bloom_only);
        let query_start = Instant::now();
        found = fetch_one(&pattern, &strands, &query_cfg).is_some();
        query_elapsed = query_start.elapsed();

        let full_scan_pattern = GuidePattern {
            collection: "bench".to_string(),
            clauses: vec![],
            includes: vec![],
            overlay: None,
            order_by: None,
            limit: None,
        };
        let full_scan_cfg = ScanConfig {
            collection_id: Some(collection_id),
            segment_metas: Some(segment_metas.clone()),
            segment_ranges: Some(segment_ranges.clone()),
            segment_block_metas: Some(block_metas.clone()),
            segment_block_ranges: Some(block_ranges.clone()),
            global_hash_indexes: Some(loaded_catalog.hash_indexes.clone()),
            global_range_indexes: Some(loaded_catalog.range_indexes.clone()),
            ..ScanConfig::default()
        };
        full_scan_skipped_segments = segment_metas
            .iter()
            .filter(|m| should_skip_segment(m, &full_scan_pattern))
            .count();
        let full_scan_skipped_segments_bloom_only = segment_metas
            .iter()
            .filter(|m| should_skip_segment_bloom_only(m, &full_scan_pattern))
            .count();
        full_scan_skipped_segments_dict_extra =
            full_scan_skipped_segments.saturating_sub(full_scan_skipped_segments_bloom_only);
        full_scan_skipped_blocks = block_metas
            .iter()
            .flatten()
            .filter(|m| should_skip_block(m, &full_scan_pattern))
            .count();
        let full_scan_skipped_blocks_bloom_only = block_metas
            .iter()
            .flatten()
            .filter(|m| should_skip_block_bloom_only(m, &full_scan_pattern))
            .count();
        full_scan_skipped_blocks_dict_extra =
            full_scan_skipped_blocks.saturating_sub(full_scan_skipped_blocks_bloom_only);
        let full_scan_start = Instant::now();
        let hits = dnadb_engine::query::scan_strands(&full_scan_pattern, &strands, &full_scan_cfg);
        full_scan_elapsed = full_scan_start.elapsed();
        hits_len = hits.len();
        // Dedicated projection-aware materialization benchmark:
        // decode only `_payload` from introns for all full-scan rows.
        let projection_start = Instant::now();
        let projected = {
            let sig_index = index_by_signature(&strands);
            let matched_rows: Vec<&Strand> = hits
                .iter()
                .filter_map(|sig| sig_index.get(sig).copied())
                .collect();
            let mut required = std::collections::HashSet::new();
            required.insert("_payload".to_string());
            project_rows(&matched_rows, &required)
        };
        projected_decode_elapsed = projection_start.elapsed();
        projected_rows = projected.len();

        let negative_pattern = GuidePattern {
            collection: "bench".to_string(),
            clauses: vec![Clause::ExactMatch {
                field_intron: "_payload".to_string(),
                operand_wire: b"__dnadb_bench_no_hit__".to_vec(),
                operand_codons: encode_bytes_to_codons(b"__dnadb_bench_no_hit__"),
            }],
            includes: vec![],
            overlay: None,
            order_by: None,
            limit: None,
        };
        let neg_cfg = ScanConfig {
            collection_id: Some(collection_id),
            segment_metas: Some(segment_metas.clone()),
            segment_ranges: Some(segment_ranges.clone()),
            segment_block_metas: Some(block_metas.clone()),
            segment_block_ranges: Some(block_ranges.clone()),
            indexed_fields: Some(["_payload".to_string()].into_iter().collect()),
            global_hash_indexes: Some(loaded_catalog.hash_indexes.clone()),
            global_range_indexes: Some(loaded_catalog.range_indexes.clone()),
            ..ScanConfig::default()
        };
        negative_exact_skipped_segments = segment_metas
            .iter()
            .filter(|m| should_skip_segment(m, &negative_pattern))
            .count();
        let negative_exact_skipped_segments_bloom_only = segment_metas
            .iter()
            .filter(|m| should_skip_segment_bloom_only(m, &negative_pattern))
            .count();
        negative_exact_skipped_segments_dict_extra = negative_exact_skipped_segments
            .saturating_sub(negative_exact_skipped_segments_bloom_only);
        negative_exact_skipped_blocks = block_metas
            .iter()
            .flatten()
            .filter(|m| should_skip_block(m, &negative_pattern))
            .count();
        let negative_exact_skipped_blocks_bloom_only = block_metas
            .iter()
            .flatten()
            .filter(|m| should_skip_block_bloom_only(m, &negative_pattern))
            .count();
        negative_exact_skipped_blocks_dict_extra = negative_exact_skipped_blocks
            .saturating_sub(negative_exact_skipped_blocks_bloom_only);
        let neg_start = Instant::now();
        let neg_hits = dnadb_engine::query::scan_strands(&negative_pattern, &strands, &neg_cfg);
        negative_exact_scan_elapsed = neg_start.elapsed();
        neg_hits_len = neg_hits.len();
    }

    let strand_count_for_metrics = if run_projection_stream {
        wal_entries
    } else {
        strands.len()
    };
    let n = cfg.records.max(1) as f64;
    let query_s = query_elapsed.as_secs_f64();
    let full_s = full_scan_elapsed.as_secs_f64();
    let neg_s = negative_exact_scan_elapsed.as_secs_f64();
    let dec_s = decode_elapsed.as_secs_f64();
    let (storage_strands_grow_events, storage_complement_grow_events, storage_meta_grow_events, strands_map_capacity_bytes, complement_map_capacity_bytes, meta_map_capacity_bytes) = {
        let storage = storage
            .lock()
            .map_err(|e| format!("storage lock metrics: {e}"))?;
        let (s, c, m) = storage.grow_events();
        let (s_cap, c_cap, m_cap) = storage.map_capacities();
        (s, c, m, s_cap, c_cap, m_cap)
    };

    let report = BenchReport {
        phase: cfg.phase,
        mode: cfg.mode,
        batch_size: cfg.batch_size,
        threads: cfg.threads,
        prep_batch: cfg.prep_batch,
        concurrent_writers: cfg.concurrent_writers,
        wal_shards: cfg.wal_shards,
        wal_interval_ms,
        records: cfg.records,
        write_seconds: write_elapsed.as_secs_f64(),
        write_records_per_sec: cfg.records as f64 / write_elapsed.as_secs_f64().max(1e-9),
        decode_seconds: dec_s,
        decode_records_per_sec: strand_count_for_metrics as f64
            / decode_elapsed.as_secs_f64().max(1e-9),
        projected_decode_seconds: projected_decode_elapsed.as_secs_f64(),
        projected_rows,
        point_read_sample: sample,
        point_read_seconds: point_elapsed.as_secs_f64(),
        point_reads_per_sec: sample as f64 / point_elapsed.as_secs_f64().max(1e-9),
        query_seconds: query_s,
        query_found: found,
        full_scan_seconds: full_s,
        full_scan_hits: hits_len,
        negative_exact_scan_seconds: neg_s,
        negative_exact_scan_hits: neg_hits_len,
        query_s_per_million_strands: query_s / n * 1e6,
        full_scan_s_per_million_strands: full_s / n * 1e6,
        negative_exact_s_per_million_strands: neg_s / n * 1e6,
        decode_s_per_million_strands: dec_s / strand_count_for_metrics.max(1) as f64 * 1e6,
        projection_pipeline_only: cfg.projection_pipeline_only && run_projection_stream,
        lock_wait_ms_total: lock_wait_ns.load(Ordering::Relaxed) as f64 / 1e6,
        lock_hold_ms_total: lock_hold_ns.load(Ordering::Relaxed) as f64 / 1e6,
        sync_events: sync_events.load(Ordering::Relaxed),
        segment_count,
        block_count,
        segment_dictionary_entries,
        block_dictionary_entries,
        segment_dictionary_coverage_pct,
        block_dictionary_coverage_pct,
        query_skipped_segments,
        query_skipped_blocks,
        query_skipped_segments_dict_extra,
        query_skipped_blocks_dict_extra,
        full_scan_skipped_segments,
        full_scan_skipped_blocks,
        full_scan_skipped_segments_dict_extra,
        full_scan_skipped_blocks_dict_extra,
        negative_exact_skipped_segments,
        negative_exact_skipped_blocks,
        negative_exact_skipped_segments_dict_extra,
        negative_exact_skipped_blocks_dict_extra,
        wal_entries,
        decoded_strands: strand_count_for_metrics,
        ingest_prep_seconds: prep_ns.load(Ordering::Relaxed) as f64 / 1e9,
        ingest_encode_seconds: encode_ns.load(Ordering::Relaxed) as f64 / 1e9,
        ingest_wal_append_seconds: wal_append_ns.load(Ordering::Relaxed) as f64 / 1e9,
        ingest_storage_append_seconds: storage_append_ns.load(Ordering::Relaxed) as f64 / 1e9,
        ingest_storage_sync_seconds: storage_sync_ns.load(Ordering::Relaxed) as f64 / 1e9,
        ingest_wal_sync_seconds: wal_sync_ns.load(Ordering::Relaxed) as f64 / 1e9,
        ingest_wal_appends: wal_appends.load(Ordering::Relaxed),
        ingest_fsync_events: sync_events.load(Ordering::Relaxed),
        storage_strands_grow_events,
        storage_complement_grow_events,
        storage_meta_grow_events,
        storage_total_grow_events: storage_strands_grow_events
            + storage_complement_grow_events
            + storage_meta_grow_events,
        strands_map_capacity_bytes,
        complement_map_capacity_bytes,
        meta_map_capacity_bytes,
        async_sync: cfg.async_sync,
        async_sync_interval_ms: cfg.async_sync_interval_ms,
        async_wal_sync_events: async_wal_sync_events.load(Ordering::Relaxed),
        async_storage_sync_events: async_storage_sync_events.load(Ordering::Relaxed),
        async_sync_max_records: cfg.async_sync_max_records,
    };

    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn parse_args() -> Result<Config, String> {
    let mut cfg = Config {
        records: 100_000,
        read_sample: 10_000,
        data_dir: PathBuf::from("./bench-data"),
        mmap_bytes: 512 * 1024 * 1024,
        mode: BenchMode::Strict,
        batch_size: 1000,
        threads: 1,
        prep_batch: 5000,
        concurrent_writers: false,
        wal_shards: 1,
        wal_interval_ms: None,
        async_sync: false,
        async_sync_interval_ms: 10,
        async_sync_wal_interval_ms: 10,
        async_sync_storage_interval_ms: 100,
        async_sync_max_records: 10000,
        phase: BenchPhase::Full,
        projection_pipeline_only: false,
    };
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--records" => {
                let v = args.next().ok_or("--records requires a value")?;
                cfg.records = v
                    .parse::<usize>()
                    .map_err(|e| format!("invalid --records: {e}"))?;
            }
            "--read-sample" => {
                let v = args.next().ok_or("--read-sample requires a value")?;
                cfg.read_sample = v
                    .parse::<usize>()
                    .map_err(|e| format!("invalid --read-sample: {e}"))?;
            }
            "--data-dir" => {
                let v = args.next().ok_or("--data-dir requires a value")?;
                cfg.data_dir = PathBuf::from(v);
            }
            "--mmap-bytes" => {
                let v = args.next().ok_or("--mmap-bytes requires a value")?;
                cfg.mmap_bytes = v
                    .parse::<usize>()
                    .map_err(|e| format!("invalid --mmap-bytes: {e}"))?;
            }
            "--mode" => {
                let v = args.next().ok_or("--mode requires a value")?;
                cfg.mode = BenchMode::parse(&v)?;
            }
            "--batch-size" => {
                let v = args.next().ok_or("--batch-size requires a value")?;
                cfg.batch_size = v
                    .parse::<usize>()
                    .map_err(|e| format!("invalid --batch-size: {e}"))?;
            }
            "--threads" => {
                let v = args.next().ok_or("--threads requires a value")?;
                cfg.threads = v
                    .parse::<usize>()
                    .map_err(|e| format!("invalid --threads: {e}"))?;
            }
            "--prep-batch" => {
                let v = args.next().ok_or("--prep-batch requires a value")?;
                cfg.prep_batch = v
                    .parse::<usize>()
                    .map_err(|e| format!("invalid --prep-batch: {e}"))?;
            }
            "--concurrent-writers" => {
                cfg.concurrent_writers = true;
            }
            "--wal-shards" => {
                let v = args.next().ok_or("--wal-shards requires a value")?;
                cfg.wal_shards = v
                    .parse::<usize>()
                    .map_err(|e| format!("invalid --wal-shards: {e}"))?;
            }
            "--wal-interval-ms" => {
                let v = args.next().ok_or("--wal-interval-ms requires a value")?;
                let n = v
                    .parse::<u64>()
                    .map_err(|e| format!("invalid --wal-interval-ms: {e}"))?;
                cfg.wal_interval_ms = Some(n);
            }
            "--phase" => {
                let v = args.next().ok_or("--phase requires a value")?;
                cfg.phase = BenchPhase::parse(&v)?;
            }
            "--projection-pipeline-only" => {
                cfg.projection_pipeline_only = true;
            }
            "--async-sync" => {
                cfg.async_sync = true;
            }
            "--async-sync-interval-ms" => {
                let v = args.next().ok_or("--async-sync-interval-ms requires a value")?;
                cfg.async_sync_interval_ms = v
                    .parse::<u64>()
                    .map_err(|e| format!("invalid --async-sync-interval-ms: {e}"))?;
            }
            "--async-sync-wal-interval-ms" => {
                let v = args
                    .next()
                    .ok_or("--async-sync-wal-interval-ms requires a value")?;
                cfg.async_sync_wal_interval_ms = v
                    .parse::<u64>()
                    .map_err(|e| format!("invalid --async-sync-wal-interval-ms: {e}"))?;
            }
            "--async-sync-storage-interval-ms" => {
                let v = args
                    .next()
                    .ok_or("--async-sync-storage-interval-ms requires a value")?;
                cfg.async_sync_storage_interval_ms = v
                    .parse::<u64>()
                    .map_err(|e| format!("invalid --async-sync-storage-interval-ms: {e}"))?;
            }
            "--async-sync-max-records" => {
                let v = args.next().ok_or("--async-sync-max-records requires a value")?;
                cfg.async_sync_max_records = v
                    .parse::<u64>()
                    .map_err(|e| format!("invalid --async-sync-max-records: {e}"))?;
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    if cfg.records == 0 {
        return Err("--records must be > 0".to_string());
    }
    if cfg.mmap_bytes < 8 * 1024 * 1024 {
        return Err("--mmap-bytes must be >= 8MB".to_string());
    }
    if cfg.batch_size == 0 {
        return Err("--batch-size must be > 0".to_string());
    }
    if cfg.threads == 0 {
        return Err("--threads must be > 0".to_string());
    }
    if cfg.prep_batch == 0 {
        return Err("--prep-batch must be > 0".to_string());
    }
    if cfg.wal_shards == 0 {
        return Err("--wal-shards must be > 0".to_string());
    }
    if cfg.async_sync_max_records == 0 {
        return Err("--async-sync-max-records must be > 0".to_string());
    }
    Ok(cfg)
}

#[derive(Debug)]
struct PreparedRecord {
    sequence: u64,
    payload: String,
    strand_bytes: Vec<u8>,
    complement_blob: Vec<u8>,
}

fn prepare_records_chunk(
    start_idx: usize,
    end_idx: usize,
    collection_id: u32,
    start_seq: u64,
    pool: Option<&ThreadPool>,
    codec: &BincodeStrandCodec,
) -> Result<Vec<PreparedRecord>, String> {
    let mk_record = |record_idx: usize| -> Result<PreparedRecord, String> {
                let payload = make_payload(record_idx);
                let sequence = start_seq + (record_idx - start_idx) as u64;
                let strand = strand_from_wal_payload(collection_id, sequence, payload.as_bytes());
                let strand_bytes = codec
                    .encode_strand(&strand)
                    .map_err(|e| format!("encode strand: {e}"))?;
                let complement_blob = encode_complement_blob(&strand.complement)
                    .map_err(|e| format!("encode complement: {e}"))?;
                Ok(PreparedRecord {
                    sequence,
                    payload,
                    strand_bytes,
                    complement_blob,
                })
    };
    if let Some(pool) = pool {
        pool.install(|| {
            (start_idx..end_idx)
                .into_par_iter()
                .map(mk_record)
                .collect::<Result<Vec<_>, _>>()
        })
    } else {
        let mut out = Vec::with_capacity(end_idx - start_idx);
        for record_idx in start_idx..end_idx {
            out.push(mk_record(record_idx)?);
        }
        Ok(out)
    }
}

fn make_payload(i: usize) -> String {
    let region = match i % 4 {
        0 => "us-east",
        1 => "us-west",
        2 => "eu-central",
        _ => "ap-south",
    };
    let active = if i % 3 == 0 { "true" } else { "false" };
    format!(
        "{{\"id\":{i},\"email\":\"user{i}@example.com\",\"region\":\"{region}\",\"score\":{},\"active\":{active}}}",
        i % 1000
    )
}

/// Stream strand frames once: full frame decode + project `_payload` only (no `Vec<Strand>` retention).
/// Wall time is reported as `decode_seconds` when `--projection-pipeline-only` is enabled.
fn projection_pipeline_stream_decode(
    storage: &CollectionStorage,
    expected: usize,
) -> Result<Duration, Box<dyn std::error::Error>> {
    let n = storage.strand_bytes_written();
    let mut bytes = vec![0u8; n];
    let mut f = fs::File::open(storage.paths.strands.as_path())?;
    f.read_exact(&mut bytes)?;
    let codec = BincodeStrandCodec;
    let mut pos = 0usize;
    let mut count = 0usize;
    let mut required = HashSet::new();
    required.insert("_payload".to_string());
    let start = Instant::now();
    while pos + 6 <= bytes.len() && bytes[pos..pos + 4] == STRAND_FORMAT_MAGIC {
        let (strand, len) = codec.decode_strand_with_len(&bytes[pos..])?;
        let _ = project_rows(&[&strand], &required);
        pos += len;
        count += 1;
    }
    let elapsed = start.elapsed();
    if count != expected {
        return Err(format!("projection pipeline: decoded {count} strands, expected {expected}").into());
    }
    Ok(elapsed)
}

fn decode_all_strands(storage: &CollectionStorage) -> Result<Vec<Strand>, Box<dyn std::error::Error>> {
    let n = storage.strand_bytes_written();
    let mut bytes = vec![0u8; n];
    let mut f = fs::File::open(storage.paths.strands.as_path())?;
    f.read_exact(&mut bytes)?;
    let codec = BincodeStrandCodec;
    let mut out = Vec::new();
    let mut pos = 0usize;
    while pos + 6 <= bytes.len() && bytes[pos..pos + 4] == STRAND_FORMAT_MAGIC {
        let (strand, len) = codec.decode_strand_with_len(&bytes[pos..])?;
        out.push(strand);
        pos += len;
    }
    Ok(out)
}

fn index_by_signature(strands: &[Strand]) -> HashMap<[u8; 8], &Strand> {
    strands.iter().map(|s| (s.signature, s)).collect()
}

fn build_segment_and_block_metadata(
    strands: &[Strand],
    segment_size: usize,
    block_size: usize,
    tracked_fields: &[&str],
) -> (
    Vec<(usize, usize)>,
    Vec<SegmentMeta>,
    Vec<Vec<(usize, usize)>>,
    Vec<Vec<SegmentBlockMeta>>,
) {
    let mut ranges = Vec::new();
    let mut metas = Vec::new();
    let mut all_block_ranges = Vec::new();
    let mut all_block_metas = Vec::new();
    let mut segment_id = 0u32;
    let mut start = 0usize;
    while start < strands.len() {
        let end = (start + segment_size).min(strands.len());
        ranges.push((start, end));
        metas.push(SegmentMeta::build(
            segment_id,
            &strands[start..end],
            tracked_fields,
            8192,
            4,
        ));
        let mut block_ranges = Vec::new();
        let mut block_metas = Vec::new();
        let mut block_id = 0u32;
        let mut block_start = start;
        while block_start < end {
            let block_end = (block_start + block_size.max(1)).min(end);
            block_ranges.push((block_start - start, block_end - start));
            block_metas.push(SegmentBlockMeta::build(
                block_id,
                &strands[block_start..block_end],
                tracked_fields,
                2048,
                3,
            ));
            block_id += 1;
            block_start = block_end;
        }
        all_block_ranges.push(block_ranges);
        all_block_metas.push(block_metas);
        segment_id += 1;
        start = end;
    }
    (ranges, metas, all_block_ranges, all_block_metas)
}
