use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
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
    should_skip_segment, Clause, GlobalIndexCatalog, GuidePattern, ScanConfig, SegmentMeta,
};
use dnadb_engine::storage::CollectionStorage;
use dnadb_engine::wal::Wal;
use rayon::prelude::*;
use rayon::ThreadPool;
use rayon::ThreadPoolBuilder;
use serde::Serialize;

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
    concurrent_writers: bool,
    /// If set, milliseconds between WAL+storage fsync groups. If unset, mode defaults apply
    /// (`strict` 10ms, `balanced` 50ms, `fast` unused). `Some(0)` disables the timer (batch only).
    wal_interval_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum BenchMode {
    Strict,
    Balanced,
    Fast,
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
    mode: BenchMode,
    batch_size: usize,
    threads: usize,
    prep_batch: usize,
    concurrent_writers: bool,
    /// Effective WAL group-commit interval (ms); 0 means timer disabled (batch-only triggers).
    wal_interval_ms: u64,
    records: usize,
    write_seconds: f64,
    write_records_per_sec: f64,
    decode_seconds: f64,
    decode_records_per_sec: f64,
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
    lock_wait_ms_total: f64,
    lock_hold_ms_total: f64,
    sync_events: u64,
    segment_count: usize,
    query_skipped_segments: usize,
    full_scan_skipped_segments: usize,
    negative_exact_skipped_segments: usize,
    wal_entries: usize,
    decoded_strands: usize,
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
    let wal = Arc::new(Mutex::new(wal));
    let storage = Arc::new(Mutex::new(storage));
    let group_commit = Arc::new(Mutex::new(group_commit_opt));

    let write_start = Instant::now();
    if cfg.concurrent_writers && cfg.threads > 1 {
        let total_records = cfg.records;
        let writer_threads = cfg.threads;
        let mut handles = Vec::with_capacity(cfg.threads);
        for tid in 0..cfg.threads {
            let wal = Arc::clone(&wal);
            let storage = Arc::clone(&storage);
            let group_commit = Arc::clone(&group_commit);
            let lock_wait_ns = Arc::clone(&lock_wait_ns);
            let lock_hold_ns = Arc::clone(&lock_hold_ns);
            let sync_events = Arc::clone(&sync_events);
            let handle = thread::spawn(move || -> Result<(), String> {
                for record_idx in (tid..total_records).step_by(writer_threads) {
                    let payload = make_payload(record_idx);

                    let wait_start = Instant::now();
                    let mut wal_guard = wal.lock().map_err(|e| format!("wal lock: {e}"))?;
                    let waited = wait_start.elapsed();
                    let hold_start = Instant::now();
                    let seq = wal_guard
                        .append(payload.as_bytes())
                        .map_err(|e| format!("wal append: {e}"))?;
                    drop(wal_guard);
                    lock_wait_ns.fetch_add(waited.as_nanos() as u64, Ordering::Relaxed);
                    lock_hold_ns.fetch_add(hold_start.elapsed().as_nanos() as u64, Ordering::Relaxed);

                    // Sequence is assigned: encode/materialize outside WAL lock.
                    let strand = strand_from_wal_payload(collection_id, seq, payload.as_bytes());
                    let strand_bytes = codec
                        .encode_strand(&strand)
                        .map_err(|e| format!("encode strand: {e}"))?;
                    let complement_blob = encode_complement_blob(&strand.complement)
                        .map_err(|e| format!("encode complement: {e}"))?;

                    let wait_start = Instant::now();
                    let mut storage_guard =
                        storage.lock().map_err(|e| format!("storage lock: {e}"))?;
                    let waited = wait_start.elapsed();
                    let hold_start = Instant::now();
                    storage_guard
                        .append_strands(&strand_bytes)
                        .map_err(|e| format!("append strands: {e}"))?;
                    storage_guard
                        .append_complement(&complement_blob)
                        .map_err(|e| format!("append complement: {e}"))?;
                    storage_guard.record_materialized_sequence(seq);
                    drop(storage_guard);
                    lock_wait_ns.fetch_add(waited.as_nanos() as u64, Ordering::Relaxed);
                    lock_hold_ns.fetch_add(hold_start.elapsed().as_nanos() as u64, Ordering::Relaxed);

                    let mut gc_guard = group_commit
                        .lock()
                        .map_err(|e| format!("group commit lock: {e}"))?;
                    let sync_now = gc_guard
                        .as_mut()
                        .map(|g| g.after_record())
                        .unwrap_or(false);
                    drop(gc_guard);
                    if sync_now {
                        let mut storage_guard =
                            storage.lock().map_err(|e| format!("storage lock sync: {e}"))?;
                        storage_guard
                            .flush_maps()
                            .map_err(|e| format!("flush maps: {e}"))?;
                        storage_guard
                            .sync_files()
                            .map_err(|e| format!("sync files: {e}"))?;
                        drop(storage_guard);
                        let mut wal_guard =
                            wal.lock().map_err(|e| format!("wal lock sync: {e}"))?;
                        wal_guard.sync().map_err(|e| format!("wal sync: {e}"))?;
                        drop(wal_guard);
                        sync_events.fetch_add(1, Ordering::Relaxed);
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
    } else {
        let mut wal = wal.lock().map_err(|e| format!("wal lock: {e}"))?;
        let mut storage = storage
            .lock()
            .map_err(|e| format!("storage lock: {e}"))?;
        let mut group_commit_opt = group_commit
            .lock()
            .map_err(|e| format!("group commit lock: {e}"))?;
        let chunk_size = cfg.prep_batch.max(1);
        let mut next_idx = 0usize;
        while next_idx < cfg.records {
            let chunk_end = (next_idx + chunk_size).min(cfg.records);
            let start_seq = wal.next_sequence();
            let prepared = prepare_records_chunk(
                next_idx,
                chunk_end,
                collection_id,
                start_seq,
                encode_pool.as_ref(),
                &codec,
            )
            .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
            for rec in prepared {
                let seq = wal.append(rec.payload.as_bytes())?;
                debug_assert_eq!(seq, rec.sequence);
                storage.append_strands(&rec.strand_bytes)?;
                storage.append_complement(&rec.complement_blob)?;
                storage.record_materialized_sequence(rec.sequence);
                let sync_now = group_commit_opt
                    .as_mut()
                    .map(|g| g.after_record())
                    .unwrap_or(false);
                if sync_now {
                    storage.flush_maps()?;
                    wal.sync()?;
                    storage.sync_files()?;
                    sync_events.fetch_add(1, Ordering::Relaxed);
                }
            }
            next_idx = chunk_end;
        }
    }
    {
        let mut storage = storage
            .lock()
            .map_err(|e| format!("storage lock final: {e}"))?;
        storage.flush_maps()?;
        storage.sync_files()?;
    }
    {
        let mut wal = wal.lock().map_err(|e| format!("wal lock final: {e}"))?;
        wal.sync()?;
    }
    let write_elapsed = write_start.elapsed();

    let wal_entries = {
        let mut wal = wal.lock().map_err(|e| format!("wal lock replay: {e}"))?;
        wal.read_all_entries()?.len()
    };

    let decode_start = Instant::now();
    {
        let mut storage = storage
            .lock()
            .map_err(|e| format!("storage lock decode: {e}"))?;
        storage.flush()?;
    }
    let storage = CollectionStorage::open_or_create(&cfg.data_dir, collection_name, Some(cfg.mmap_bytes))?;
    let strands = decode_all_strands(&storage)?;
    let decode_elapsed = decode_start.elapsed();

    if strands.len() != cfg.records {
        return Err(
            format!("decoded {} strands but expected {}", strands.len(), cfg.records).into(),
        );
    }

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
    let point_elapsed = point_start.elapsed();

    let (segment_ranges, segment_metas) = build_segment_metadata(&strands, 4096, &["_payload"]);
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
        indexed_fields: Some(["_payload".to_string()].into_iter().collect()),
        global_hash_indexes: Some(loaded_catalog.hash_indexes.clone()),
        global_range_indexes: Some(loaded_catalog.range_indexes.clone()),
        ..ScanConfig::default()
    };
    let query_skipped_segments = segment_metas
        .iter()
        .filter(|m| should_skip_segment(m, &pattern))
        .count();
    let query_start = Instant::now();
    let found = fetch_one(
        &pattern,
        &strands,
        &query_cfg,
    )
    .is_some();
    let query_elapsed = query_start.elapsed();

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
        global_hash_indexes: Some(loaded_catalog.hash_indexes.clone()),
        global_range_indexes: Some(loaded_catalog.range_indexes.clone()),
        ..ScanConfig::default()
    };
    let full_scan_skipped_segments = segment_metas
        .iter()
        .filter(|m| should_skip_segment(m, &full_scan_pattern))
        .count();
    let full_scan_start = Instant::now();
    let hits = dnadb_engine::query::scan_strands(
        &full_scan_pattern,
        &strands,
        &full_scan_cfg,
    );
    let full_scan_elapsed = full_scan_start.elapsed();

    // Full parallel pass with a payload that never matches (same intron fast path as exact match).
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
        indexed_fields: Some(["_payload".to_string()].into_iter().collect()),
        global_hash_indexes: Some(loaded_catalog.hash_indexes.clone()),
        global_range_indexes: Some(loaded_catalog.range_indexes.clone()),
        ..ScanConfig::default()
    };
    let negative_exact_skipped_segments = segment_metas
        .iter()
        .filter(|m| should_skip_segment(m, &negative_pattern))
        .count();
    let neg_start = Instant::now();
    let neg_hits = dnadb_engine::query::scan_strands(
        &negative_pattern,
        &strands,
        &neg_cfg,
    );
    let negative_exact_scan_elapsed = neg_start.elapsed();

    let n = cfg.records as f64;
    let query_s = query_elapsed.as_secs_f64();
    let full_s = full_scan_elapsed.as_secs_f64();
    let neg_s = negative_exact_scan_elapsed.as_secs_f64();
    let dec_s = decode_elapsed.as_secs_f64();

    let report = BenchReport {
        mode: cfg.mode,
        batch_size: cfg.batch_size,
        threads: cfg.threads,
        prep_batch: cfg.prep_batch,
        concurrent_writers: cfg.concurrent_writers,
        wal_interval_ms,
        records: cfg.records,
        write_seconds: write_elapsed.as_secs_f64(),
        write_records_per_sec: cfg.records as f64 / write_elapsed.as_secs_f64().max(1e-9),
        decode_seconds: dec_s,
        decode_records_per_sec: cfg.records as f64 / decode_elapsed.as_secs_f64().max(1e-9),
        point_read_sample: sample,
        point_read_seconds: point_elapsed.as_secs_f64(),
        point_reads_per_sec: sample as f64 / point_elapsed.as_secs_f64().max(1e-9),
        query_seconds: query_s,
        query_found: found,
        full_scan_seconds: full_s,
        full_scan_hits: hits.len(),
        negative_exact_scan_seconds: neg_s,
        negative_exact_scan_hits: neg_hits.len(),
        query_s_per_million_strands: query_s / n * 1e6,
        full_scan_s_per_million_strands: full_s / n * 1e6,
        negative_exact_s_per_million_strands: neg_s / n * 1e6,
        decode_s_per_million_strands: dec_s / n * 1e6,
        lock_wait_ms_total: lock_wait_ns.load(Ordering::Relaxed) as f64 / 1e6,
        lock_hold_ms_total: lock_hold_ns.load(Ordering::Relaxed) as f64 / 1e6,
        sync_events: sync_events.load(Ordering::Relaxed),
        segment_count: segment_metas.len(),
        query_skipped_segments,
        full_scan_skipped_segments,
        negative_exact_skipped_segments,
        wal_entries,
        decoded_strands: strands.len(),
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
        wal_interval_ms: None,
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
            "--wal-interval-ms" => {
                let v = args.next().ok_or("--wal-interval-ms requires a value")?;
                let n = v
                    .parse::<u64>()
                    .map_err(|e| format!("invalid --wal-interval-ms: {e}"))?;
                cfg.wal_interval_ms = Some(n);
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

fn decode_all_strands(storage: &CollectionStorage) -> Result<Vec<Strand>, Box<dyn std::error::Error>> {
    let n = storage.strand_bytes_written();
    let mut bytes = vec![0u8; n];
    let mut f = fs::File::open(storage.paths.strands.as_path())?;
    f.read_exact(&mut bytes)?;
    let codec = BincodeStrandCodec;
    let mut out = Vec::new();
    let mut pos = 0usize;
    while pos + 6 <= bytes.len() && bytes[pos..pos + 4] == STRAND_FORMAT_MAGIC {
        let strand = codec.decode_strand(&bytes[pos..])?;
        let len = codec.encode_strand(&strand)?.len();
        out.push(strand);
        pos += len;
    }
    Ok(out)
}

fn index_by_signature(strands: &[Strand]) -> HashMap<[u8; 8], &Strand> {
    strands.iter().map(|s| (s.signature, s)).collect()
}

fn build_segment_metadata(
    strands: &[Strand],
    segment_size: usize,
    tracked_fields: &[&str],
) -> (Vec<(usize, usize)>, Vec<SegmentMeta>) {
    let mut ranges = Vec::new();
    let mut metas = Vec::new();
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
        segment_id += 1;
        start = end;
    }
    (ranges, metas)
}
