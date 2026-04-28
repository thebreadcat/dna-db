//! Engine runtime facade: wire translation + durable transactional execution.
//!
//! **Durable path** — `DurableTransactionStore` (MVCC, strand materialization, indexes).  
//! **Raw-segment path** — `LsmWritePipeline` under `raw_journal/<collection>/` (WAL + memtable →
//! sealed raw record segments; no strands). Ingest only — not yet visible to MVCC `query` until
//! a materialization bridge exists.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde_json::Value;
use thiserror::Error;

use crate::transaction_durable::{DurableTransactionStore, DurableTxnError, ExecutionResult};
use crate::write_pipeline::{LsmWritePipeline, WritePipelineError};
use crate::wire::{
    translate_mongo_command, translate_postgres_query, MongoCommand, PostgresQuery, WireOperation,
    WireTranslateError,
};

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("wire translation: {0}")]
    WireTranslate(#[from] WireTranslateError),
    #[error("durable transaction: {0}")]
    Durable(#[from] DurableTxnError),
    #[error("raw segment journal: {0}")]
    WritePipeline(#[from] WritePipelineError),
    #[error("raw segment: {0}")]
    RawSegment(&'static str),
    #[error(
        "raw ingest backpressure: pending WAL sequences {pending} exceeds limit {max} (raise DNADB_RAW_MAX_PENDING_WAL or materialize)"
    )]
    RawIngestBackpressure { max: u64, pending: u64 },
    #[error(
        "raw ingest halted: pending WAL sequences {pending} exceeds critical threshold {critical} (materialize or raise DNADB_RAW_CRITICAL_PENDING)"
    )]
    RawIngestCriticalLag { critical: u64, pending: u64 },
}

/// Default memtable cap for the raw-segment pipeline (seal to on-disk segment when full).
const DEFAULT_RAW_MEMTABLE_RECORDS: usize = 8_192;

/// Fixed inner name for `LsmWritePipeline` files under `data_dir/raw_journal/<collection>/` so
/// they never collide with `DurableTransactionStore`’s `users.wal` at the data root.
const RAW_JOURNAL_INNER: &str = "raw";

/// Checkpoint persisted next to the raw journal (`materialize_state.json`).
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct RawMaterializeState {
    /// Raw WAL entries with `sequence <= this` have been upserted into the durable store.
    pub last_applied_wal_sequence: u64,
    /// Wall-clock ms since UNIX epoch when the last materialize run with `records_applied > 0` finished.
    #[serde(default)]
    pub last_materialize_unix_ms: Option<u64>,
    /// Documents upserted in that last non-empty materialize run.
    #[serde(default)]
    pub last_materialize_records: Option<u64>,
    /// Wall duration of that last non-empty run (for throughput estimates).
    #[serde(default)]
    pub last_materialize_duration_ms: Option<u64>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct MaterializationReport {
    pub records_applied: usize,
    pub last_applied_wal_sequence: u64,
    pub batches: usize,
    pub raw_wal_high_water_sequence: u64,
    /// Wall seconds for this materialize call when `records_applied > 0`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<f64>,
    /// `records_applied / duration` from this call when applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub materialization_records_per_sec: Option<f64>,
    /// Whether adaptive (`batch_records == 0`) batching was used.
    pub adaptive_batch: bool,
}

/// Per-collection counters for HTTP observability (process lifetime).
#[derive(Debug, Clone)]
pub struct RawIngestStats {
    pub total_documents: u64,
    pub started_at: Instant,
}

pub struct EngineRuntime {
    root: PathBuf,
    initial_mmap: Option<usize>,
    stores: HashMap<String, DurableTransactionStore>,
    /// Raw append-only LSM journals; key = collection name. Isolated under `raw_journal/`.
    raw_pipelines: HashMap<String, LsmWritePipeline>,
    raw_ingest_stats: HashMap<String, RawIngestStats>,
}

impl EngineRuntime {
    pub fn open(root: &Path, initial_mmap: Option<usize>) -> Self {
        Self {
            root: root.to_path_buf(),
            initial_mmap,
            stores: HashMap::new(),
            raw_pipelines: HashMap::new(),
            raw_ingest_stats: HashMap::new(),
        }
    }

    /// Documents appended via raw segment ingest since engine open (not WAL sequences).
    pub fn raw_ingest_total_documents(&mut self, collection: &str) -> u64 {
        self.raw_ingest_stats
            .get(collection)
            .map(|s| s.total_documents)
            .unwrap_or(0)
    }

    /// Rough ingest throughput estimate (documents / seconds since first raw ingest on this collection).
    pub fn raw_ingest_documents_per_sec_estimate(&mut self, collection: &str) -> Option<f64> {
        let s = self.raw_ingest_stats.get(collection)?;
        let elapsed = s.started_at.elapsed().as_secs_f64();
        if elapsed <= f64::EPSILON {
            return None;
        }
        Some(s.total_documents as f64 / elapsed)
    }

    pub fn execute_mongo_command(
        &mut self,
        cmd: MongoCommand,
    ) -> Result<ExecutionResult, RuntimeError> {
        let op = translate_mongo_command(cmd)?;
        self.execute_wire_operation(op)
    }

    pub fn execute_postgres_query(
        &mut self,
        query: PostgresQuery,
    ) -> Result<ExecutionResult, RuntimeError> {
        let op = translate_postgres_query(query)?;
        self.execute_wire_operation(op)
    }

    pub fn execute_wire_operation(
        &mut self,
        op: WireOperation,
    ) -> Result<ExecutionResult, RuntimeError> {
        let collection = collection_name_for_operation(&op);
        let store = self.ensure_store(&collection)?;
        Ok(store.execute_wire_operation(op)?)
    }

    pub fn execute_mongo_insert_many(
        &mut self,
        collection: &str,
        records: Vec<Value>,
    ) -> Result<ExecutionResult, RuntimeError> {
        let store = self.ensure_store(collection)?;
        Ok(store.execute_insert_many(records)?)
    }

    pub fn execute_mongo_insert_many_with_mode(
        &mut self,
        collection: &str,
        records: Vec<Value>,
        rebuild_indexes: bool,
        defer_storage_fsync: bool,
    ) -> Result<ExecutionResult, RuntimeError> {
        let store = self.ensure_store(collection)?;
        Ok(store.execute_insert_many_with_mode(
            records,
            rebuild_indexes,
            defer_storage_fsync,
        )?)
    }

    pub fn rebuild_indexes(&mut self, collection: &str) -> Result<(), RuntimeError> {
        let store = self.ensure_store(collection)?;
        store.rebuild_indexes()?;
        Ok(())
    }

    pub fn sync_collection_storage(&mut self, collection: &str) -> Result<(), RuntimeError> {
        let store = self.ensure_store(collection)?;
        store.sync_storage_to_disk()?;
        Ok(())
    }

    /// Append JSON documents to the **raw-segment** write path: only `id` + `to_vec` + WAL
    /// (no strand / intron / index work). Seals any open memtable to segment files at the end.
    ///
    /// These rows are **not** yet served by `DurableTransactionStore` / `query` — that requires
    /// a later materialization or read bridge.
    pub fn execute_raw_segment_bulk_insert(
        &mut self,
        collection: &str,
        records: Vec<Value>,
    ) -> Result<usize, RuntimeError> {
        if records.is_empty() {
            return Ok(0);
        }
        let pending = self.raw_journal_pending_sequences(collection)?;
        // Soft limit first (429): operators typically set `max < critical` so normal catch-up uses backpressure
        // before the disaster brake (503).
        if let Some(max) = raw_max_pending_wal_sequences() {
            if pending > max {
                return Err(RuntimeError::RawIngestBackpressure {
                    max,
                    pending,
                });
            }
        }
        if let Some(critical) = raw_critical_pending_wal_sequences() {
            if pending > critical {
                return Err(RuntimeError::RawIngestCriticalLag {
                    critical,
                    pending,
                });
            }
        }
        let n = records.len();
        {
            let now = Instant::now();
            let e = self
                .raw_ingest_stats
                .entry(collection.to_string())
                .or_insert_with(|| RawIngestStats {
                    total_documents: 0,
                    started_at: now,
                });
            e.total_documents = e.total_documents.saturating_add(n as u64);
        }
        let pipe = self.ensure_raw_pipeline(collection)?;
        for rec in records {
            let id = record_id_from_json(&rec).map_err(RuntimeError::RawSegment)?;
            let payload =
                serde_json::to_vec(&rec).map_err(|e| RuntimeError::WritePipeline(e.into()))?;
            pipe.upsert(id, &payload)?;
        }
        pipe.flush_memtable()?;
        Ok(n)
    }

    /// Background Option A: replay raw-journal WAL (ordered) into the durable MVCC store using
    /// **incremental indexes** per batch (`commit_inner(..., apply_incremental_indexes=true)`).
    /// Saves checkpoint after each batch; ends with **`sync_collection_storage`** only — no full O(N) index rebuild.
    ///
    /// **Privacy**: rows land in durable storage under the same rules as direct inserts; visibility uses the
    /// existing query path + overlays — raw segments alone stay non-queryable until this runs.
    ///
    /// **`batch_records == 0`** selects **adaptive** batch sizing from current pending WAL lag
    /// (`clamp(lag/4, min, max)` or tiered; see `DNADB_RAW_MATERIALIZE_*` env vars). Any `> 0` value fixes
    /// the batch size for each inner decode.
    pub fn materialize_raw_journal_to_durable(
        &mut self,
        collection: &str,
        batch_records: usize,
    ) -> Result<MaterializationReport, RuntimeError> {
        let adaptive = batch_records == 0;
        let fixed_batch = if batch_records == 0 {
            None
        } else {
            Some(batch_records.max(1))
        };
        let t_start = Instant::now();
        let mut state = self.load_raw_materialize_state(collection)?;
        let mut records_applied = 0usize;
        let mut batches = 0usize;

        let raw_high_water = {
            let pipe = self.ensure_raw_pipeline(collection)?;
            pipe.raw_wal_high_water_sequence()?
        };

        // Cheap path: nothing pending (repeat cron calls avoid durable store work).
        if raw_high_water <= state.last_applied_wal_sequence {
            return Ok(MaterializationReport {
                records_applied: 0,
                last_applied_wal_sequence: state.last_applied_wal_sequence,
                batches: 0,
                raw_wal_high_water_sequence: raw_high_water,
                duration_ms: None,
                materialization_records_per_sec: None,
                adaptive_batch: adaptive,
            });
        }

        loop {
            let from_seq = state.last_applied_wal_sequence.saturating_add(1);
            let lag = raw_high_water.saturating_sub(state.last_applied_wal_sequence);
            let bs = fixed_batch.unwrap_or_else(|| adaptive_materialize_batch_records(lag));
            let decoded = {
                let pipe = self.ensure_raw_pipeline(collection)?;
                pipe.decode_raw_wal_from_sequence(from_seq, bs)?
            };
            if decoded.is_empty() {
                break;
            }

            let max_seq = decoded
                .iter()
                .map(|d| d.wal_sequence)
                .max()
                .expect("non-empty batch");

            let mut docs = Vec::with_capacity(decoded.len());
            for d in decoded.iter() {
                let v: Value = serde_json::from_slice(&d.payload_json).map_err(|e| {
                    RuntimeError::WritePipeline(WritePipelineError::SerdeJson(e))
                })?;
                let rid = record_id_from_json(&v).map_err(RuntimeError::RawSegment)?;
                if rid != d.record_id {
                    return Err(RuntimeError::RawSegment(
                        "materialize: JSON `id` must match WAL record_id",
                    ));
                }
                docs.push(v);
            }

            let store = self.ensure_store(collection)?;
            store.execute_insert_many_incremental_indexes(docs, true)?;

            state.last_applied_wal_sequence = max_seq;
            self.save_raw_materialize_state(collection, &state)?;
            records_applied += decoded.len();
            batches += 1;
        }

        // Indexes maintained incrementally per batch; strand fsync batched via defer across commits.
        if records_applied > 0 {
            self.sync_collection_storage(collection)?;
            let dur_ms = t_start.elapsed().as_secs_f64() * 1000.0;
            let rps = if dur_ms > 0.0 {
                Some((records_applied as f64) / (dur_ms / 1000.0))
            } else {
                None
            };
            state.last_materialize_unix_ms = Some(unix_epoch_ms_u64());
            state.last_materialize_records = Some(records_applied as u64);
            state.last_materialize_duration_ms = Some(dur_ms.max(1.0) as u64);
            self.save_raw_materialize_state(collection, &state)?;
            return Ok(MaterializationReport {
                records_applied,
                last_applied_wal_sequence: state.last_applied_wal_sequence,
                batches,
                raw_wal_high_water_sequence: raw_high_water,
                duration_ms: Some(dur_ms),
                materialization_records_per_sec: rps,
                adaptive_batch: adaptive,
            });
        }

        Ok(MaterializationReport {
            records_applied,
            last_applied_wal_sequence: state.last_applied_wal_sequence,
            batches,
            raw_wal_high_water_sequence: raw_high_water,
            duration_ms: None,
            materialization_records_per_sec: None,
            adaptive_batch: adaptive,
        })
    }

    /// Pending WAL sequences × adaptive rules → suggested decode batch (for status / debugging).
    pub fn adaptive_materialize_batch_preview(&mut self, collection: &str) -> Result<usize, RuntimeError> {
        let lag = self.raw_journal_pending_sequences(collection)?;
        Ok(adaptive_materialize_batch_records(lag))
    }

    pub fn raw_journal_pending_sequences(&mut self, collection: &str) -> Result<u64, RuntimeError> {
        let (st, hw) = self.raw_journal_materialization_status(collection)?;
        Ok(hw.saturating_sub(st.last_applied_wal_sequence))
    }

    /// Checkpoint + raw WAL tail sequence (`pending = high_water - applied`).
    pub fn raw_journal_materialization_status(
        &mut self,
        collection: &str,
    ) -> Result<(RawMaterializeState, u64), RuntimeError> {
        let st = self.load_raw_materialize_state(collection)?;
        let hw = {
            let pipe = self.ensure_raw_pipeline(collection)?;
            pipe.raw_wal_high_water_sequence()?
        };
        Ok((st, hw))
    }

    pub fn load_raw_materialize_state(&self, collection: &str) -> Result<RawMaterializeState, RuntimeError> {
        let p = self.raw_journal_root(collection).join("materialize_state.json");
        if !p.exists() {
            return Ok(RawMaterializeState::default());
        }
        let bytes = std::fs::read(&p).map_err(|e| {
            RuntimeError::WritePipeline(WritePipelineError::Io(e))
        })?;
        serde_json::from_slice(&bytes)
            .map_err(|e| RuntimeError::WritePipeline(WritePipelineError::SerdeJson(e)))
    }

    fn save_raw_materialize_state(
        &self,
        collection: &str,
        state: &RawMaterializeState,
    ) -> Result<(), RuntimeError> {
        let p = self.raw_journal_root(collection).join("materialize_state.json");
        if let Some(parent) = p.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                RuntimeError::WritePipeline(WritePipelineError::Io(e))
            })?;
        }
        let bytes = serde_json::to_vec_pretty(state).map_err(|e| {
            RuntimeError::WritePipeline(WritePipelineError::SerdeJson(e))
        })?;
        std::fs::write(&p, bytes).map_err(|e| RuntimeError::WritePipeline(WritePipelineError::Io(e)))?;
        Ok(())
    }

    pub fn configure_sort_indexes(
        &mut self,
        collection: &str,
        fields: &[String],
    ) -> Result<Vec<String>, RuntimeError> {
        let store = self.ensure_store(collection)?;
        store.configure_sort_indexes(fields)?;
        Ok(store.sort_index_fields())
    }

    pub fn add_sort_index(
        &mut self,
        collection: &str,
        field: &str,
    ) -> Result<Vec<String>, RuntimeError> {
        let store = self.ensure_store(collection)?;
        store.add_sort_index(field)?;
        Ok(store.sort_index_fields())
    }

    pub fn sort_index_fields(&mut self, collection: &str) -> Result<Vec<String>, RuntimeError> {
        let store = self.ensure_store(collection)?;
        Ok(store.sort_index_fields())
    }

    pub fn sort_index_status(
        &mut self,
        collection: &str,
    ) -> Result<(Vec<String>, Vec<(String, String)>, Vec<String>, &'static str), RuntimeError> {
        let store = self.ensure_store(collection)?;
        let (fields, state) = store.sort_index_status();
        let exact = store.exact_string_index_fields();
        Ok((fields, store.composite_sort_index_defs(), exact, state))
    }

    pub fn configure_exact_string_index_fields(
        &mut self,
        collection: &str,
        fields: &[String],
    ) -> Result<Vec<String>, RuntimeError> {
        let store = self.ensure_store(collection)?;
        store.configure_exact_string_index_fields(fields)?;
        Ok(store.exact_string_index_fields())
    }

    pub fn exact_string_index_fields(
        &mut self,
        collection: &str,
    ) -> Result<Vec<String>, RuntimeError> {
        let store = self.ensure_store(collection)?;
        Ok(store.exact_string_index_fields())
    }

    pub fn configure_composite_sort_indexes(
        &mut self,
        collection: &str,
        defs: &[(String, String)],
    ) -> Result<Vec<(String, String)>, RuntimeError> {
        let store = self.ensure_store(collection)?;
        store.configure_composite_sort_indexes(defs)?;
        Ok(store.composite_sort_index_defs())
    }

    pub fn add_composite_sort_index(
        &mut self,
        collection: &str,
        filter_field: &str,
        order_field: &str,
    ) -> Result<Vec<(String, String)>, RuntimeError> {
        let store = self.ensure_store(collection)?;
        store.add_composite_sort_index(filter_field, order_field)?;
        Ok(store.composite_sort_index_defs())
    }

    fn ensure_store(
        &mut self,
        collection: &str,
    ) -> Result<&mut DurableTransactionStore, RuntimeError> {
        if !self.stores.contains_key(collection) {
            let collection_id = stable_collection_id(collection);
            let store = DurableTransactionStore::open_or_create(
                &self.root,
                collection,
                collection_id,
                self.initial_mmap,
            )?;
            self.stores.insert(collection.to_string(), store);
        }
        Ok(self
            .stores
            .get_mut(collection)
            .expect("store inserted or already present"))
    }

    fn raw_journal_root(&self, collection: &str) -> PathBuf {
        self.root.join("raw_journal").join(collection)
    }

    fn ensure_raw_pipeline(
        &mut self,
        collection: &str,
    ) -> Result<&mut LsmWritePipeline, RuntimeError> {
        if !self.raw_pipelines.contains_key(collection) {
            let jr = self.raw_journal_root(collection);
            let cap = std::env::var("DNADB_RAW_MEMTABLE_RECORDS")
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .filter(|n| *n > 0)
                .unwrap_or(DEFAULT_RAW_MEMTABLE_RECORDS);
            let pipe = LsmWritePipeline::open_or_create(&jr, RAW_JOURNAL_INNER, cap)?;
            self.raw_pipelines.insert(collection.to_string(), pipe);
        }
        Ok(self
            .raw_pipelines
            .get_mut(collection)
            .expect("raw pipeline present"))
    }
}

/// Parse numeric `id` from a JSON object (same contract as durable insert).
fn raw_max_pending_wal_sequences() -> Option<u64> {
    std::env::var("DNADB_RAW_MAX_PENDING_WAL")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|n| *n > 0)
}

fn raw_critical_pending_wal_sequences() -> Option<u64> {
    std::env::var("DNADB_RAW_CRITICAL_PENDING")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|n| *n > 0)
}

fn unix_epoch_ms_u64() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn env_usize_positive(var: &str, default: usize) -> usize {
    std::env::var(var)
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(default)
}

/// Soft lag target (pending WAL sequences). When lag exceeds this, adaptive batches step up (bounded by max).
fn raw_target_pending_sequences() -> Option<u64> {
    std::env::var("DNADB_RAW_TARGET_PENDING_SEQUENCES")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|n| *n > 0)
}

fn adaptive_materialize_mode_is_tiered() -> bool {
    matches!(
        std::env::var("DNADB_RAW_MATERIALIZE_ADAPTIVE")
            .ok()
            .as_deref(),
        Some("tiered") | Some("TIERED")
    )
}

/// Pending WAL sequences → decode batch size (`batch_records == 0` path).
fn adaptive_materialize_batch_records(lag_sequences: u64) -> usize {
    let min_bs = env_usize_positive("DNADB_RAW_MATERIALIZE_BATCH_MIN", 512);
    let max_bs = env_usize_positive("DNADB_RAW_MATERIALIZE_BATCH_MAX", 32_768).max(min_bs);

    let mut bs = if adaptive_materialize_mode_is_tiered() {
        match lag_sequences {
            0..=1_000 => 512,
            1_001..=10_000 => 4_096,
            10_001..=100_000 => 16_384,
            _ => 32_768,
        }
    } else {
        let scaled = lag_sequences.saturating_div(4).max(1) as usize;
        scaled.clamp(min_bs, max_bs)
    };

    bs = bs.clamp(min_bs, max_bs);

    if let Some(target) = raw_target_pending_sequences() {
        if lag_sequences > target {
            bs = (bs.saturating_mul(2)).min(max_bs);
        }
    }

    bs.max(1)
}

fn record_id_from_json(value: &Value) -> Result<u64, &'static str> {
    let id = value
        .get("id")
        .ok_or("record must contain numeric `id` field")?;
    match id {
        Value::Number(n) => n.as_u64().ok_or("`id` must fit in u64"),
        _ => Err("`id` must be a JSON number"),
    }
}

fn stable_collection_id(collection: &str) -> u32 {
    let hash = blake3::hash(collection.as_bytes());
    let bytes = hash.as_bytes();
    u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

fn collection_name_for_operation(op: &WireOperation) -> String {
    match op {
        WireOperation::Query(ast) => ast.collection.clone(),
        WireOperation::Insert(insert) => insert.collection.clone(),
        WireOperation::Update(update) => update.collection.clone(),
        WireOperation::Delete(delete) => delete.collection.clone(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Map, Value};
    use std::sync::Mutex;
    use tempfile::tempdir;

    use super::{EngineRuntime, RuntimeError};
    use super::RAW_JOURNAL_INNER;
    use crate::transaction_durable::ExecutionResult;
    use crate::write_pipeline::LsmWritePipeline;
    use crate::wire::{
        MongoCommand, MongoFindCommand, MongoInsertOneCommand, PostgresQuery,
    };

    /// Serialize tests that mutate process-global raw WAL env vars (`DNADB_RAW_*`).
    static RAW_WAL_ENV_MUTEX: Mutex<()> = Mutex::new(());

    #[test]
    fn runtime_executes_mongo_insert_and_find() {
        let dir = tempdir().expect("tempdir");
        let mut rt = EngineRuntime::open(dir.path(), Some(1024 * 1024));

        let insert = MongoCommand::InsertOne(MongoInsertOneCommand {
            collection: "users".to_string(),
            document: json!({"id": 1, "email": "x@example.com", "age": 20}),
        });
        let out = rt.execute_mongo_command(insert).expect("insert");
        assert_eq!(out, ExecutionResult::AffectedRows(1));

        let mut filter = Map::new();
        filter.insert("email".to_string(), Value::String("x@example.com".to_string()));
        let find = MongoCommand::Find(MongoFindCommand {
            collection: "users".to_string(),
            filter,
            sort: None,
            limit: Some(1),
            include_paths: vec![],
        });
        let out = rt.execute_mongo_command(find).expect("find");
        match out {
            ExecutionResult::QueryRows(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].get("id"), Some(&json!(1)));
            }
            _ => panic!("expected query rows"),
        }
    }

    #[test]
    fn raw_segment_bulk_writes_isolated_journal() {
        let dir = tempdir().expect("tempdir");
        let mut rt = EngineRuntime::open(dir.path(), Some(1024 * 1024));
        let n = rt
            .execute_raw_segment_bulk_insert(
                "posts",
                vec![
                    json!({"id": 1, "slug": "a"}),
                    json!({"id": 2, "slug": "b"}),
                ],
            )
            .expect("raw bulk");
        assert_eq!(n, 2);
        let jr = dir.path().join("raw_journal").join("posts");
        assert!(jr.join("raw.wal").exists(), "expected {}", jr.join("raw.wal").display());
        assert!(
            jr.join("raw.segments").join("manifest.json").exists(),
            "manifest after seal"
        );
        let pipe =
            LsmWritePipeline::open_or_create(&jr, RAW_JOURNAL_INNER, 100).expect("reopen raw");
        let v: Value = serde_json::from_slice(&pipe.get(1).expect("id 1")).expect("json");
        assert_eq!(v, json!({"id": 1, "slug": "a"}));
    }

    #[test]
    fn raw_ingest_background_materialize_is_queryable() {
        let dir = tempdir().expect("tempdir");
        let mut rt = EngineRuntime::open(dir.path(), Some(1024 * 1024));
        rt.execute_raw_segment_bulk_insert(
            "items",
            vec![json!({"id": 42, "slug": "x"})],
        )
        .expect("raw");

        let r = rt
            .materialize_raw_journal_to_durable("items", 1_000)
            .expect("materialize");
        assert_eq!(r.records_applied, 1);
        assert_eq!(r.last_applied_wal_sequence, r.raw_wal_high_water_sequence);
        let (st, hw) = rt.raw_journal_materialization_status("items").expect("status");
        assert_eq!(st.last_applied_wal_sequence, hw);
        assert_eq!(hw.saturating_sub(st.last_applied_wal_sequence), 0);

        let mut filter = Map::new();
        filter.insert("id".to_string(), json!(42));
        let out = rt
            .execute_mongo_command(MongoCommand::Find(MongoFindCommand {
                collection: "items".to_string(),
                filter,
                sort: None,
                limit: Some(1),
                include_paths: vec![],
            }))
            .expect("find");
        match out {
            ExecutionResult::QueryRows(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].get("slug"), Some(&json!("x")));
            }
            _ => panic!("expected rows"),
        }

        let idle = rt
            .materialize_raw_journal_to_durable("items", 512)
            .expect("idle materialize");
        assert_eq!(idle.records_applied, 0);
        assert_eq!(idle.batches, 0);
    }

    #[test]
    fn adaptive_batch_smooth_uses_lag_over_four() {
        let _env = RAW_WAL_ENV_MUTEX.lock().expect("env mutex");
        unsafe {
            std::env::remove_var("DNADB_RAW_MATERIALIZE_ADAPTIVE");
            std::env::remove_var("DNADB_RAW_TARGET_PENDING_SEQUENCES");
        }
        assert_eq!(super::adaptive_materialize_batch_records(100), 512);
        assert_eq!(super::adaptive_materialize_batch_records(4_000), 1_000);
        assert_eq!(super::adaptive_materialize_batch_records(10_000), 2_500);
    }

    #[test]
    fn adaptive_batch_tiered_mode() {
        let _env = RAW_WAL_ENV_MUTEX.lock().expect("env mutex");
        unsafe {
            std::env::remove_var("DNADB_RAW_TARGET_PENDING_SEQUENCES");
            std::env::set_var("DNADB_RAW_MATERIALIZE_ADAPTIVE", "tiered");
        }
        assert_eq!(super::adaptive_materialize_batch_records(500), 512);
        assert_eq!(super::adaptive_materialize_batch_records(5_000), 4_096);
        assert_eq!(super::adaptive_materialize_batch_records(50_000), 16_384);
        unsafe {
            std::env::remove_var("DNADB_RAW_MATERIALIZE_ADAPTIVE");
        }
    }

    #[test]
    fn raw_ingest_critical_lag_hits_before_max() {
        let _env = RAW_WAL_ENV_MUTEX.lock().expect("env mutex");
        unsafe {
            std::env::set_var("DNADB_RAW_CRITICAL_PENDING", "5");
            std::env::set_var("DNADB_RAW_MAX_PENDING_WAL", "10_000");
        }
        let dir = tempdir().expect("tempdir");
        let mut rt = EngineRuntime::open(dir.path(), Some(4096));
        rt.execute_raw_segment_bulk_insert(
            "c",
            (1..=6_i64).map(|id| json!({"id": id})).collect(),
        )
        .expect("first bulk");

        let err = rt
            .execute_raw_segment_bulk_insert("c", vec![json!({"id": 99_i64})])
            .unwrap_err();

        unsafe {
            std::env::remove_var("DNADB_RAW_CRITICAL_PENDING");
            std::env::remove_var("DNADB_RAW_MAX_PENDING_WAL");
        }

        match err {
            RuntimeError::RawIngestCriticalLag { critical, pending } => {
                assert_eq!(critical, 5);
                assert!(pending > critical);
            }
            other => panic!("expected critical lag, got {other:?}"),
        }
    }

    #[test]
    fn raw_ingest_backpressure_when_pending_exceeds_limit() {
        let _env = RAW_WAL_ENV_MUTEX.lock().expect("env mutex");
        unsafe {
            std::env::remove_var("DNADB_RAW_CRITICAL_PENDING");
            std::env::set_var("DNADB_RAW_MAX_PENDING_WAL", "5");
        }
        let dir = tempdir().expect("tempdir");
        let mut rt = EngineRuntime::open(dir.path(), Some(4096));
        rt.execute_raw_segment_bulk_insert(
            "bp",
            (1..=6_i64).map(|id| json!({"id": id})).collect(),
        )
        .expect("first bulk");

        let err = rt
            .execute_raw_segment_bulk_insert("bp", vec![json!({"id": 99_i64})])
            .unwrap_err();

        unsafe {
            std::env::remove_var("DNADB_RAW_MAX_PENDING_WAL");
        }

        match err {
            RuntimeError::RawIngestBackpressure { max, pending } => {
                assert_eq!(max, 5);
                assert!(pending > max);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn runtime_executes_postgres_insert_and_select() {
        let dir = tempdir().expect("tempdir");
        let mut rt = EngineRuntime::open(dir.path(), Some(1024 * 1024));

        rt.execute_postgres_query(PostgresQuery {
            sql: "INSERT INTO users (id, email) VALUES (7, 'a@b.com')".to_string(),
        })
        .expect("insert");

        let out = rt
            .execute_postgres_query(PostgresQuery {
                sql: "SELECT * FROM users WHERE id = 7".to_string(),
            })
            .expect("select");
        match out {
            ExecutionResult::QueryRows(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].get("email"), Some(&json!("a@b.com")));
            }
            _ => panic!("expected query rows"),
        }
    }
}

