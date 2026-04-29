//! Durable transaction runtime: MVCC transaction manager + WAL/materialization integration.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::query::{
    choose_path, compile_query, like_util::sql_like_contains_literal,
    like_util::sql_like_prefix_literal, CollectionStats, QueryAst, QueryLiteral, QueryPath,
    RangeOp, SortDirection, WhereOp,
};
use crate::codec::BincodeStrandCodec;
use crate::processor::{process_wal_entry_with_mode, PersistMode, ProcessorError};
use crate::storage::{CollectionStorage, StorageError};
use crate::transaction::{Transaction, TransactionManager, WriteOp};
use crate::wal::{Wal, WalError};
use crate::wire::{DeleteOp, InsertOp, UpdateOp, WireOperation};

#[derive(Debug, thiserror::Error)]
pub enum DurableTxnError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("wal: {0}")]
    Wal(#[from] WalError),
    #[error("storage: {0}")]
    Storage(#[from] StorageError),
    #[error("processor: {0}")]
    Processor(#[from] ProcessorError),
    #[error("serde json: {0}")]
    SerdeJson(#[from] serde_json::Error),
    #[error("transaction: {0}")]
    Transaction(&'static str),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum DurableOpPayload {
    Upsert {
        txn_id: u64,
        record_id: u64,
        record: crate::mvcc::Record,
    },
    Delete {
        txn_id: u64,
        record_id: u64,
    },
}

pub struct DurableTransactionStore {
    tx_manager: TransactionManager,
    wal: Wal,
    storage: CollectionStorage,
    codec: BincodeStrandCodec,
    collection_id: u32,
    sort_index_config_path: PathBuf,
    sort_index_fields: HashSet<String>,
    /// Sealed run: sorted ascending by `(key, record_id)` from last full rebuild.
    sort_index_sealed: HashMap<String, Vec<SortIndexEntry>>,
    /// Mutable since last seal; merged at query time with `sort_index_sealed` (§12).
    sort_index_active: HashMap<String, BTreeSet<SortIndexEntry>>,
    /// Record ids to ignore in `sort_index_sealed` for this field (updates/deletes after seal).
    sort_index_sealed_stale: HashMap<String, HashSet<u64>>,
    sort_index_values: HashMap<String, HashMap<u64, i64>>,
    exact_string_indexes: HashMap<String, BTreeMap<String, HashSet<u64>>>,
    exact_string_index_values: HashMap<u64, Vec<(String, String)>>,
    text_trigram_indexes: HashMap<String, BTreeMap<String, HashSet<u64>>>,
    text_trigram_index_values: HashMap<u64, Vec<(String, Vec<String>)>>,
    composite_sort_indexes: HashMap<(String, String), HashMap<String, BTreeSet<SortIndexEntry>>>,
    composite_sort_index_values: HashMap<(String, String), HashMap<u64, (String, i64)>>,
    composite_sort_index_defs: HashSet<(String, String)>,
    /// String fields maintained in `exact_string_indexes` for equality fast paths.
    exact_string_index_fields: HashSet<String>,
    /// Strand/complement/meta files skipped `sync_data` on the last deferred bulk commit.
    /// Cleared by [`Self::sync_storage_to_disk`], [`Self::rebuild_sort_indexes`], or any full flush commit.
    pending_deferred_storage_sync: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionResult {
    QueryRows(Vec<crate::mvcc::Record>),
    AffectedRows(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotQueryPlan {
    Direct,
    ExactIndex,
    SortIndex,
    Index,
    GuidedScan,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FastPathKind {
    CompositeSort,
    ExactIndex,
    /// Prefix-only `LIKE 'foo%'` on an exact-string-indexed field (§13).
    PrefixStringIndex,
    /// Contains-only `LIKE '%foo%'` using a trigram postings index.
    ContainsStringIndex,
    SortIndex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct SortIndexEntry {
    key: i64,
    record_id: u64,
}

const DEFAULT_SORT_INDEX_FIELDS: [&str; 4] = ["id", "updated_at", "created_at", "published_at"];
const DEFAULT_EXACT_STRING_INDEX_FIELDS: [&str; 3] = ["slug", "title", "email"];

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SortIndexConfigFile {
    #[serde(default)]
    fields: Vec<String>,
    #[serde(default)]
    composite_fields: Vec<[String; 2]>,
    /// Omitted in legacy files → load uses engine defaults (`slug`, `title`, `email`).
    /// Present as `[]` disables exact-string indexes for this collection.
    #[serde(default)]
    exact_string_fields: Option<Vec<String>>,
}

#[derive(Debug, Default)]
struct SnapshotIndexes {
    direct_by_id: HashMap<u64, crate::mvcc::Record>,
    exact_by_field: HashMap<String, HashMap<String, Vec<u64>>>,
    range_by_field: HashMap<String, Vec<(f64, u64)>>,
    all_records_by_id: HashMap<u64, crate::mvcc::Record>,
}

impl DurableTransactionStore {
    pub fn open_or_create(
        root: &Path,
        collection: &str,
        collection_id: u32,
        initial_mmap: Option<usize>,
    ) -> Result<Self, DurableTxnError> {
        let sort_index_config_path = root.join(format!("{collection}.sort_indexes.json"));
        let (configured_fields, configured_composites, configured_exact) =
            load_sort_index_config(&sort_index_config_path)?.unwrap_or_else(|| {
                (
                    DEFAULT_SORT_INDEX_FIELDS
                        .iter()
                        .map(|s| (*s).to_string())
                        .collect::<Vec<_>>(),
                    Vec::new(),
                    default_exact_string_index_field_set(),
                )
            });
        let mut out = Self {
            tx_manager: TransactionManager::new(),
            wal: Wal::open_or_create(root, collection)?,
            storage: CollectionStorage::open_or_create(root, collection, initial_mmap)?,
            codec: BincodeStrandCodec,
            collection_id,
            sort_index_config_path,
            sort_index_fields: configured_fields.into_iter().collect(),
            sort_index_sealed: HashMap::new(),
            sort_index_active: HashMap::new(),
            sort_index_sealed_stale: HashMap::new(),
            sort_index_values: HashMap::new(),
            exact_string_indexes: HashMap::new(),
            exact_string_index_values: HashMap::new(),
            text_trigram_indexes: HashMap::new(),
            text_trigram_index_values: HashMap::new(),
            composite_sort_indexes: HashMap::new(),
            composite_sort_index_values: HashMap::new(),
            composite_sort_index_defs: configured_composites
                .into_iter()
                .map(|[a, b]| (a, b))
                .collect(),
            exact_string_index_fields: configured_exact,
            pending_deferred_storage_sync: false,
        };
        out.replay_wal_to_mvcc()?;
        out.rebuild_sort_indexes()?;
        Ok(out)
    }

    pub fn configure_sort_indexes(&mut self, fields: &[String]) -> Result<(), DurableTxnError> {
        self.sort_index_fields = fields.iter().cloned().collect();
        self.persist_index_config()?;
        self.rebuild_sort_indexes()?;
        Ok(())
    }

    pub fn add_sort_index(&mut self, field: &str) -> Result<(), DurableTxnError> {
        if field.trim().is_empty() {
            return Ok(());
        }
        self.sort_index_fields.insert(field.to_string());
        self.persist_index_config()?;
        self.rebuild_sort_indexes()?;
        Ok(())
    }

    pub fn configure_composite_sort_indexes(
        &mut self,
        defs: &[(String, String)],
    ) -> Result<(), DurableTxnError> {
        self.composite_sort_index_defs = defs.iter().cloned().collect();
        self.persist_index_config()?;
        self.rebuild_sort_indexes()?;
        Ok(())
    }

    pub fn add_composite_sort_index(
        &mut self,
        filter_field: &str,
        order_field: &str,
    ) -> Result<(), DurableTxnError> {
        let filter = filter_field.trim();
        let order = order_field.trim();
        if filter.is_empty() || order.is_empty() {
            return Ok(());
        }
        self.composite_sort_index_defs
            .insert((filter.to_string(), order.to_string()));
        self.persist_index_config()?;
        self.rebuild_sort_indexes()?;
        Ok(())
    }

    /// Replace the set of string fields indexed for exact equality (`WHERE field = "…"`).
    /// Pass an empty slice to disable exact-string indexes for this collection.
    pub fn configure_exact_string_index_fields(
        &mut self,
        fields: &[String],
    ) -> Result<(), DurableTxnError> {
        self.exact_string_index_fields = fields
            .iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        self.persist_index_config()?;
        self.rebuild_sort_indexes()?;
        Ok(())
    }

    pub fn exact_string_index_fields(&self) -> Vec<String> {
        let mut out: Vec<String> = self.exact_string_index_fields.iter().cloned().collect();
        out.sort();
        out
    }

    fn persist_index_config(&self) -> Result<(), DurableTxnError> {
        save_sort_index_config(
            &self.sort_index_config_path,
            &self.sort_index_fields,
            &self.composite_sort_index_defs,
            &self.exact_string_index_fields,
        )
    }

    pub fn sort_index_fields(&self) -> Vec<String> {
        let mut out: Vec<String> = self.sort_index_fields.iter().cloned().collect();
        out.sort();
        out
    }

    pub fn sort_index_status(&self) -> (Vec<String>, &'static str) {
        (self.sort_index_fields(), "ready")
    }

    pub fn composite_sort_index_defs(&self) -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = self.composite_sort_index_defs.iter().cloned().collect();
        out.sort();
        out
    }

    pub fn begin(&mut self) -> Transaction {
        self.tx_manager.begin()
    }

    pub fn read(
        &self,
        txn: &Transaction,
        record_id: u64,
    ) -> Option<crate::mvcc::Record> {
        self.tx_manager.read(txn, record_id)
    }

    pub fn query(&self, txn: &Transaction, ast: &QueryAst) -> Vec<crate::mvcc::Record> {
        let (mut rows, _) = self.query_with_plan(txn, ast);
        apply_sort_and_limit(&mut rows, ast);
        rows
    }

    fn query_with_plan(
        &self,
        txn: &Transaction,
        ast: &QueryAst,
    ) -> (Vec<crate::mvcc::Record>, SnapshotQueryPlan) {
        let effective_ast = self.with_default_order_for_limit(ast);
        let ast = &effective_ast;

        // Hot-path optimization: `id = ...` lookups can read directly from MVCC
        // without materializing a full visible snapshot or per-query indexes.
        if let Some(id) = direct_lookup_id(ast) {
            let rows = self
                .tx_manager
                .read(txn, id)
                .filter(|r| matches_record(r, ast))
                .into_iter()
                .collect();
            return (rows, SnapshotQueryPlan::Direct);
        }

        if let Some(kind) = self.choose_fast_path(ast) {
            match kind {
                FastPathKind::CompositeSort => {
                    if let Some(rows) = self.composite_sort_index_query_rows(txn, ast) {
                        return (rows, SnapshotQueryPlan::SortIndex);
                    }
                }
                FastPathKind::ExactIndex => {
                    if let Some(rows) = self.exact_string_index_query_rows(txn, ast) {
                        return (rows, SnapshotQueryPlan::ExactIndex);
                    }
                }
                FastPathKind::PrefixStringIndex => {
                    if let Some(rows) = self.prefix_string_index_query_rows(txn, ast) {
                        return (rows, SnapshotQueryPlan::ExactIndex);
                    }
                }
                FastPathKind::ContainsStringIndex => {
                    if let Some(rows) = self.contains_string_index_query_rows(txn, ast) {
                        return (rows, SnapshotQueryPlan::ExactIndex);
                    }
                }
                FastPathKind::SortIndex => {
                    if let Some(rows) = self.sort_index_query_rows(txn, ast) {
                        return (rows, SnapshotQueryPlan::SortIndex);
                    }
                }
            }
        }

        let visible: Vec<(u64, crate::mvcc::Record)> = self.tx_manager.visible_records(txn);
        let indexes = build_snapshot_indexes(&visible);
        let Some(guide) = compile_query(ast, None).ok() else {
            let mut rows: Vec<crate::mvcc::Record> = visible
                .into_iter()
                .map(|(_, r)| r)
                .filter(|r| matches_record(r, ast))
                .collect();
            apply_sort_and_limit(&mut rows, ast);
            return (rows, SnapshotQueryPlan::GuidedScan);
        };
        let stats = build_snapshot_collection_stats(ast, &indexes);
        let plan = match choose_path(&guide, &stats) {
            QueryPath::Direct => SnapshotQueryPlan::Direct,
            QueryPath::Index { .. } => SnapshotQueryPlan::Index,
            QueryPath::GuidedScan => SnapshotQueryPlan::GuidedScan,
        };
        let mut rows = match plan {
            SnapshotQueryPlan::Direct => direct_query_rows(ast, &indexes),
            SnapshotQueryPlan::ExactIndex => vec![],
            SnapshotQueryPlan::SortIndex => vec![],
            SnapshotQueryPlan::Index => indexed_query_rows(ast, &indexes),
            SnapshotQueryPlan::GuidedScan => indexes
                .all_records_by_id
                .values()
                .filter(|r| matches_record(r, ast))
                .cloned()
                .collect(),
        };
        apply_sort_and_limit(&mut rows, ast);
        (rows, plan)
    }

    pub fn upsert(&self, txn: &mut Transaction, record_id: u64, record: crate::mvcc::Record) {
        self.tx_manager.upsert(txn, record_id, record);
    }

    pub fn delete(&self, txn: &mut Transaction, record_id: u64) {
        self.tx_manager.delete(txn, record_id);
    }

    pub fn rollback(&mut self, txn: &mut Transaction) -> Result<(), DurableTxnError> {
        self.tx_manager
            .rollback(txn)
            .map_err(DurableTxnError::Transaction)
    }

    pub fn commit(&mut self, txn: &mut Transaction) -> Result<u64, DurableTxnError> {
        self.commit_inner(txn, true, false)
    }

    /// Like [`Self::commit`], but skips incremental index updates. Caller must bring indexes
    /// back in sync (typically via [`Self::rebuild_sort_indexes`]) before serving queries.
    ///
    /// When `defer_storage_fsync` is true: [`CollectionStorage::flush_maps`] runs and WAL is synced,
    /// but [`CollectionStorage::sync_files`] is skipped until [`Self::sync_storage_to_disk`] or the
    /// end of [`Self::rebuild_sort_indexes`] (indexes still need rebuilding when bulk skipped incremental indexes).
    fn commit_inner(
        &mut self,
        txn: &mut Transaction,
        apply_incremental_indexes: bool,
        defer_storage_fsync: bool,
    ) -> Result<u64, DurableTxnError> {
        let profile = env::var_os("DNADB_COMMIT_PROFILE").is_some();
        let t0 = profile.then(Instant::now);

        self.tx_manager
            .can_commit(txn)
            .map_err(DurableTxnError::Transaction)?;
        if profile {
            eprintln!(
                "dnadb_commit_profile: can_commit {}µs write_set_len={}",
                t0.expect("profile").elapsed().as_micros(),
                txn.write_set.len()
            );
        }

        // Only needed when incremental indexes run after MVCC commit.
        let pending_writes = if apply_incremental_indexes {
            let t_clone = profile.then(Instant::now);
            let c = txn.write_set.clone();
            if profile {
                eprintln!(
                    "dnadb_commit_profile: clone_write_set {}µs",
                    t_clone.expect("profile").elapsed().as_micros()
                );
            }
            Some(c)
        } else {
            None
        };

        let write_breakdown = env::var_os("DNADB_WRITE_BREAKDOWN").is_some();
        #[derive(Default)]
        struct WriteBreakdownAccum {
            payload_serialize_us: u128,
            wal_append_us: u128,
            materialize_us: u128,
            flush_maps_us: u128,
            sync_files_us: u128,
            wal_sync_us: u128,
        }
        let mut wb = WriteBreakdownAccum::default();

        let t_wal = profile.then(Instant::now);
        for op in &txn.write_set {
            let payload = if write_breakdown {
                let tp = Instant::now();
                let p = durable_payload_from_write_op(txn.txn_id, op)?;
                wb.payload_serialize_us += tp.elapsed().as_micros() as u128;
                p
            } else {
                durable_payload_from_write_op(txn.txn_id, op)?
            };

            let seq = if write_breakdown {
                let ta = Instant::now();
                let s = self.wal.append(&payload)?;
                wb.wal_append_us += ta.elapsed().as_micros() as u128;
                s
            } else {
                self.wal.append(&payload)?
            };

            let t_mat = if write_breakdown {
                Some(Instant::now())
            } else {
                None
            };
            process_wal_entry_with_mode(
                &mut self.storage,
                &self.codec,
                self.collection_id,
                seq,
                &payload,
                PersistMode::Buffered,
            )?;
            if let Some(t) = t_mat {
                wb.materialize_us += t.elapsed().as_micros() as u128;
            }
        }
        if profile {
            eprintln!(
                "dnadb_commit_profile: wal_append_and_materialize {}µs ops={}",
                t_wal.expect("profile").elapsed().as_micros(),
                txn.write_set.len()
            );
        }

        let t_flush = profile.then(Instant::now);
        let tfm = Instant::now();
        self.storage.flush_maps()?;
        if write_breakdown {
            wb.flush_maps_us += tfm.elapsed().as_micros() as u128;
        }
        if defer_storage_fsync {
            self.pending_deferred_storage_sync = true;
        } else {
            let ts = Instant::now();
            self.storage.sync_files()?;
            if write_breakdown {
                wb.sync_files_us += ts.elapsed().as_micros() as u128;
            }
            self.pending_deferred_storage_sync = false;
        }
        let tw = Instant::now();
        self.wal.sync()?;
        if write_breakdown {
            wb.wal_sync_us += tw.elapsed().as_micros() as u128;
            let ops = txn.write_set.len();
            let wal_materialize_total = wb.payload_serialize_us + wb.wal_append_us + wb.materialize_us;
            let storage_and_wal_sync = wb.flush_maps_us + wb.sync_files_us + wb.wal_sync_us;
            let total_write_path_us = wal_materialize_total + storage_and_wal_sync;
            eprintln!(
                "dnadb_write_breakdown: ops={ops} \
                 payload_serialize_us={} wal_append_us={} materialize_us={} \
                 flush_maps_us={} sync_files_us={} wal_sync_us={} \
                 wal_log_plus_materialize_us={wal_materialize_total} storage_plus_wal_fsync_us={storage_and_wal_sync} \
                 total_write_path_us={total_write_path_us}",
                wb.payload_serialize_us,
                wb.wal_append_us,
                wb.materialize_us,
                wb.flush_maps_us,
                wb.sync_files_us,
                wb.wal_sync_us,
            );
            eprintln!(
                "dnadb_write_breakdown_hint: materialize_us = strand encode + append strands/complement/CRC (grows with record size); \
                 sync_files_us = fdatasync on strand files; set DNADB_COMMIT_PROFILE for mvcc/index slices"
            );
        }
        if profile {
            eprintln!(
                "dnadb_commit_profile: storage_flush_wal_sync {}µs defer_fsync={}",
                t_flush.expect("profile").elapsed().as_micros(),
                defer_storage_fsync
            );
        }

        let t_mvcc = profile.then(Instant::now);
        let commit_ts = self
            .tx_manager
            .commit(txn)
            .map_err(DurableTxnError::Transaction)?;
        if profile {
            eprintln!(
                "dnadb_commit_profile: tx_manager_commit {}µs",
                t_mvcc.expect("profile").elapsed().as_micros()
            );
        }

        let t_idx = profile.then(Instant::now);
        if let Some(pw) = pending_writes.as_ref() {
            self.apply_sort_index_writes(pw);
        }
        if profile {
            eprintln!(
                "dnadb_commit_profile: index_writes {}µs incremental={}",
                t_idx.expect("profile").elapsed().as_micros(),
                apply_incremental_indexes
            );
        }
        Ok(commit_ts)
    }

    /// Run MVCC version cleanup using active-snapshot safety.
    pub fn run_version_compaction(&mut self, min_versions_to_keep: usize) {
        self.tx_manager
            .compact_versions_with_floor(min_versions_to_keep.max(1));
    }

    pub fn record_version_count(&self, record_id: u64) -> usize {
        self.tx_manager.record_version_count(record_id)
    }

    fn replay_wal_to_mvcc(&mut self) -> Result<(), DurableTxnError> {
        let entries = self.wal.read_all_entries()?;
        for entry in entries {
            if let Ok(op) = serde_json::from_slice::<DurableOpPayload>(&entry.payload) {
                apply_replayed_op(&mut self.tx_manager, op);
            }
        }
        Ok(())
    }

    pub fn execute_wire_operation(
        &mut self,
        op: WireOperation,
    ) -> Result<ExecutionResult, DurableTxnError> {
        match op {
            WireOperation::Query(ast) => {
                let tx = self.begin();
                Ok(ExecutionResult::QueryRows(self.query(&tx, &ast)))
            }
            WireOperation::Insert(InsertOp { record, .. }) => {
                let mut tx = self.begin();
                let row = json_object_to_record(record)?;
                let id = record_id_from_record(&row).ok_or(DurableTxnError::Transaction(
                    "record must contain numeric `id` field",
                ))?;
                self.upsert(&mut tx, id, row);
                self.commit(&mut tx)?;
                Ok(ExecutionResult::AffectedRows(1))
            }
            WireOperation::Update(UpdateOp {
                filter, set_fields, ..
            }) => {
                let mut tx = self.begin();
                let ids: Vec<u64> = self
                    .tx_manager
                    .visible_records(&tx)
                    .into_iter()
                    .filter(|(_, r)| matches_record(r, &filter))
                    .filter_map(|(id, _)| Some(id))
                    .collect();
                for id in &ids {
                    if let Some(mut row) = self.read(&tx, *id) {
                        for (k, v) in &set_fields {
                            row.insert(k.clone(), v.clone());
                        }
                        self.upsert(&mut tx, *id, row);
                    }
                }
                if !ids.is_empty() {
                    self.commit(&mut tx)?;
                } else {
                    self.rollback(&mut tx)?;
                }
                Ok(ExecutionResult::AffectedRows(ids.len()))
            }
            WireOperation::Delete(DeleteOp { filter, .. }) => {
                let mut tx = self.begin();
                let ids: Vec<u64> = self
                    .tx_manager
                    .visible_records(&tx)
                    .into_iter()
                    .filter(|(_, r)| matches_record(r, &filter))
                    .map(|(id, _)| id)
                    .collect();
                for id in &ids {
                    self.delete(&mut tx, *id);
                }
                if !ids.is_empty() {
                    self.commit(&mut tx)?;
                } else {
                    self.rollback(&mut tx)?;
                }
                Ok(ExecutionResult::AffectedRows(ids.len()))
            }
        }
    }

    pub fn execute_insert_many(
        &mut self,
        records: Vec<Value>,
    ) -> Result<ExecutionResult, DurableTxnError> {
        self.execute_insert_many_with_mode(records, true, false)
    }

    pub fn execute_insert_many_with_mode(
        &mut self,
        records: Vec<Value>,
        rebuild_indexes: bool,
        defer_storage_fsync: bool,
    ) -> Result<ExecutionResult, DurableTxnError> {
        if records.is_empty() {
            return Ok(ExecutionResult::AffectedRows(0));
        }
        let affected = records.len();
        let mut tx = self.begin();
        for record in records {
            let row = json_object_to_record(record)?;
            let id = record_id_from_record(&row).ok_or(DurableTxnError::Transaction(
                "record must contain numeric `id` field",
            ))?;
            self.upsert(&mut tx, id, row);
        }
        // One full rebuild is far cheaper than per-row deindex + index for large batches
        // (especially for cold inserts where deindex is mostly wasted work).
        self.commit_inner(&mut tx, false, defer_storage_fsync)?;
        if rebuild_indexes {
            self.rebuild_sort_indexes()?;
        }
        Ok(ExecutionResult::AffectedRows(affected))
    }

    /// Same as [`Self::execute_insert_many_with_mode`] with `rebuild_indexes: false`, but uses
    /// **incremental** sort/exact/composite index updates via [`Self::commit_inner`] (`apply_incremental_indexes: true`).
    /// Prefer this for streaming materialization batches — avoids O(N) full [`Self::rebuild_sort_indexes`] each batch.
    pub fn execute_insert_many_incremental_indexes(
        &mut self,
        records: Vec<Value>,
        defer_storage_fsync: bool,
    ) -> Result<ExecutionResult, DurableTxnError> {
        if records.is_empty() {
            return Ok(ExecutionResult::AffectedRows(0));
        }
        let affected = records.len();
        let mut tx = self.begin();
        for record in records {
            let row = json_object_to_record(record)?;
            let id = record_id_from_record(&row).ok_or(DurableTxnError::Transaction(
                "record must contain numeric `id` field",
            ))?;
            self.upsert(&mut tx, id, row);
        }
        self.commit_inner(&mut tx, true, defer_storage_fsync)?;
        Ok(ExecutionResult::AffectedRows(affected))
    }

    /// Full storage durability: mmap flush + `sync_data` on strand files. Clears any deferred fsync state.
    pub fn sync_storage_to_disk(&mut self) -> Result<(), DurableTxnError> {
        self.storage.flush()?;
        self.pending_deferred_storage_sync = false;
        Ok(())
    }

    pub fn rebuild_indexes(&mut self) -> Result<(), DurableTxnError> {
        self.rebuild_sort_indexes()
    }

    fn flush_pending_deferred_storage_sync(&mut self) -> Result<(), DurableTxnError> {
        if self.pending_deferred_storage_sync {
            self.storage.flush()?;
            self.pending_deferred_storage_sync = false;
        }
        Ok(())
    }

    fn rebuild_sort_indexes(&mut self) -> Result<(), DurableTxnError> {
        self.sort_index_sealed.clear();
        self.sort_index_active.clear();
        self.sort_index_sealed_stale.clear();
        self.sort_index_values.clear();
        self.exact_string_indexes.clear();
        self.exact_string_index_values.clear();
        self.text_trigram_indexes.clear();
        self.text_trigram_index_values.clear();
        self.composite_sort_indexes.clear();
        self.composite_sort_index_values.clear();

        let tx = self.begin();
        let visible: Vec<(u64, crate::mvcc::Record)> = self.tx_manager.visible_records(&tx);

        for field in self.sort_index_fields.iter() {
            let mut v: Vec<SortIndexEntry> = Vec::new();
            for (record_id, record) in &visible {
                if let Some(key) = record.get(field.as_str()).and_then(value_to_sort_key) {
                    v.push(SortIndexEntry {
                        key,
                        record_id: *record_id,
                    });
                }
            }
            v.sort_unstable();
            if !v.is_empty() {
                self.sort_index_sealed.insert(field.clone(), v);
            }
        }
        for field in self.sort_index_fields.iter() {
            if let Some(vec) = self.sort_index_sealed.get(field) {
                let m = self.sort_index_values.entry(field.clone()).or_default();
                for e in vec {
                    m.insert(e.record_id, e.key);
                }
            }
        }

        for (record_id, record) in visible {
            self.index_exact_and_composite_only(record_id, &record);
        }

        self.flush_pending_deferred_storage_sync()?;
        Ok(())
    }

    fn index_exact_and_composite_only(
        &mut self,
        record_id: u64,
        record: &crate::mvcc::Record,
    ) {
        let mut exact_values_for_record = Vec::new();
        let mut trigram_values_for_record = Vec::new();
        for (field, value) in record {
            let Value::String(text) = value else {
                continue;
            };
            if !self.exact_string_index_fields.contains(field.as_str()) {
                continue;
            }
            let key = canonical_value_key(value);
            self.exact_string_indexes
                .entry(field.clone())
                .or_default()
                .entry(key.clone())
                .or_default()
                .insert(record_id);
            exact_values_for_record.push((field.clone(), key));
            let grams = trigrams_for_text(text);
            if !grams.is_empty() {
                let field_trigrams = self.text_trigram_indexes.entry(field.clone()).or_default();
                for gram in &grams {
                    field_trigrams
                        .entry(gram.clone())
                        .or_default()
                        .insert(record_id);
                }
                trigram_values_for_record.push((field.clone(), grams));
            }
        }
        if !exact_values_for_record.is_empty() {
            self.exact_string_index_values
                .insert(record_id, exact_values_for_record);
        }
        if !trigram_values_for_record.is_empty() {
            self.text_trigram_index_values
                .insert(record_id, trigram_values_for_record);
        }

        let defs: Vec<(String, String)> = self.composite_sort_index_defs.iter().cloned().collect();
        for (filter_field, order_field) in defs {
            let Some(filter_value) = record.get(filter_field.as_str()).map(canonical_value_key) else {
                continue;
            };
            let Some(sort_key) = record.get(order_field.as_str()).and_then(value_to_sort_key) else {
                continue;
            };
            self.composite_sort_indexes
                .entry((filter_field.clone(), order_field.clone()))
                .or_default()
                .entry(filter_value.clone())
                .or_default()
                .insert(SortIndexEntry {
                    key: sort_key,
                    record_id,
                });
            self.composite_sort_index_values
                .entry((filter_field, order_field))
                .or_default()
                .insert(record_id, (filter_value, sort_key));
        }
    }

    fn apply_sort_index_writes(&mut self, writes: &[WriteOp]) {
        for op in writes {
            match op {
                WriteOp::Upsert { record_id, record } => {
                    self.deindex_record(*record_id);
                    self.index_record_values(*record_id, record);
                }
                WriteOp::Delete { record_id } => {
                    self.deindex_record(*record_id);
                }
            }
        }
    }

    fn deindex_record(&mut self, record_id: u64) {
        let fields: Vec<String> = self.sort_index_fields.iter().cloned().collect();
        for field in fields {
            if let Some(prev_val) = self
                .sort_index_values
                .get_mut(field.as_str())
                .and_then(|m| m.remove(&record_id))
            {
                let removed_from_active = self
                    .sort_index_active
                    .get_mut(field.as_str())
                    .map(|entries| {
                        entries.remove(&SortIndexEntry {
                            key: prev_val,
                            record_id,
                        })
                    })
                    .unwrap_or(false);
                if !removed_from_active {
                    self.sort_index_sealed_stale
                        .entry(field)
                        .or_default()
                        .insert(record_id);
                }
            }
        }
        if let Some(items) = self.exact_string_index_values.remove(&record_id) {
            for (field, key) in items {
                if let Some(values) = self.exact_string_indexes.get_mut(&field) {
                    if let Some(ids) = values.get_mut(&key) {
                        ids.remove(&record_id);
                    }
                }
            }
        }
        if let Some(items) = self.text_trigram_index_values.remove(&record_id) {
            for (field, grams) in items {
                if let Some(postings) = self.text_trigram_indexes.get_mut(&field) {
                    for gram in grams {
                        if let Some(ids) = postings.get_mut(&gram) {
                            ids.remove(&record_id);
                        }
                    }
                }
            }
        }
        let defs: Vec<(String, String)> = self.composite_sort_index_defs.iter().cloned().collect();
        for def in defs {
            if let Some((bucket, prev_val)) = self
                .composite_sort_index_values
                .get_mut(&def)
                .and_then(|m| m.remove(&record_id))
            {
                if let Some(buckets) = self.composite_sort_indexes.get_mut(&def) {
                    if let Some(entries) = buckets.get_mut(&bucket) {
                        entries.remove(&SortIndexEntry {
                            key: prev_val,
                            record_id,
                        });
                    }
                }
            }
        }
    }

    fn index_record_values(&mut self, record_id: u64, record: &crate::mvcc::Record) {
        let fields: Vec<String> = self.sort_index_fields.iter().cloned().collect();
        for field in fields {
            let Some(key) = record.get(field.as_str()).and_then(value_to_sort_key) else {
                continue;
            };
            self.sort_index_active
                .entry(field.clone())
                .or_default()
                .insert(SortIndexEntry { key, record_id });
            self.sort_index_values
                .entry(field)
                .or_default()
                .insert(record_id, key);
        }
        self.index_exact_and_composite_only(record_id, record);
    }

    fn sort_index_query_rows(
        &self,
        txn: &Transaction,
        ast: &QueryAst,
    ) -> Option<Vec<crate::mvcc::Record>> {
        let order = ast.order_by.as_ref()?;
        let limit = ast.limit.map(|v| v as usize)?;
        if limit == 0 || ast.includes.len() > 0 {
            return None;
        }
        let sealed_slice: &[SortIndexEntry] = self
            .sort_index_sealed
            .get(&order.field)
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        let active_bt = self.sort_index_active.get(&order.field);
        let stale = self.sort_index_sealed_stale.get(&order.field);
        let active_empty = active_bt.map(|a| a.is_empty()).unwrap_or(true);
        if sealed_slice.is_empty() && active_empty {
            return None;
        }

        let mut out = Vec::with_capacity(limit);

        match order.direction {
            SortDirection::Asc => {
                let mut si = 0usize;
                let mut active_peek = active_bt.map(|a| a.iter().peekable());
                while out.len() < limit {
                    while si < sealed_slice.len() {
                        let e = &sealed_slice[si];
                        if stale.map_or(false, |s| s.contains(&e.record_id)) {
                            si += 1;
                        } else {
                            break;
                        }
                    }
                    let sealed_next =
                        (si < sealed_slice.len()).then_some(&sealed_slice[si]);
                    let active_next = active_peek.as_mut().and_then(|p| p.peek().copied());
                    let take_sealed = match (sealed_next, active_next) {
                        (Some(s), Some(a)) => *s < *a,
                        (Some(_), None) => true,
                        (None, Some(_)) => false,
                        (None, None) => break,
                    };
                    let entry = if take_sealed {
                        si += 1;
                        sealed_next.unwrap()
                    } else {
                        active_peek.as_mut().unwrap().next().unwrap()
                    };
                    if let Some(row) = self.tx_manager.read(txn, entry.record_id) {
                        if matches_record(&row, ast) {
                            out.push(row);
                        }
                    }
                }
            }
            SortDirection::Desc => {
                let mut si = sealed_slice.len();
                let mut active_peek = active_bt.map(|a| a.iter().rev().peekable());
                while out.len() < limit {
                    while si > 0 {
                        let e = &sealed_slice[si - 1];
                        if stale.map_or(false, |s| s.contains(&e.record_id)) {
                            si -= 1;
                        } else {
                            break;
                        }
                    }
                    let sealed_next = (si > 0).then(|| &sealed_slice[si - 1]);
                    let active_next = active_peek.as_mut().and_then(|p| p.peek().copied());
                    let take_sealed = match (sealed_next, active_next) {
                        // Desc: larger sort key first — prefer sealed when it strictly wins.
                        (Some(s), Some(a)) => *s > *a,
                        (Some(_), None) => true,
                        (None, Some(_)) => false,
                        (None, None) => break,
                    };
                    let entry = if take_sealed {
                        si -= 1;
                        sealed_next.unwrap()
                    } else {
                        active_peek.as_mut().unwrap().next().unwrap()
                    };
                    if let Some(row) = self.tx_manager.read(txn, entry.record_id) {
                        if matches_record(&row, ast) {
                            out.push(row);
                        }
                    }
                }
            }
        }
        Some(out)
    }

    fn composite_sort_index_query_rows(
        &self,
        txn: &Transaction,
        ast: &QueryAst,
    ) -> Option<Vec<crate::mvcc::Record>> {
        let order = ast.order_by.as_ref()?;
        let limit = ast.limit.map(|v| v as usize)?;
        if limit == 0 || ast.includes.len() > 0 {
            return None;
        }
        let eq_clause = ast
            .wheres
            .iter()
            .find(|w| w.op == WhereOp::Eq)?;
        let def = (eq_clause.field.clone(), order.field.clone());
        let buckets = self.composite_sort_indexes.get(&def)?;
        let filter_key = canonical_literal_key(&eq_clause.value);
        let entries = buckets.get(&filter_key)?;
        let mut out = Vec::with_capacity(limit);
        match order.direction {
            SortDirection::Asc => {
                for entry in entries {
                    if let Some(row) = self.tx_manager.read(txn, entry.record_id) {
                        if matches_record(&row, ast) {
                            out.push(row);
                            if out.len() >= limit {
                                break;
                            }
                        }
                    }
                }
            }
            SortDirection::Desc => {
                for entry in entries.iter().rev() {
                    if let Some(row) = self.tx_manager.read(txn, entry.record_id) {
                        if matches_record(&row, ast) {
                            out.push(row);
                            if out.len() >= limit {
                                break;
                            }
                        }
                    }
                }
            }
        }
        Some(out)
    }

    fn exact_string_index_query_rows(
        &self,
        txn: &Transaction,
        ast: &QueryAst,
    ) -> Option<Vec<crate::mvcc::Record>> {
        if ast.wheres.is_empty() {
            return None;
        }
        let mut candidate_ids: Option<HashSet<u64>> = None;
        for clause in &ast.wheres {
            if clause.op != WhereOp::Eq || !matches!(clause.value, QueryLiteral::String(_)) {
                return None;
            }
            let value_key = canonical_literal_key(&clause.value);
            let ids = self
                .exact_string_indexes
                .get(&clause.field)?
                .get(&value_key)?
                .clone();
            candidate_ids = Some(match candidate_ids {
                None => ids,
                Some(existing) => existing.intersection(&ids).copied().collect(),
            });
        }
        let mut rows = Vec::new();
        for id in candidate_ids.unwrap_or_default() {
            if let Some(row) = self.tx_manager.read(txn, id) {
                if matches_record(&row, ast) {
                    rows.push(row);
                }
            }
        }
        Some(rows)
    }

    /// Prefix-only `LIKE 'foo%'` on a field in [`Self::exact_string_index_fields`], using
    /// exact-string bucket keys (JSON string tokens) — avoids full visible scans when the
    /// index maps a manageable number of distinct values.
    fn prefix_string_index_query_rows(
        &self,
        txn: &Transaction,
        ast: &QueryAst,
    ) -> Option<Vec<crate::mvcc::Record>> {
        if ast.wheres.len() != 1 || !ast.includes.is_empty() {
            return None;
        }
        let w = ast.wheres.first()?;
        if w.op != WhereOp::Like {
            return None;
        }
        let QueryLiteral::String(pat) = &w.value else {
            return None;
        };
        let prefix = sql_like_prefix_literal(pat)?;
        if !self.exact_string_index_fields.contains(w.field.as_str()) {
            return None;
        }
        let field_map = self.exact_string_indexes.get(&w.field)?;
        let range_prefix = canonical_string_prefix_key(prefix)?;
        let range_hi = prefix_upper_bound(&range_prefix);
        let limit = ast.limit.map(|v| v as usize).unwrap_or(usize::MAX);
        let order_by_id = ast
            .order_by
            .as_ref()
            .filter(|o| o.field == "id")
            .map(|o| o.direction);

        // For `LIKE 'prefix%' ORDER BY id LIMIT N`, scanning the id-sort index and applying
        // the predicate in-id-order lets us stop once LIMIT is reached. This avoids scanning
        // every prefix-match candidate when the prefix fan-out is large (e.g. 1M-scale CMS).
        if order_by_id.is_some()
            && (self.sort_index_sealed.contains_key("id") || self.sort_index_active.contains_key("id"))
        {
            if let Some(rows) = self.sort_index_query_rows(txn, ast) {
                return Some(rows);
            }
        }

        // Prefix+LIMIT hot path:
        // - no ORDER BY: stop at LIMIT while scanning prefix key range
        // - ORDER BY id: keep only top-LIMIT ids in a bounded heap, then materialize those rows
        let mut ids = Vec::new();
        if let Some(direction) = order_by_id {
            match direction {
                SortDirection::Asc => {
                    let mut heap: BinaryHeap<u64> = BinaryHeap::new();
                    with_prefix_idsets(field_map, &range_prefix, range_hi.as_deref(), |idset| {
                        for id in idset {
                            heap.push(*id);
                            if heap.len() > limit {
                                heap.pop();
                            }
                        }
                        true
                    });
                    ids = heap.into_vec();
                    ids.sort_unstable();
                }
                SortDirection::Desc => {
                    let mut heap: BinaryHeap<Reverse<u64>> = BinaryHeap::new();
                    with_prefix_idsets(field_map, &range_prefix, range_hi.as_deref(), |idset| {
                        for id in idset {
                            heap.push(Reverse(*id));
                            if heap.len() > limit {
                                heap.pop();
                            }
                        }
                        true
                    });
                    ids = heap.into_iter().map(|v| v.0).collect();
                    ids.sort_unstable_by(|a, b| b.cmp(a));
                }
            }
        } else {
            with_prefix_idsets(field_map, &range_prefix, range_hi.as_deref(), |idset| {
                if ids.len() >= limit {
                    return false;
                }
                for id in idset {
                    ids.push(*id);
                    if ids.len() >= limit {
                        break;
                    }
                }
                ids.len() < limit
            });
        }
        let mut rows = Vec::new();
        for id in ids {
            if let Some(row) = self.tx_manager.read(txn, id) {
                if matches_record(&row, ast) {
                    rows.push(row);
                }
            }
        }
        Some(rows)
    }

    /// Contains-only `LIKE '%foo%'` on an exact-string-indexed field using trigram postings.
    fn contains_string_index_query_rows(
        &self,
        txn: &Transaction,
        ast: &QueryAst,
    ) -> Option<Vec<crate::mvcc::Record>> {
        if ast.wheres.len() != 1 || !ast.includes.is_empty() {
            return None;
        }
        let w = ast.wheres.first()?;
        if w.op != WhereOp::Like {
            return None;
        }
        let QueryLiteral::String(pat) = &w.value else {
            return None;
        };
        let needle = sql_like_contains_literal(pat)?;
        if !self.exact_string_index_fields.contains(w.field.as_str()) {
            return None;
        }
        let mut ids = candidate_ids_for_contains(self.text_trigram_indexes.get(&w.field)?, needle)?;
        if ids.is_empty() {
            return Some(Vec::new());
        }
        if let Some(limit) = ast.limit.map(|v| v as usize) {
            if ids.len() > limit.saturating_mul(8) {
                ids.truncate(limit.saturating_mul(8));
            }
        }
        let mut rows = Vec::new();
        for id in ids {
            if let Some(row) = self.tx_manager.read(txn, id) {
                if matches_record(&row, ast) {
                    rows.push(row);
                }
            }
        }
        Some(rows)
    }

    fn choose_fast_path(&self, ast: &QueryAst) -> Option<FastPathKind> {
        let mut candidates: Vec<(usize, FastPathKind)> = Vec::new();
        if let Some(cost) = self.estimate_composite_sort_cost(ast) {
            candidates.push((cost, FastPathKind::CompositeSort));
        }
        if let Some(cost) = self.estimate_exact_index_cost(ast) {
            candidates.push((cost, FastPathKind::ExactIndex));
        }
        if let Some(cost) = self.estimate_prefix_string_index_cost(ast) {
            candidates.push((cost, FastPathKind::PrefixStringIndex));
        }
        if let Some(cost) = self.estimate_contains_string_index_cost(ast) {
            candidates.push((cost, FastPathKind::ContainsStringIndex));
        }
        if let Some(cost) = self.estimate_sort_index_cost(ast) {
            candidates.push((cost, FastPathKind::SortIndex));
        }
        candidates.sort_by_key(|(cost, _)| *cost);
        candidates.first().map(|(_, kind)| *kind)
    }

    fn estimate_composite_sort_cost(&self, ast: &QueryAst) -> Option<usize> {
        let order = ast.order_by.as_ref()?;
        let limit = ast.limit.map(|v| v as usize)?;
        if limit == 0 || ast.includes.len() > 0 {
            return None;
        }
        let eq_clause = ast.wheres.iter().find(|w| w.op == WhereOp::Eq)?;
        let def = (eq_clause.field.clone(), order.field.clone());
        let buckets = self.composite_sort_indexes.get(&def)?;
        let filter_key = canonical_literal_key(&eq_clause.value);
        let bucket_len = buckets.get(&filter_key)?.len();
        Some(bucket_len.min(limit.saturating_mul(4)).max(1))
    }

    fn estimate_exact_index_cost(&self, ast: &QueryAst) -> Option<usize> {
        // Exact-equality fast path is best for unordered fetches.
        // For ordered queries, composite/sort paths avoid large in-memory sorts.
        if ast.wheres.is_empty() || ast.includes.len() > 0 || ast.order_by.is_some() {
            return None;
        }
        let mut min_bucket_len = usize::MAX;
        for clause in &ast.wheres {
            if clause.op != WhereOp::Eq || !matches!(clause.value, QueryLiteral::String(_)) {
                return None;
            }
            let key = canonical_literal_key(&clause.value);
            let bucket_len = self
                .exact_string_indexes
                .get(&clause.field)?
                .get(&key)?
                .len();
            min_bucket_len = min_bucket_len.min(bucket_len);
        }
        if min_bucket_len == usize::MAX {
            return None;
        }
        let limit = ast.limit.map(|v| v as usize).unwrap_or(min_bucket_len);
        Some(min_bucket_len.min(limit).max(1))
    }

    fn estimate_sort_index_cost(&self, ast: &QueryAst) -> Option<usize> {
        let order = ast.order_by.as_ref()?;
        let limit = ast.limit.map(|v| v as usize)?;
        if limit == 0 || ast.includes.len() > 0 {
            return None;
        }
        let sealed_n = self
            .sort_index_sealed
            .get(&order.field)
            .map(|v| v.len())
            .unwrap_or(0);
        let active_n = self
            .sort_index_active
            .get(&order.field)
            .map(|b| b.len())
            .unwrap_or(0);
        let entries_n = sealed_n + active_n;
        if entries_n == 0 {
            return None;
        }
        if ast.wheres.is_empty() {
            return Some(limit.max(1));
        }
        let mut estimated = entries_n;
        for clause in &ast.wheres {
            if clause.op != WhereOp::Eq || !matches!(clause.value, QueryLiteral::String(_)) {
                continue;
            }
            let key = canonical_literal_key(&clause.value);
            if let Some(bucket_len) = self
                .exact_string_indexes
                .get(&clause.field)
                .and_then(|m| m.get(&key))
                .map(|ids| ids.len())
            {
                estimated = estimated.min(bucket_len.saturating_mul(2));
            }
        }
        Some(estimated.max(limit).max(1))
    }

    fn estimate_prefix_string_index_cost(&self, ast: &QueryAst) -> Option<usize> {
        let limit = ast.limit.map(|v| v as usize)?;
        if ast.wheres.len() != 1 || !ast.includes.is_empty() {
            return None;
        }
        if ast
            .order_by
            .as_ref()
            .is_some_and(|o| o.field == "id")
            && (self.sort_index_sealed.contains_key("id") || self.sort_index_active.contains_key("id"))
        {
            // Prefer id-sort scan for ORDER BY id prefix queries; it can early-stop at LIMIT
            // without enumerating all prefix candidates.
            return None;
        }
        let w = ast.wheres.first()?;
        if w.op != WhereOp::Like {
            return None;
        }
        let QueryLiteral::String(pat) = &w.value else {
            return None;
        };
        let prefix = sql_like_prefix_literal(pat)?;
        if !self.exact_string_index_fields.contains(w.field.as_str()) {
            return None;
        }
        let field_map = self.exact_string_indexes.get(&w.field)?;
        let range_prefix = canonical_string_prefix_key(prefix)?;
        let range_hi = prefix_upper_bound(&range_prefix);
        let mut matched_rows = 0usize;
        match range_hi {
            Some(hi) => {
                for (_, idset) in field_map.range(range_prefix..hi) {
                    matched_rows = matched_rows.saturating_add(idset.len());
                }
            }
            None => {
                for (_, idset) in field_map.range(range_prefix..) {
                    matched_rows = matched_rows.saturating_add(idset.len());
                }
            }
        }
        if matched_rows == 0 {
            return None;
        }
        Some(matched_rows.min(limit.saturating_mul(4)).max(1))
    }

    fn estimate_contains_string_index_cost(&self, ast: &QueryAst) -> Option<usize> {
        let limit = ast.limit.map(|v| v as usize)?;
        if ast.wheres.len() != 1 || !ast.includes.is_empty() {
            return None;
        }
        let w = ast.wheres.first()?;
        if w.op != WhereOp::Like {
            return None;
        }
        let QueryLiteral::String(pat) = &w.value else {
            return None;
        };
        let needle = sql_like_contains_literal(pat)?;
        let postings = self.text_trigram_indexes.get(&w.field)?;
        let grams = trigrams_for_text(needle);
        if grams.is_empty() {
            return None;
        }
        let mut min_posting = usize::MAX;
        for gram in grams {
            let len = postings.get(&gram).map(|ids| ids.len()).unwrap_or(0);
            if len == 0 {
                return Some(1);
            }
            min_posting = min_posting.min(len);
        }
        if min_posting == usize::MAX {
            return None;
        }
        Some(min_posting.min(limit.saturating_mul(4)).max(1))
    }

    fn with_default_order_for_limit(&self, ast: &QueryAst) -> QueryAst {
        if ast.limit.is_none() || ast.order_by.is_some() {
            return ast.clone();
        }
        // Single-key `id = …` point reads must not pick up a synthetic ORDER BY,
        // or we lose the direct MVCC fast path.
        if direct_lookup_id(ast).is_some() {
            return ast.clone();
        }
        // Single natural-key string lookups (`slug`, `title`, …) should use the
        // exact-string index without a synthetic sort (order is irrelevant at LIMIT 1).
        if ast.wheres.len() == 1 {
            let w = &ast.wheres[0];
            if w.op == WhereOp::Eq && matches!(w.value, QueryLiteral::String(_)) {
                if self.exact_string_index_fields.contains(w.field.as_str()) {
                    return ast.clone();
                }
            }
        }
        let mut out = ast.clone();
        if self.sort_index_fields.contains("created_at") {
            out = out.order_by("created_at", SortDirection::Desc);
        } else if self.sort_index_fields.contains("updated_at") {
            out = out.order_by("updated_at", SortDirection::Desc);
        } else if let Some(first_field) = self.sort_index_fields.iter().min() {
            out = out.order_by(first_field.clone(), SortDirection::Desc);
        }
        out
    }
}

fn value_to_sort_key(value: &Value) -> Option<i64> {
    match value {
        Value::Number(n) => n.as_i64().or_else(|| n.as_u64().and_then(|v| i64::try_from(v).ok())),
        _ => None,
    }
}

fn default_exact_string_index_field_set() -> HashSet<String> {
    DEFAULT_EXACT_STRING_INDEX_FIELDS
        .iter()
        .map(|s| (*s).to_string())
        .collect()
}

fn exact_string_fields_from_file_option(exact: Option<Vec<String>>) -> HashSet<String> {
    match exact {
        None => default_exact_string_index_field_set(),
        Some(v) => v
            .into_iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
    }
}

fn load_sort_index_config(
    path: &Path,
) -> Result<Option<(Vec<String>, Vec<[String; 2]>, HashSet<String>)>, DurableTxnError> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(path)?;
    let cfg: SortIndexConfigFile = serde_json::from_slice(&bytes)?;
    let fields = cfg
        .fields
        .into_iter()
        .map(|f| f.trim().to_string())
        .filter(|f| !f.is_empty())
        .collect::<Vec<_>>();
    let composite_fields = cfg
        .composite_fields
        .into_iter()
        .map(|[a, b]| [a.trim().to_string(), b.trim().to_string()])
        .filter(|[a, b]| !a.is_empty() && !b.is_empty())
        .collect::<Vec<_>>();
    let exact = exact_string_fields_from_file_option(cfg.exact_string_fields);
    Ok(Some((fields, composite_fields, exact)))
}

fn save_sort_index_config(
    path: &Path,
    fields: &HashSet<String>,
    composites: &HashSet<(String, String)>,
    exact_string_fields: &HashSet<String>,
) -> Result<(), DurableTxnError> {
    let mut sorted: Vec<String> = fields
        .iter()
        .map(|f| f.trim().to_string())
        .filter(|f| !f.is_empty())
        .collect();
    sorted.sort();
    let mut composite_fields: Vec<[String; 2]> = composites
        .iter()
        .map(|(a, b)| [a.trim().to_string(), b.trim().to_string()])
        .filter(|[a, b]| !a.is_empty() && !b.is_empty())
        .collect();
    composite_fields.sort();
    let mut exact_sorted: Vec<String> = exact_string_fields
        .iter()
        .map(|f| f.trim().to_string())
        .filter(|f| !f.is_empty())
        .collect();
    exact_sorted.sort();
    let cfg = SortIndexConfigFile {
        fields: sorted,
        composite_fields,
        exact_string_fields: Some(exact_sorted),
    };
    let bytes = serde_json::to_vec_pretty(&cfg)?;
    fs::write(path, bytes)?;
    Ok(())
}

fn build_snapshot_indexes(visible: &[(u64, crate::mvcc::Record)]) -> SnapshotIndexes {
    let mut out = SnapshotIndexes::default();
    for (id, record) in visible {
        out.direct_by_id.insert(*id, record.clone());
        out.all_records_by_id.insert(*id, record.clone());
        for (field, value) in record {
            let key = canonical_value_key(value);
            out.exact_by_field
                .entry(field.clone())
                .or_default()
                .entry(key)
                .or_default()
                .push(*id);
            if let Some(num) = value.as_f64() {
                out.range_by_field
                    .entry(field.clone())
                    .or_default()
                    .push((num, *id));
            }
        }
    }
    for values in out.range_by_field.values_mut() {
        values.sort_by(|a, b| a.0.total_cmp(&b.0));
    }
    out
}

fn build_snapshot_collection_stats(ast: &QueryAst, indexes: &SnapshotIndexes) -> CollectionStats {
    let mut stats = CollectionStats::fake();
    stats.record_count = indexes.all_records_by_id.len();
    stats.direct_lookup_fields = ["id".to_string()].into_iter().collect();
    let mut indexed_fields = HashSet::new();
    for w in &ast.wheres {
        if indexes.exact_by_field.contains_key(&w.field) || indexes.range_by_field.contains_key(&w.field) {
            indexed_fields.insert(w.field.clone());
        }
    }
    stats.indexed_fields = indexed_fields;
    for field in &stats.indexed_fields {
        let distinct = indexes
            .exact_by_field
            .get(field)
            .map(|m| m.len().max(1))
            .unwrap_or(1);
        stats
            .index_selectivity
            .insert(field.clone(), 1.0 / distinct as f64);
    }
    stats
}

fn direct_query_rows(ast: &QueryAst, indexes: &SnapshotIndexes) -> Vec<crate::mvcc::Record> {
    let Some(id) = direct_lookup_id(ast) else {
        return vec![];
    };
    indexes
        .direct_by_id
        .get(&id)
        .filter(|r| matches_record(r, ast))
        .cloned()
        .into_iter()
        .collect()
}

fn indexed_query_rows(ast: &QueryAst, indexes: &SnapshotIndexes) -> Vec<crate::mvcc::Record> {
    let Some(where_clause) = ast.wheres.iter().find(|w| {
        indexes.exact_by_field.contains_key(&w.field) || indexes.range_by_field.contains_key(&w.field)
    }) else {
        return indexes
            .all_records_by_id
            .values()
            .filter(|r| matches_record(r, ast))
            .cloned()
            .collect();
    };

    let candidate_ids: Vec<u64> = match where_clause.op {
        WhereOp::Eq => {
            let key = canonical_literal_key(&where_clause.value);
            indexes
                .exact_by_field
                .get(&where_clause.field)
                .and_then(|m| m.get(&key))
                .cloned()
                .unwrap_or_default()
        }
        WhereOp::Gt | WhereOp::Gte | WhereOp::Lt | WhereOp::Lte | WhereOp::Ne => {
            let Some(operand) = literal_as_f64(&where_clause.value) else {
                return indexes
                    .all_records_by_id
                    .values()
                    .filter(|r| matches_record(r, ast))
                    .cloned()
                    .collect();
            };
            indexes
                .range_by_field
                .get(&where_clause.field)
                .into_iter()
                .flat_map(|entries| entries.iter())
                .filter_map(|(val, id)| {
                    let op = match where_clause.op {
                        WhereOp::Gt => RangeOp::GreaterThan,
                        WhereOp::Gte => RangeOp::GreaterOrEqual,
                        WhereOp::Lt => RangeOp::LessThan,
                        WhereOp::Lte => RangeOp::LessOrEqual,
                        WhereOp::Ne => RangeOp::NotEqual,
                        _ => return None,
                    };
                    if numeric_range_match(*val, operand, op) {
                        Some(*id)
                    } else {
                        None
                    }
                })
                .collect()
        }
        WhereOp::Like => indexes
            .all_records_by_id
            .keys()
            .copied()
            .collect(),
    };

    candidate_ids
        .into_iter()
        .filter_map(|id| indexes.all_records_by_id.get(&id).cloned())
        .filter(|r| matches_record(r, ast))
        .collect()
}

fn direct_lookup_id(ast: &QueryAst) -> Option<u64> {
    if ast.wheres.len() != 1 {
        return None;
    }
    let clause = ast.wheres.first()?;
    if clause.field != "id" || clause.op != WhereOp::Eq || ast.order_by.is_some() || ast.includes.len() > 0 {
        return None;
    }
    match clause.value {
        QueryLiteral::U64(v) => Some(v),
        QueryLiteral::I64(v) => u64::try_from(v).ok(),
        _ => None,
    }
}

fn canonical_value_key(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| format!("{value:?}"))
}

fn canonical_literal_key(lit: &QueryLiteral) -> String {
    match lit {
        QueryLiteral::String(s) => format!("\"{s}\""),
        QueryLiteral::I64(v) => v.to_string(),
        QueryLiteral::U64(v) => v.to_string(),
        QueryLiteral::F64(v) => v.to_string(),
        QueryLiteral::Bool(v) => v.to_string(),
    }
}

fn literal_as_f64(lit: &QueryLiteral) -> Option<f64> {
    match lit {
        QueryLiteral::I64(v) => Some(*v as f64),
        QueryLiteral::U64(v) => Some(*v as f64),
        QueryLiteral::F64(v) => Some(*v),
        _ => None,
    }
}

fn numeric_range_match(value: f64, operand: f64, op: RangeOp) -> bool {
    match op {
        RangeOp::GreaterThan => value > operand,
        RangeOp::GreaterOrEqual => value >= operand,
        RangeOp::LessThan => value < operand,
        RangeOp::LessOrEqual => value <= operand,
        RangeOp::NotEqual => value != operand,
    }
}

fn json_object_to_record(value: Value) -> Result<crate::mvcc::Record, DurableTxnError> {
    let Value::Object(map) = value else {
        return Err(DurableTxnError::Transaction("insert record must be object"));
    };
    Ok(map.into_iter().collect())
}

fn record_id_from_record(record: &crate::mvcc::Record) -> Option<u64> {
    match record.get("id") {
        Some(Value::Number(n)) => n.as_u64().or_else(|| n.as_i64().and_then(|v| u64::try_from(v).ok())),
        _ => None,
    }
}

fn trigrams_for_text(value: &str) -> Vec<String> {
    let chars: Vec<char> = value.chars().collect();
    if chars.len() < 3 {
        return Vec::new();
    }
    let mut grams = HashSet::new();
    for i in 0..=(chars.len() - 3) {
        let gram: String = chars[i..i + 3].iter().collect();
        grams.insert(gram);
    }
    let mut out: Vec<String> = grams.into_iter().collect();
    out.sort();
    out
}

fn candidate_ids_for_contains(
    postings: &BTreeMap<String, HashSet<u64>>,
    needle: &str,
) -> Option<Vec<u64>> {
    let grams = trigrams_for_text(needle);
    if grams.is_empty() {
        return None;
    }
    let mut acc: Option<HashSet<u64>> = None;
    for gram in grams {
        let ids = postings.get(&gram)?;
        acc = Some(match acc {
            None => ids.clone(),
            Some(existing) => existing.intersection(ids).copied().collect(),
        });
    }
    let mut out: Vec<u64> = acc.unwrap_or_default().into_iter().collect();
    out.sort_unstable();
    Some(out)
}

fn canonical_string_prefix_key(prefix: &str) -> Option<String> {
    let mut s = serde_json::to_string(prefix).ok()?;
    if s.pop() != Some('"') {
        return None;
    }
    Some(s)
}

fn prefix_upper_bound(prefix: &str) -> Option<String> {
    let mut bytes = prefix.as_bytes().to_vec();
    for i in (0..bytes.len()).rev() {
        if bytes[i] != 0xFF {
            bytes[i] = bytes[i].saturating_add(1);
            bytes.truncate(i + 1);
            return String::from_utf8(bytes).ok();
        }
    }
    None
}

fn with_prefix_idsets<F>(
    field_map: &BTreeMap<String, HashSet<u64>>,
    range_prefix: &str,
    range_hi: Option<&str>,
    mut f: F,
) where
    F: FnMut(&HashSet<u64>) -> bool,
{
    match range_hi {
        Some(hi) => {
            for (_, idset) in field_map.range(range_prefix.to_string()..hi.to_string()) {
                if !f(idset) {
                    break;
                }
            }
        }
        None => {
            for (_, idset) in field_map.range(range_prefix.to_string()..) {
                if !f(idset) {
                    break;
                }
            }
        }
    }
}

fn matches_record(record: &crate::mvcc::Record, ast: &QueryAst) -> bool {
    ast.wheres.iter().all(|w| {
        let Some(value) = record.get(&w.field) else {
            return false;
        };
        literal_matches(value, w.op, &w.value)
    })
}

fn literal_matches(value: &Value, op: WhereOp, lit: &QueryLiteral) -> bool {
    match (value, lit) {
        (Value::String(s), QueryLiteral::String(q)) => match op {
            WhereOp::Eq => s == q,
            WhereOp::Ne => s != q,
            WhereOp::Like => like_matches(s, q),
            WhereOp::Gt => s > q,
            WhereOp::Gte => s >= q,
            WhereOp::Lt => s < q,
            WhereOp::Lte => s <= q,
        },
        (Value::Bool(b), QueryLiteral::Bool(q)) => match op {
            WhereOp::Eq => b == q,
            WhereOp::Ne => b != q,
            _ => false,
        },
        (Value::Number(n), QueryLiteral::U64(q)) => n
            .as_u64()
            .is_some_and(|v| cmp_ord(v as f64, *q as f64, op)),
        (Value::Number(n), QueryLiteral::I64(q)) => n
            .as_i64()
            .is_some_and(|v| cmp_ord(v as f64, *q as f64, op)),
        (Value::Number(n), QueryLiteral::F64(q)) => n
            .as_f64()
            .is_some_and(|v| cmp_ord(v, *q, op)),
        _ => false,
    }
}

fn cmp_ord(left: f64, right: f64, op: WhereOp) -> bool {
    match op {
        WhereOp::Eq => left == right,
        WhereOp::Ne => left != right,
        WhereOp::Gt => left > right,
        WhereOp::Gte => left >= right,
        WhereOp::Lt => left < right,
        WhereOp::Lte => left <= right,
        WhereOp::Like => false,
    }
}

fn apply_sort_and_limit(rows: &mut Vec<crate::mvcc::Record>, ast: &QueryAst) {
    if let Some(order) = &ast.order_by {
        rows.sort_by(|a, b| {
            let va = a.get(&order.field).cloned().unwrap_or(Value::Null);
            let vb = b.get(&order.field).cloned().unwrap_or(Value::Null);
            let ord = format!("{va:?}").cmp(&format!("{vb:?}"));
            match order.direction {
                SortDirection::Asc => ord,
                SortDirection::Desc => ord.reverse(),
            }
        });
    }
    if let Some(limit) = ast.limit {
        let n = limit as usize;
        if rows.len() > n {
            rows.truncate(n);
        }
    }
}

fn like_matches(value: &str, pattern: &str) -> bool {
    let v: Vec<char> = value.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    let mut dp = vec![vec![false; v.len() + 1]; p.len() + 1];
    dp[0][0] = true;
    for i in 1..=p.len() {
        if p[i - 1] == '%' {
            dp[i][0] = dp[i - 1][0];
        }
    }
    for i in 1..=p.len() {
        for j in 1..=v.len() {
            dp[i][j] = match p[i - 1] {
                '%' => dp[i - 1][j] || dp[i][j - 1],
                '_' => dp[i - 1][j - 1],
                c => dp[i - 1][j - 1] && c == v[j - 1],
            };
        }
    }
    dp[p.len()][v.len()]
}

fn durable_payload_from_write_op(txn_id: u64, op: &WriteOp) -> Result<Vec<u8>, DurableTxnError> {
    let payload = match op {
        WriteOp::Upsert { record_id, record } => DurableOpPayload::Upsert {
            txn_id,
            record_id: *record_id,
            record: record.clone(),
        },
        WriteOp::Delete { record_id } => DurableOpPayload::Delete {
            txn_id,
            record_id: *record_id,
        },
    };
    Ok(serde_json::to_vec(&payload)?)
}

fn apply_replayed_op(txm: &mut TransactionManager, op: DurableOpPayload) {
    let mut tx = txm.begin();
    match op {
        DurableOpPayload::Upsert {
            record_id, record, ..
        } => txm.upsert(&mut tx, record_id, record),
        DurableOpPayload::Delete { record_id, .. } => txm.delete(&mut tx, record_id),
    }
    let _ = txm.commit(&mut tx);
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use serde_json::json;
    use tempfile::tempdir;

    use super::json_to_record;
    use super::DurableTransactionStore;
    use super::ExecutionResult;
    use super::SnapshotQueryPlan;
    use crate::mvcc::Record;
    use crate::query::{QueryAst, QueryLiteral, SortDirection, WhereOp};
    use crate::wire::{InsertOp, WireOperation};

    fn record(v: u64) -> Record {
        let mut r = Record::new();
        r.insert("v".to_string(), json!(v));
        r
    }

    #[test]
    fn durable_commit_survives_reopen() {
        let dir = tempdir().expect("tempdir");
        let root: PathBuf = dir.path().to_path_buf();
        {
            let mut store = DurableTransactionStore::open_or_create(&root, "users", 1, Some(1024 * 1024))
                .expect("open");
            let mut tx = store.begin();
            store.upsert(&mut tx, 7, record(1));
            store.commit(&mut tx).expect("commit");
        }
        let mut reopened =
            DurableTransactionStore::open_or_create(&root, "users", 1, Some(1024 * 1024))
                .expect("reopen");
        let reader = reopened.begin();
        let row = reopened.read(&reader, 7).expect("row after reopen");
        assert_eq!(row.get("v"), Some(&json!(1)));
    }

    #[test]
    fn rollback_does_not_persist() {
        let dir = tempdir().expect("tempdir");
        let root: PathBuf = dir.path().to_path_buf();
        let mut store =
            DurableTransactionStore::open_or_create(&root, "users", 1, Some(1024 * 1024))
                .expect("open");
        let mut tx = store.begin();
        store.upsert(&mut tx, 8, record(2));
        store.rollback(&mut tx).expect("rollback");
        let reader = store.begin();
        assert!(store.read(&reader, 8).is_none());
    }

    #[test]
    fn exact_string_index_fields_persist_across_reopen() {
        let dir = tempdir().expect("tempdir");
        let root: PathBuf = dir.path().to_path_buf();
        {
            let mut store =
                DurableTransactionStore::open_or_create(&root, "posts", 1, Some(1024 * 1024))
                    .expect("open");
            store
                .configure_exact_string_index_fields(&["slug".to_string()])
                .expect("configure exact");
            let mut tx = store.begin();
            store.upsert(
                &mut tx,
                1,
                json_to_record(json!({"id": 1u64, "slug": "a", "body": "not-indexed"})),
            );
            store.commit(&mut tx).expect("commit");
        }
        let mut reopened =
            DurableTransactionStore::open_or_create(&root, "posts", 1, Some(1024 * 1024))
                .expect("reopen");
        assert_eq!(reopened.exact_string_index_fields(), vec!["slug".to_string()]);
        let reader = reopened.begin();
        let ast = QueryAst::new("posts").r#where(
            "slug",
            WhereOp::Eq,
            QueryLiteral::String("a".to_string()),
        );
        let (rows, _) = reopened.query_with_plan(&reader, &ast);
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn sort_index_sealed_plus_active_merge_desc() {
        let dir = tempdir().expect("tempdir");
        let root: PathBuf = dir.path().to_path_buf();
        let mut store =
            DurableTransactionStore::open_or_create(&root, "posts", 1, Some(1024 * 1024))
                .expect("open");
        store
            .configure_sort_indexes(&["updated_at".to_string()])
            .expect("idx");
        let mut tx = store.begin();
        for (id, ts) in [(1u64, 100i64), (2u64, 300i64), (3u64, 200i64)] {
            store.upsert(
                &mut tx,
                id,
                json_to_record(json!({"id": id, "updated_at": ts})),
            );
        }
        store.commit(&mut tx).expect("c");
        store.rebuild_indexes().expect("rebuild");
        let mut tx2 = store.begin();
        store.upsert(
            &mut tx2,
            1,
            json_to_record(json!({"id": 1u64, "updated_at": 400i64})),
        );
        store.commit(&mut tx2).expect("c2");

        let reader = store.begin();
        let ast = QueryAst::new("posts")
            .order_by("updated_at", SortDirection::Desc)
            .limit(3);
        let (rows, _) = store.query_with_plan(&reader, &ast);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].get("id"), Some(&json!(1u64)));
        assert_eq!(rows[1].get("id"), Some(&json!(2u64)));
        assert_eq!(rows[2].get("id"), Some(&json!(3u64)));
    }

    #[test]
    fn insert_many_rebuild_indexes_so_ordered_queries_work() {
        let dir = tempdir().expect("tempdir");
        let root: PathBuf = dir.path().to_path_buf();
        let mut store =
            DurableTransactionStore::open_or_create(&root, "posts", 1, Some(1024 * 1024))
                .expect("open");
        store
            .configure_sort_indexes(&["updated_at".to_string()])
            .expect("indexes");
        let docs = vec![
            json!({"id": 1u64, "updated_at": 100i64, "slug": "a"}),
            json!({"id": 2u64, "updated_at": 300i64, "slug": "b"}),
            json!({"id": 3u64, "updated_at": 200i64, "slug": "c"}),
        ];
        store.execute_insert_many(docs).expect("bulk");
        let reader = store.begin();
        let ast = QueryAst::new("posts")
            .order_by("updated_at", SortDirection::Desc)
            .limit(2);
        let (rows, _) = store.query_with_plan(&reader, &ast);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("id"), Some(&json!(2u64)));
        assert_eq!(rows[1].get("id"), Some(&json!(3u64)));
    }

    #[test]
    fn wire_insert_and_query_uses_transactional_visibility() {
        let dir = tempdir().expect("tempdir");
        let root: PathBuf = dir.path().to_path_buf();
        let mut store =
            DurableTransactionStore::open_or_create(&root, "users", 1, Some(1024 * 1024))
                .expect("open");

        let insert = WireOperation::Insert(InsertOp {
            collection: "users".to_string(),
            record: json!({"id": 42, "email": "alice@example.com", "age": 30}),
        });
        let inserted = store.execute_wire_operation(insert).expect("insert");
        assert_eq!(inserted, ExecutionResult::AffectedRows(1));

        let query = QueryAst::new("users")
            .r#where(
                "email",
                WhereOp::Eq,
                QueryLiteral::String("alice@example.com".to_string()),
            )
            .limit(1);
        let out = store
            .execute_wire_operation(WireOperation::Query(query))
            .expect("query");
        match out {
            ExecutionResult::QueryRows(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].get("id"), Some(&json!(42)));
            }
            _ => panic!("expected query rows"),
        }
    }

    #[test]
    fn compaction_keeps_snapshot_visible_and_respects_floor() {
        let dir = tempdir().expect("tempdir");
        let root: PathBuf = dir.path().to_path_buf();
        let mut store =
            DurableTransactionStore::open_or_create(&root, "users", 1, Some(1024 * 1024))
                .expect("open");

        let mut t1 = store.begin();
        store.upsert(&mut t1, 5, json_to_record(json!({"id": 5, "v": 1})));
        store.commit(&mut t1).expect("c1");
        let old_reader = store.begin();

        let mut t2 = store.begin();
        store.upsert(&mut t2, 5, json_to_record(json!({"id": 5, "v": 2})));
        store.commit(&mut t2).expect("c2");
        let mut t3 = store.begin();
        store.upsert(&mut t3, 5, json_to_record(json!({"id": 5, "v": 3})));
        store.commit(&mut t3).expect("c3");

        store.run_version_compaction(2);
        assert!(store.record_version_count(5) >= 2);
        let old = store.read(&old_reader, 5).expect("old visible");
        assert_eq!(old.get("v"), Some(&json!(1)));
        let new_reader = store.begin();
        let newest = store.read(&new_reader, 5).expect("new visible");
        assert_eq!(newest.get("v"), Some(&json!(3)));
    }

    #[test]
    fn snapshot_query_uses_direct_plan_for_id_lookup() {
        let dir = tempdir().expect("tempdir");
        let root: PathBuf = dir.path().to_path_buf();
        let mut store =
            DurableTransactionStore::open_or_create(&root, "users", 1, Some(1024 * 1024))
                .expect("open");
        let mut txw = store.begin();
        store.upsert(&mut txw, 101, json_to_record(json!({"id": 101, "email": "d@x.com"})));
        store.commit(&mut txw).expect("commit");
        let reader = store.begin();
        let ast = QueryAst::new("users").r#where("id", WhereOp::Eq, QueryLiteral::U64(101));
        let (rows, plan) = store.query_with_plan(&reader, &ast);
        assert_eq!(plan, SnapshotQueryPlan::Direct);
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn snapshot_query_uses_index_plan_for_selective_field() {
        let dir = tempdir().expect("tempdir");
        let root: PathBuf = dir.path().to_path_buf();
        let mut store =
            DurableTransactionStore::open_or_create(&root, "users", 1, Some(1024 * 1024))
                .expect("open");
        let mut tx = store.begin();
        for i in 0..100u64 {
            let domain = if i == 77 { "rare.test" } else { "common.test" };
            store.upsert(
                &mut tx,
                i,
                json_to_record(json!({"id": i, "email": format!("u{i}@{domain}")})),
            );
        }
        store.commit(&mut tx).expect("seed commit");
        let reader = store.begin();
        let ast = QueryAst::new("users").r#where(
            "email",
            WhereOp::Eq,
            QueryLiteral::String("u77@rare.test".to_string()),
        );
        let (rows, plan) = store.query_with_plan(&reader, &ast);
        assert!(
            matches!(plan, SnapshotQueryPlan::Index | SnapshotQueryPlan::ExactIndex),
            "unexpected plan: {plan:?}"
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("id"), Some(&json!(77)));
    }

    #[test]
    fn prefix_like_slug_uses_exact_string_index_fast_path() {
        let dir = tempdir().expect("tempdir");
        let root: PathBuf = dir.path().to_path_buf();
        let mut store =
            DurableTransactionStore::open_or_create(&root, "posts", 1, Some(1024 * 1024))
                .expect("open");
        store
            .configure_sort_indexes(&["updated_at".to_string()])
            .expect("sort idx");
        let docs: Vec<_> = (1..=50u64)
            .map(|i| {
                json!({
                    "id": i,
                    "updated_at": (1000 + i as i64),
                    "slug": format!("post-{i}"),
                })
            })
            .collect();
        store.execute_insert_many(docs).expect("bulk");
        let reader = store.begin();
        let ast = QueryAst::new("posts")
            .r#where(
                "slug",
                WhereOp::Like,
                QueryLiteral::String("post-4%".to_string()),
            )
            .limit(20);
        let (rows, plan) = store.query_with_plan(&reader, &ast);
        assert_eq!(plan, SnapshotQueryPlan::ExactIndex);
        assert_eq!(rows.len(), 11, "post-4 + post-40..post-49");
        for r in &rows {
            let slug = r.get("slug").and_then(|v| v.as_str()).expect("slug");
            assert!(slug.starts_with("post-4"), "unexpected slug {slug}");
        }
    }

    #[test]
    fn contains_like_uses_trigram_fast_path() {
        let dir = tempdir().expect("tempdir");
        let root: PathBuf = dir.path().to_path_buf();
        let mut store =
            DurableTransactionStore::open_or_create(&root, "posts", 1, Some(1024 * 1024))
                .expect("open");
        let docs: Vec<_> = (1..=500u64)
            .map(|i| {
                let body = if i % 10 == 0 {
                    format!("breaking news item {i} with dnadb launch notes")
                } else {
                    format!("routine update item {i}")
                };
                json!({
                    "id": i,
                    "slug": format!("post-{i}"),
                    "title": body,
                })
            })
            .collect();
        store.execute_insert_many(docs).expect("bulk");
        let reader = store.begin();
        let ast = QueryAst::new("posts")
            .r#where(
                "title",
                WhereOp::Like,
                QueryLiteral::String("%dnadb launch%".to_string()),
            )
            .limit(100);
        let (rows, plan) = store.query_with_plan(&reader, &ast);
        assert_eq!(plan, SnapshotQueryPlan::ExactIndex);
        assert!(!rows.is_empty());
        for r in &rows {
            let title = r.get("title").and_then(|v| v.as_str()).expect("title");
            assert!(title.contains("dnadb launch"));
        }
    }
}

#[cfg(test)]
fn json_to_record(value: Value) -> crate::mvcc::Record {
    let Value::Object(map) = value else {
        panic!("test record must be an object");
    };
    map.into_iter().collect()
}

