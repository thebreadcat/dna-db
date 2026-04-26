//! Durable transaction runtime: MVCC transaction manager + WAL/materialization integration.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::query::{
    choose_path, compile_query, CollectionStats, QueryAst, QueryLiteral, QueryPath, RangeOp,
    SortDirection, WhereOp,
};
use crate::codec::BincodeStrandCodec;
use crate::processor::{process_wal_entry_with_mode, PersistMode, ProcessorError};
use crate::storage::{CollectionStorage, StorageError};
use crate::transaction::{Transaction, TransactionManager, WriteOp};
use crate::wal::{Wal, WalError};
use crate::wire::{DeleteOp, InsertOp, UpdateOp, WireOperation};

#[derive(Debug, thiserror::Error)]
pub enum DurableTxnError {
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
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionResult {
    QueryRows(Vec<crate::mvcc::Record>),
    AffectedRows(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotQueryPlan {
    Direct,
    Index,
    GuidedScan,
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
        let mut out = Self {
            tx_manager: TransactionManager::new(),
            wal: Wal::open_or_create(root, collection)?,
            storage: CollectionStorage::open_or_create(root, collection, initial_mmap)?,
            codec: BincodeStrandCodec,
            collection_id,
        };
        out.replay_wal_to_mvcc()?;
        Ok(out)
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
        self.tx_manager
            .can_commit(txn)
            .map_err(DurableTxnError::Transaction)?;

        for op in &txn.write_set {
            let payload = durable_payload_from_write_op(txn.txn_id, op)?;
            let seq = self.wal.append(&payload)?;
            process_wal_entry_with_mode(
                &mut self.storage,
                &self.codec,
                self.collection_id,
                seq,
                &payload,
                PersistMode::Buffered,
            )?;
        }

        self.storage.flush()?;
        self.wal.sync()?;

        self.tx_manager
            .commit(txn)
            .map_err(DurableTxnError::Transaction)
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
    use crate::query::{QueryAst, QueryLiteral, WhereOp};
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
        assert_eq!(plan, SnapshotQueryPlan::Index);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("id"), Some(&json!(77)));
    }
}

#[cfg(test)]
fn json_to_record(value: Value) -> crate::mvcc::Record {
    let Value::Object(map) = value else {
        panic!("test record must be an object");
    };
    map.into_iter().collect()
}

