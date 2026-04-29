//! Engine runtime facade: wire translation + durable transactional execution.
//!
//! All collection data flows through [`DurableTransactionStore`] (WAL, strand materialization, MVCC,
//! indexes). There is no separate raw-ingest journal.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde_json::Value;
use thiserror::Error;

use crate::transaction_durable::{DurableTransactionStore, DurableTxnError, ExecutionResult};
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
}

pub struct EngineRuntime {
    root: PathBuf,
    initial_mmap: Option<usize>,
    stores: HashMap<String, DurableTransactionStore>,
}

impl EngineRuntime {
    pub fn open(root: &Path, initial_mmap: Option<usize>) -> Self {
        Self {
            root: root.to_path_buf(),
            initial_mmap,
            stores: HashMap::new(),
        }
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

    pub fn root_path(&self) -> &Path {
        &self.root
    }

    /// Best-effort discovery of known durable collections from on-disk WAL files
    /// and already-open stores. Returns sorted unique names.
    pub fn list_known_collections(&self) -> Vec<String> {
        let mut out: std::collections::BTreeSet<String> = self.stores.keys().cloned().collect();
        if let Ok(rd) = std::fs::read_dir(&self.root) {
            for entry in rd.flatten() {
                let p = entry.path();
                if !p.is_file() {
                    continue;
                }
                let Some(name) = p.file_name().and_then(|n| n.to_str()) else {
                    continue;
                };
                if let Some(coll) = name.strip_suffix(".wal") {
                    if !coll.trim().is_empty() {
                        out.insert(coll.to_string());
                    }
                }
            }
        }
        out.into_iter().collect()
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
    use tempfile::tempdir;

    use super::EngineRuntime;
    use crate::transaction_durable::ExecutionResult;
    use crate::wire::{
        MongoCommand, MongoFindCommand, MongoInsertOneCommand, PostgresQuery,
    };

    fn count_all(rt: &mut EngineRuntime, collection: &str) -> usize {
        let out = rt
            .execute_mongo_command(MongoCommand::Find(MongoFindCommand {
                collection: collection.to_string(),
                filter: Map::new(),
                sort: None,
                limit: None,
                include_paths: vec![],
            }))
            .expect("count query");
        match out {
            ExecutionResult::QueryRows(rows) => rows.len(),
            _ => 0,
        }
    }

    fn get_by_id(rt: &mut EngineRuntime, collection: &str, id: u64) -> Option<crate::mvcc::Record> {
        let mut filter = Map::new();
        filter.insert("id".to_string(), json!(id));
        let out = rt
            .execute_mongo_command(MongoCommand::Find(MongoFindCommand {
                collection: collection.to_string(),
                filter,
                sort: None,
                limit: Some(1),
                include_paths: vec![],
            }))
            .expect("get by id");
        match out {
            ExecutionResult::QueryRows(mut rows) => rows.pop(),
            _ => None,
        }
    }

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

    #[test]
    fn crash_recovery_basic() {
        let dir = tempdir().expect("tempdir");
        {
            let mut rt = EngineRuntime::open(dir.path(), Some(1024 * 1024));
            let docs: Vec<Value> = (0..1000u64)
                .map(|i| json!({"id": i, "slug": format!("post-{i}"), "updated_at": i as i64}))
                .collect();
            rt.execute_mongo_insert_many("posts", docs).expect("insert");
            std::mem::forget(rt);
        }
        let mut rt = EngineRuntime::open(dir.path(), Some(1024 * 1024));
        for i in 0..1000u64 {
            assert!(get_by_id(&mut rt, "posts", i).is_some(), "record {i} lost after crash");
        }
    }

    #[test]
    fn crash_recovery_indexes_consistent() {
        let dir = tempdir().expect("tempdir");
        {
            let mut rt = EngineRuntime::open(dir.path(), Some(1024 * 1024));
            let docs: Vec<Value> = (0..1000u64)
                .map(|i| {
                    json!({
                        "id": i,
                        "slug": format!("post-{i}"),
                        "title": format!("post-{i} title"),
                        "status": if i % 2 == 0 { "published" } else { "draft" },
                        "updated_at": i as i64
                    })
                })
                .collect();
            rt.execute_mongo_insert_many("posts", docs).expect("insert");
            std::mem::forget(rt);
        }
        let mut rt = EngineRuntime::open(dir.path(), Some(1024 * 1024));

        let latest = rt
            .execute_mongo_command(MongoCommand::Find(MongoFindCommand {
                collection: "posts".to_string(),
                filter: Map::new(),
                sort: Some({
                    let mut s = Map::new();
                    s.insert("updated_at".to_string(), json!(-1));
                    s
                }),
                limit: Some(50),
                include_paths: vec![],
            }))
            .expect("latest 50");
        let latest_count = match latest {
            ExecutionResult::QueryRows(rows) => rows.len(),
            _ => 0,
        };
        assert_eq!(latest_count, 50);

        let exact = rt
            .execute_mongo_command(MongoCommand::Find(MongoFindCommand {
                collection: "posts".to_string(),
                filter: {
                    let mut f = Map::new();
                    f.insert("slug".to_string(), json!("post-500"));
                    f
                },
                sort: None,
                limit: Some(1),
                include_paths: vec![],
            }))
            .expect("exact slug");
        let exact_count = match exact {
            ExecutionResult::QueryRows(rows) => rows.len(),
            _ => 0,
        };
        assert!(exact_count > 0, "exact index path did not return row");

        let contains = rt
            .execute_mongo_command(MongoCommand::Find(MongoFindCommand {
                collection: "posts".to_string(),
                filter: {
                    let mut f = Map::new();
                    f.insert("slug".to_string(), json!({"$regex": "%post-5%"}));
                    f
                },
                sort: Some({
                    let mut s = Map::new();
                    s.insert("id".to_string(), json!(1));
                    s
                }),
                limit: Some(50),
                include_paths: vec![],
            }))
            .expect("contains");
        let contains_count = match contains {
            ExecutionResult::QueryRows(rows) => rows.len(),
            _ => 0,
        };
        assert!(contains_count > 0, "contains path empty after recovery");

        let prefix = rt
            .execute_mongo_command(MongoCommand::Find(MongoFindCommand {
                collection: "posts".to_string(),
                filter: {
                    let mut f = Map::new();
                    f.insert("slug".to_string(), json!({"$regex": "post-5%"}));
                    f
                },
                sort: Some({
                    let mut s = Map::new();
                    s.insert("id".to_string(), json!(1));
                    s
                }),
                limit: Some(50),
                include_paths: vec![],
            }))
            .expect("prefix");
        let prefix_count = match prefix {
            ExecutionResult::QueryRows(rows) => rows.len(),
            _ => 0,
        };
        assert!(prefix_count > 0, "prefix path empty after recovery");
    }

    #[test]
    fn crash_recovery_repeated_cycles() {
        let dir = tempdir().expect("tempdir");
        let mut total = 0u64;
        for cycle in 0..5 {
            {
                let mut rt = EngineRuntime::open(dir.path(), Some(1024 * 1024));
                let docs: Vec<Value> = (0..200u64)
                    .map(|i| {
                        let id = total + i;
                        json!({"id": id, "slug": format!("post-{id}"), "updated_at": id as i64})
                    })
                    .collect();
                rt.execute_mongo_insert_many("posts", docs).expect("insert cycle");
                total += 200;
                std::mem::forget(rt);
            }
            let mut rt = EngineRuntime::open(dir.path(), Some(1024 * 1024));
            let count = count_all(&mut rt, "posts") as u64;
            assert_eq!(
                count, total,
                "lost records after cycle {cycle}: expected {total} got {count}"
            );
        }
    }
}
