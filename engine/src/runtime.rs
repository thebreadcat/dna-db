//! Engine runtime facade: wire translation + durable transactional execution.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

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
}

