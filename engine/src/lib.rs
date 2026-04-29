//! DNA-DB core engine crate.
//!
//! Stage 1: strand storage, WAL, encoding, recovery.
//! Stage 2: query AST → guide pattern compilation (execution follows).
//! Stage 3: auth foundation (identity + session model) in progress.

pub const ENGINE_NAME: &str = "dnadb-engine";

pub mod auth;
pub mod audit;
pub mod codec;
pub mod complement;
pub mod encoding;
pub mod group_commit;
pub mod histone;
pub mod integrity;
pub mod lifecycle;
pub mod model;
pub mod mvcc;
pub mod privacy;
pub mod processor;
pub mod query;
pub mod recovery;
pub mod replication;
pub mod runtime;
pub mod overlay;
pub mod storage;
pub mod transaction;
pub mod transaction_durable;
pub mod transfer;
pub mod tls;
pub mod wal;
pub mod wire;
