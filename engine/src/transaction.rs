//! Transaction runtime semantics on top of MVCC version chains.

use std::collections::HashMap;

use crate::mvcc::{Record, VersionChain};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxnStatus {
    Active,
    Committed,
    RolledBack,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteOp {
    Upsert { record_id: u64, record: Record },
    Delete { record_id: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transaction {
    pub txn_id: u64,
    pub snapshot_ts: u64,
    pub write_set: Vec<WriteOp>,
    pub status: TxnStatus,
}

#[derive(Debug, Default)]
pub struct TransactionManager {
    chains: HashMap<u64, VersionChain>,
    active_txns: HashMap<u64, u64>, // txn_id -> snapshot_ts
    next_txn_id: u64,
    clock_ts: u64,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn begin(&mut self) -> Transaction {
        self.next_txn_id = self.next_txn_id.saturating_add(1);
        let txn_id = self.next_txn_id;
        let snapshot_ts = self.clock_ts;
        self.active_txns.insert(txn_id, snapshot_ts);
        Transaction {
            txn_id,
            snapshot_ts,
            write_set: Vec::new(),
            status: TxnStatus::Active,
        }
    }

    pub fn read(&self, txn: &Transaction, record_id: u64) -> Option<Record> {
        if let Some(op) = txn
            .write_set
            .iter()
            .rev()
            .find(|op| match op {
                WriteOp::Upsert { record_id: id, .. } | WriteOp::Delete { record_id: id } => {
                    *id == record_id
                }
            })
        {
            return match op {
                WriteOp::Upsert { record, .. } => Some(record.clone()),
                WriteOp::Delete { .. } => None,
            };
        }

        self.chains
            .get(&record_id)
            .and_then(|chain| chain.visible_at(txn.snapshot_ts))
            .cloned()
    }

    pub fn visible_records(&self, txn: &Transaction) -> Vec<(u64, Record)> {
        self.chains
            .iter()
            .filter_map(|(record_id, chain)| {
                chain
                    .visible_at(txn.snapshot_ts)
                    .cloned()
                    .map(|record| (*record_id, record))
            })
            .collect()
    }

    pub fn upsert(&self, txn: &mut Transaction, record_id: u64, record: Record) {
        txn.write_set.push(WriteOp::Upsert { record_id, record });
    }

    pub fn delete(&self, txn: &mut Transaction, record_id: u64) {
        txn.write_set.push(WriteOp::Delete { record_id });
    }

    pub fn can_commit(&self, txn: &Transaction) -> Result<(), &'static str> {
        if txn.status != TxnStatus::Active {
            return Err("transaction is not active");
        }
        // Optimistic conflict check: if any target record was updated after this
        // transaction's snapshot, reject commit.
        for op in &txn.write_set {
            let record_id = match op {
                WriteOp::Upsert { record_id, .. } | WriteOp::Delete { record_id } => *record_id,
            };
            if let Some(chain) = self.chains.get(&record_id) {
                if chain.latest_ts().is_some_and(|ts| ts > txn.snapshot_ts) {
                    return Err("write-write conflict");
                }
            }
        }
        Ok(())
    }

    pub fn commit(&mut self, txn: &mut Transaction) -> Result<u64, &'static str> {
        self.can_commit(txn)?;
        self.clock_ts = self.clock_ts.saturating_add(1);
        let commit_ts = self.clock_ts;
        for op in txn.write_set.drain(..) {
            match op {
                WriteOp::Upsert { record_id, record } => self
                    .chains
                    .entry(record_id)
                    .or_insert_with(|| VersionChain::new(record_id))
                    .insert_version(record, txn.txn_id, commit_ts),
                WriteOp::Delete { record_id } => self
                    .chains
                    .entry(record_id)
                    .or_insert_with(|| VersionChain::new(record_id))
                    .delete(txn.txn_id, commit_ts),
            }
        }
        self.active_txns.remove(&txn.txn_id);
        txn.status = TxnStatus::Committed;
        Ok(commit_ts)
    }

    pub fn rollback(&mut self, txn: &mut Transaction) -> Result<(), &'static str> {
        if txn.status != TxnStatus::Active {
            return Err("transaction is not active");
        }
        txn.write_set.clear();
        self.active_txns.remove(&txn.txn_id);
        txn.status = TxnStatus::RolledBack;
        Ok(())
    }

    pub fn oldest_active_snapshot(&self) -> Option<u64> {
        self.active_txns.values().copied().min()
    }

    pub fn compact_versions(&mut self) {
        self.compact_versions_with_floor(1);
    }

    /// Compacts historical versions while preserving active-snapshot visibility and
    /// keeping at least `min_versions_to_keep` newest versions per record.
    pub fn compact_versions_with_floor(&mut self, min_versions_to_keep: usize) {
        let Some(oldest) = self.oldest_active_snapshot() else {
            // No active readers: all but latest version can be compacted away.
            for chain in self.chains.values_mut() {
                chain.compact_for_oldest_snapshot_with_floor(u64::MAX, min_versions_to_keep);
            }
            return;
        };
        for chain in self.chains.values_mut() {
            chain.compact_for_oldest_snapshot_with_floor(oldest, min_versions_to_keep);
        }
    }

    pub fn record_version_count(&self, record_id: u64) -> usize {
        self.chains
            .get(&record_id)
            .map(VersionChain::version_count)
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{TransactionManager, TxnStatus};
    use crate::mvcc::Record;

    fn record(version: u64) -> Record {
        let mut r = Record::new();
        r.insert("v".to_string(), json!(version));
        r
    }

    #[test]
    fn commit_is_atomic_and_visible_after_commit() {
        let mut tm = TransactionManager::new();
        let mut tx = tm.begin();
        tm.upsert(&mut tx, 1, record(1));
        tm.upsert(&mut tx, 2, record(2));
        assert!(tm.read(&tx, 1).is_some(), "read-your-writes before commit");
        assert_eq!(tx.status, TxnStatus::Active);
        tm.commit(&mut tx).expect("commit");
        assert_eq!(tx.status, TxnStatus::Committed);

        let reader = tm.begin();
        assert_eq!(
            tm.read(&reader, 1).and_then(|r| r.get("v").cloned()),
            Some(json!(1))
        );
        assert_eq!(
            tm.read(&reader, 2).and_then(|r| r.get("v").cloned()),
            Some(json!(2))
        );
    }

    #[test]
    fn rollback_discards_pending_writes() {
        let mut tm = TransactionManager::new();
        let mut tx = tm.begin();
        tm.upsert(&mut tx, 1, record(1));
        tm.rollback(&mut tx).expect("rollback");
        assert_eq!(tx.status, TxnStatus::RolledBack);
        let reader = tm.begin();
        assert!(tm.read(&reader, 1).is_none());
    }

    #[test]
    fn snapshot_isolation_keeps_old_reader_stable() {
        let mut tm = TransactionManager::new();
        let mut t1 = tm.begin();
        tm.upsert(&mut t1, 10, record(1));
        tm.commit(&mut t1).expect("commit t1");

        let old_reader = tm.begin(); // snapshot before new write

        let mut writer = tm.begin();
        tm.upsert(&mut writer, 10, record(2));
        tm.commit(&mut writer).expect("commit writer");

        assert_eq!(
            tm.read(&old_reader, 10).and_then(|r| r.get("v").cloned()),
            Some(json!(1))
        );
        let new_reader = tm.begin();
        assert_eq!(
            tm.read(&new_reader, 10).and_then(|r| r.get("v").cloned()),
            Some(json!(2))
        );
    }

    #[test]
    fn delete_creates_tombstone_visibility() {
        let mut tm = TransactionManager::new();
        let mut create = tm.begin();
        tm.upsert(&mut create, 3, record(1));
        tm.commit(&mut create).expect("create commit");

        let old_reader = tm.begin();
        let mut deleter = tm.begin();
        tm.delete(&mut deleter, 3);
        tm.commit(&mut deleter).expect("delete commit");

        assert!(tm.read(&old_reader, 3).is_some(), "old snapshot still sees row");
        let new_reader = tm.begin();
        assert!(tm.read(&new_reader, 3).is_none(), "new snapshot sees deletion");
    }

    #[test]
    fn conflicting_writes_reject_later_commit() {
        let mut tm = TransactionManager::new();
        let mut seed = tm.begin();
        tm.upsert(&mut seed, 77, record(1));
        tm.commit(&mut seed).expect("seed commit");

        let mut t1 = tm.begin();
        let mut t2 = tm.begin();
        tm.upsert(&mut t1, 77, record(2));
        tm.upsert(&mut t2, 77, record(3));

        tm.commit(&mut t1).expect("first writer commits");
        let err = tm.commit(&mut t2).expect_err("second writer should conflict");
        assert_eq!(err, "write-write conflict");
    }

    #[test]
    fn compaction_preserves_oldest_active_snapshot_visibility() {
        let mut tm = TransactionManager::new();
        let mut seed = tm.begin();
        tm.upsert(&mut seed, 1, record(1));
        tm.commit(&mut seed).expect("seed");

        let old_reader = tm.begin();

        let mut w2 = tm.begin();
        tm.upsert(&mut w2, 1, record(2));
        tm.commit(&mut w2).expect("w2");
        let mut w3 = tm.begin();
        tm.upsert(&mut w3, 1, record(3));
        tm.commit(&mut w3).expect("w3");

        tm.compact_versions_with_floor(1);
        assert_eq!(
            tm.read(&old_reader, 1).and_then(|r| r.get("v").cloned()),
            Some(json!(1))
        );
        let new_reader = tm.begin();
        assert_eq!(
            tm.read(&new_reader, 1).and_then(|r| r.get("v").cloned()),
            Some(json!(3))
        );
    }
}

