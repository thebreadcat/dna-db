//! Layer 10 MVCC primitives: version chains, snapshot visibility, and delete tombstones.

use std::collections::HashMap;

use serde_json::Value;

pub type Record = HashMap<String, Value>;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct VersionEntry {
    pub txn_id: u64,
    pub ts: u64,
    pub record: Option<Record>, // None = delete tombstone
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct VersionChain {
    pub record_id: u64,
    versions: Vec<VersionEntry>, // sorted by ts ascending
}

impl VersionChain {
    pub fn new(record_id: u64) -> Self {
        Self {
            record_id,
            versions: Vec::new(),
        }
    }

    pub fn insert_version(&mut self, record: Record, txn_id: u64, ts: u64) {
        let ver = VersionEntry {
            txn_id,
            ts,
            record: Some(record),
        };
        if self.versions.last().map(|v| v.ts) < Some(ts) {
            self.versions.push(ver);
        } else {
            let pos = self.versions.partition_point(|v| v.ts <= ts);
            self.versions.insert(pos, ver);
        }
    }

    pub fn delete(&mut self, txn_id: u64, ts: u64) {
        let ver = VersionEntry {
            txn_id,
            ts,
            record: None,
        };
        if self.versions.last().map(|v| v.ts) < Some(ts) {
            self.versions.push(ver);
        } else {
            let pos = self.versions.partition_point(|v| v.ts <= ts);
            self.versions.insert(pos, ver);
        }
    }

    /// Visible value at snapshot timestamp `snapshot_ts`.
    pub fn visible_at(&self, snapshot_ts: u64) -> Option<&Record> {
        self.versions
            .iter()
            .rev()
            .find(|v| v.ts <= snapshot_ts)
            .and_then(|v| v.record.as_ref())
    }

    pub fn version_count(&self) -> usize {
        self.versions.len()
    }

    pub fn latest_ts(&self) -> Option<u64> {
        self.versions.last().map(|v| v.ts)
    }

    /// Remove obsolete versions that are older than the oldest active snapshot.
    ///
    /// Keeps all versions newer than `oldest_active_snapshot` and at most one
    /// fallback version at-or-before that boundary.
    pub fn compact_for_oldest_snapshot(&mut self, oldest_active_snapshot: u64) {
        self.compact_for_oldest_snapshot_with_floor(oldest_active_snapshot, 1);
    }

    /// Same as `compact_for_oldest_snapshot`, but keeps at least `min_versions_to_keep`
    /// newest versions regardless of snapshot boundary.
    pub fn compact_for_oldest_snapshot_with_floor(
        &mut self,
        oldest_active_snapshot: u64,
        min_versions_to_keep: usize,
    ) {
        if self.versions.len() <= 1 {
            return;
        }

        let keep_tail_start = self
            .versions
            .iter()
            .position(|v| v.ts > oldest_active_snapshot)
            .unwrap_or(self.versions.len());

        if keep_tail_start == 0 {
            return;
        }

        let mut compacted = Vec::with_capacity(self.versions.len() - keep_tail_start + 1);
        // Preserve one baseline version visible to the oldest snapshot.
        compacted.push(self.versions[keep_tail_start - 1].clone());
        compacted.extend(self.versions[keep_tail_start..].iter().cloned());
        let floor = min_versions_to_keep.max(1);
        if compacted.len() < floor {
            let need = floor - compacted.len();
            let available_before_baseline = keep_tail_start.saturating_sub(1);
            let take = need.min(available_before_baseline);
            let prepend_start = (keep_tail_start - 1).saturating_sub(take);
            let mut with_floor = Vec::with_capacity(compacted.len() + take);
            with_floor.extend(self.versions[prepend_start..keep_tail_start - 1].iter().cloned());
            with_floor.extend(compacted);
            self.versions = with_floor;
            return;
        }
        self.versions = compacted;
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use serde_json::json;

    use super::{Record, VersionChain};

    fn fake_record(version: u64) -> Record {
        let mut r = Record::new();
        r.insert("version".to_string(), json!(version));
        r
    }

    #[test]
    fn read_sees_own_write() {
        let mut chain = VersionChain::new(42);
        chain.insert_version(fake_record(1), 1, 100);
        assert!(chain.visible_at(100).is_some());
    }

    #[test]
    fn read_does_not_see_future_write() {
        let mut chain = VersionChain::new(42);
        chain.insert_version(fake_record(2), 2, 200);
        assert!(chain.visible_at(100).is_none());
    }

    #[test]
    fn deleted_record_is_invisible() {
        let mut chain = VersionChain::new(42);
        chain.insert_version(fake_record(1), 1, 100);
        chain.delete(2, 200);
        assert!(chain.visible_at(300).is_none());
    }

    #[test]
    fn concurrent_snapshot_isolation_semantics() {
        let mut chain = VersionChain::new(7);
        chain.insert_version(fake_record(1), 1, 100);
        // "Long-running read transaction" snapshot at 150
        let before = chain.visible_at(150).expect("visible at 150");
        assert_eq!(before.get("version"), Some(&json!(1)));
        // Concurrent write at ts=200
        chain.insert_version(fake_record(2), 2, 200);
        // Old snapshot still sees old value; new snapshot sees new value.
        let old_snap = chain.visible_at(150).expect("visible at old snapshot");
        let new_snap = chain.visible_at(250).expect("visible at new snapshot");
        assert_eq!(old_snap.get("version"), Some(&json!(1)));
        assert_eq!(new_snap.get("version"), Some(&json!(2)));
    }

    #[test]
    fn compaction_keeps_boundary_and_newer_versions() {
        let mut chain = VersionChain::new(99);
        chain.insert_version(fake_record(1), 1, 10);
        chain.insert_version(fake_record(2), 2, 20);
        chain.insert_version(fake_record(3), 3, 30);
        chain.insert_version(fake_record(4), 4, 40);
        chain.compact_for_oldest_snapshot(25);
        assert_eq!(chain.version_count(), 3);
        let old = chain.visible_at(25).expect("visible at 25");
        let newest = chain.visible_at(100).expect("visible at 100");
        assert_eq!(old.get("version"), Some(&json!(2)));
        assert_eq!(newest.get("version"), Some(&json!(4)));
    }

    #[test]
    fn compaction_floor_keeps_minimum_versions() {
        let mut chain = VersionChain::new(100);
        chain.insert_version(fake_record(1), 1, 10);
        chain.insert_version(fake_record(2), 2, 20);
        chain.insert_version(fake_record(3), 3, 30);
        chain.compact_for_oldest_snapshot_with_floor(5, 2);
        assert!(chain.version_count() >= 2);
    }

    proptest! {
        #[test]
        fn snapshot_never_sees_future_write(snapshot_ts in 0u64..1000, write_ts in 0u64..1000) {
            let mut chain = VersionChain::new(1);
            chain.insert_version(fake_record(1), 1, write_ts);
            let visible = chain.visible_at(snapshot_ts);
            if write_ts <= snapshot_ts {
                prop_assert!(visible.is_some());
            } else {
                prop_assert!(visible.is_none());
            }
        }
    }
}

