//! Shared group-commit policy: fsync after N records and/or after a wall-clock interval.
//!
//! Used by the load benchmark and any writer that wants PostgreSQL-style WAL batching:
//! append many records, then a single `Wal::sync` + storage `sync_data` for the group.

use std::time::{Duration, Instant};

/// Upper bounds before the caller should flush WAL + storage to disk.
#[derive(Debug, Clone)]
pub struct GroupCommitPolicy {
    /// Sync after this many records since the last sync (minimum 1).
    pub max_batch: usize,
    /// Sync when this duration has elapsed since the **first** append in the current unsynced
    /// group, if set.
    pub max_interval: Option<Duration>,
}

/// Tracks pending records since the last successful group sync.
#[derive(Debug)]
pub struct GroupCommit {
    policy: GroupCommitPolicy,
    pending_since_sync: usize,
    /// Wall time when the current unsynced group began (`None` immediately after a sync).
    unsynced_since: Option<Instant>,
}

impl GroupCommit {
    pub fn new(policy: GroupCommitPolicy) -> Self {
        Self {
            policy,
            pending_since_sync: 0,
            unsynced_since: None,
        }
    }

    /// Call after each appended + processed record. Returns `true` when the caller should
    /// `wal.sync()` and `storage.sync_files()` (or equivalent).
    ///
    /// Interval semantics: sync if **uncommitted data** has been pending for `max_interval`
    /// (time since the first append after the previous sync), not time since the last fsync.
    /// On very slow storage, a short interval can still force frequent fsyncs; use
    /// `max_interval: None` (harness default) for batch-only group commit.
    pub fn after_record(&mut self) -> bool {
        self.pending_since_sync += 1;
        if self.unsynced_since.is_none() {
            self.unsynced_since = Some(Instant::now());
        }
        let batch_full = self.pending_since_sync >= self.policy.max_batch.max(1);
        let interval_hit = self.policy.max_interval.is_some_and(|iv| {
            self.unsynced_since
                .is_some_and(|t| t.elapsed() >= iv)
        });
        if batch_full || interval_hit {
            self.pending_since_sync = 0;
            self.unsynced_since = None;
            true
        } else {
            false
        }
    }

    pub fn has_pending(&self) -> bool {
        self.pending_since_sync > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fires_on_batch_only() {
        let mut g = GroupCommit::new(GroupCommitPolicy {
            max_batch: 3,
            max_interval: None,
        });
        assert!(!g.after_record());
        assert!(!g.after_record());
        assert!(g.after_record());
        assert!(!g.has_pending());
        assert!(!g.after_record());
    }

    #[test]
    fn fires_on_interval_after_first_record() {
        let mut g = GroupCommit::new(GroupCommitPolicy {
            max_batch: 1000,
            max_interval: Some(Duration::from_millis(0)),
        });
        assert!(g.after_record());
    }
}
