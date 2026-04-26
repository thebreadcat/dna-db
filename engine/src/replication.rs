//! WAL-stream replication runtime (source polling + resume checkpoint + ordered apply).

use std::fs;
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::codec::BincodeStrandCodec;
use crate::processor::{process_wal_entry_with_mode, PersistMode, ProcessorError};
use crate::storage::{CollectionStorage, StorageError};
use crate::wal::{Wal, WalEntry, WalError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReplicationCheckpoint {
    pub last_applied_source_sequence: u64,
}

impl Default for ReplicationCheckpoint {
    fn default() -> Self {
        Self {
            last_applied_source_sequence: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationBatch {
    pub entries: Vec<WalEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationApplyReport {
    pub applied: usize,
    pub skipped_replay: usize,
    pub checkpoint_after: ReplicationCheckpoint,
}

#[derive(Debug, Error)]
pub enum ReplicationError {
    #[error("wal: {0}")]
    Wal(#[from] WalError),
    #[error("storage: {0}")]
    Storage(#[from] StorageError),
    #[error("processor: {0}")]
    Processor(#[from] ProcessorError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("serde json: {0}")]
    SerdeJson(#[from] serde_json::Error),
    #[error("out-of-order source sequence: got {got}, expected >= {expected_min}")]
    OutOfOrderSourceSequence { got: u64, expected_min: u64 },
}

pub struct WalStreamSource {
    wal: Wal,
}

impl WalStreamSource {
    pub fn open(root: &Path, collection: &str) -> Result<Self, ReplicationError> {
        Ok(Self {
            wal: Wal::open_or_create(root, collection)?,
        })
    }

    pub fn fetch_since(
        &mut self,
        checkpoint: ReplicationCheckpoint,
        max_entries: usize,
    ) -> Result<ReplicationBatch, ReplicationError> {
        let from = checkpoint.last_applied_source_sequence.saturating_add(1);
        let entries = self.wal.read_entries_from(from, Some(max_entries))?;
        Ok(ReplicationBatch { entries })
    }
}

pub struct WalStreamReplica {
    storage: CollectionStorage,
    codec: BincodeStrandCodec,
    checkpoint_path: PathBuf,
    checkpoint: ReplicationCheckpoint,
    collection_id: u32,
}

impl WalStreamReplica {
    pub fn open_or_create(
        root: &Path,
        collection: &str,
        collection_id: u32,
        initial_mmap: Option<usize>,
        replica_id: &str,
    ) -> Result<Self, ReplicationError> {
        let checkpoint_path = root.join(format!("{collection}.replica-{replica_id}.checkpoint.json"));
        let checkpoint = load_checkpoint(&checkpoint_path)?;
        Ok(Self {
            storage: CollectionStorage::open_or_create(root, collection, initial_mmap)?,
            codec: BincodeStrandCodec,
            checkpoint_path,
            checkpoint,
            collection_id,
        })
    }

    pub fn checkpoint(&self) -> ReplicationCheckpoint {
        self.checkpoint
    }

    pub fn apply_batch(
        &mut self,
        batch: ReplicationBatch,
    ) -> Result<ReplicationApplyReport, ReplicationError> {
        let mut applied = 0usize;
        let mut skipped_replay = 0usize;
        let mut expected_min = self.checkpoint.last_applied_source_sequence.saturating_add(1);

        for entry in batch.entries {
            if entry.sequence <= self.checkpoint.last_applied_source_sequence {
                skipped_replay = skipped_replay.saturating_add(1);
                continue;
            }
            if entry.sequence < expected_min {
                return Err(ReplicationError::OutOfOrderSourceSequence {
                    got: entry.sequence,
                    expected_min,
                });
            }
            process_wal_entry_with_mode(
                &mut self.storage,
                &self.codec,
                self.collection_id,
                entry.sequence,
                &entry.payload,
                PersistMode::Buffered,
            )?;
            self.checkpoint.last_applied_source_sequence = entry.sequence;
            expected_min = entry.sequence.saturating_add(1);
            applied = applied.saturating_add(1);
        }

        self.storage.flush()?;
        save_checkpoint(&self.checkpoint_path, self.checkpoint)?;
        Ok(ReplicationApplyReport {
            applied,
            skipped_replay,
            checkpoint_after: self.checkpoint,
        })
    }
}

fn load_checkpoint(path: &Path) -> Result<ReplicationCheckpoint, ReplicationError> {
    if !path.exists() {
        return Ok(ReplicationCheckpoint::default());
    }
    let bytes = fs::read(path)?;
    Ok(serde_json::from_slice::<ReplicationCheckpoint>(&bytes)?)
}

fn save_checkpoint(path: &Path, checkpoint: ReplicationCheckpoint) -> Result<(), ReplicationError> {
    let bytes = serde_json::to_vec_pretty(&checkpoint)?;
    fs::write(path, bytes)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::codec::{BincodeStrandCodec, StrandCodec, STRAND_FORMAT_MAGIC};
    use crate::wal::Wal;
    use tempfile::tempdir;

    use super::{ReplicationCheckpoint, WalStreamReplica, WalStreamSource};

    fn decode_count(root: &Path, collection: &str) -> usize {
        let storage = crate::storage::CollectionStorage::open_or_create(root, collection, Some(1024 * 1024))
            .expect("open storage");
        let bytes = std::fs::read(storage.paths.strands).expect("read strands");
        let codec = BincodeStrandCodec;
        let mut pos = 0usize;
        let mut out = 0usize;
        while pos + 6 <= bytes.len() && bytes[pos..pos + 4] == STRAND_FORMAT_MAGIC {
            let s = codec.decode_strand(&bytes[pos..]).expect("decode");
            pos += codec.encode_strand(&s).expect("encode size").len();
            out += 1;
        }
        out
    }

    use std::path::Path;

    #[test]
    fn replication_stream_resumes_from_checkpoint() {
        let dir = tempdir().expect("tempdir");
        let mut primary = Wal::open_or_create(dir.path(), "users").expect("open primary wal");
        primary.append_and_fsync(br#"{"id":1}"#).expect("append1");
        primary.append_and_fsync(br#"{"id":2}"#).expect("append2");
        primary.append_and_fsync(br#"{"id":3}"#).expect("append3");

        let mut source = WalStreamSource::open(dir.path(), "users").expect("open source");
        let mut replica =
            WalStreamReplica::open_or_create(dir.path(), "users-replica", 7, Some(1024 * 1024), "n2")
                .expect("open replica");

        let batch1 = source
            .fetch_since(ReplicationCheckpoint::default(), 2)
            .expect("fetch batch1");
        let report1 = replica.apply_batch(batch1).expect("apply batch1");
        assert_eq!(report1.applied, 2);
        assert_eq!(report1.checkpoint_after.last_applied_source_sequence, 2);
        assert_eq!(decode_count(dir.path(), "users-replica"), 2);

        // Resume from persisted checkpoint.
        let mut resumed_replica =
            WalStreamReplica::open_or_create(dir.path(), "users-replica", 7, Some(1024 * 1024), "n2")
                .expect("reopen replica");
        let batch2 = source
            .fetch_since(resumed_replica.checkpoint(), 10)
            .expect("fetch batch2");
        let report2 = resumed_replica.apply_batch(batch2).expect("apply batch2");
        assert_eq!(report2.applied, 1);
        assert_eq!(report2.checkpoint_after.last_applied_source_sequence, 3);
        assert_eq!(decode_count(dir.path(), "users-replica"), 3);
    }

    #[test]
    fn replication_apply_skips_replayed_entries() {
        let dir = tempdir().expect("tempdir");
        let mut primary = Wal::open_or_create(dir.path(), "users").expect("open primary wal");
        primary.append_and_fsync(br#"{"id":1}"#).expect("append1");
        primary.append_and_fsync(br#"{"id":2}"#).expect("append2");

        let mut source = WalStreamSource::open(dir.path(), "users").expect("open source");
        let mut replica =
            WalStreamReplica::open_or_create(dir.path(), "users-replica", 7, Some(1024 * 1024), "n3")
                .expect("open replica");

        let batch = source
            .fetch_since(ReplicationCheckpoint::default(), 10)
            .expect("fetch");
        let report = replica.apply_batch(batch.clone()).expect("apply once");
        assert_eq!(report.applied, 2);

        let replay = replica.apply_batch(batch).expect("apply replay");
        assert_eq!(replay.applied, 0);
        assert_eq!(replay.skipped_replay, 2);
        assert_eq!(decode_count(dir.path(), "users-replica"), 2);
    }
}

