//! Startup crash recovery: replay WAL entries not yet reflected in the strand pool.

use std::path::Path;

use thiserror::Error;

use crate::codec::BincodeStrandCodec;
use crate::processor::{process_wal_entry, ProcessorError};
use crate::storage::CollectionStorage;
use crate::wal::{Wal, WalError};

#[derive(Debug, Error)]
pub enum RecoveryError {
    #[error("wal: {0}")]
    Wal(#[from] WalError),
    #[error("processor: {0}")]
    Processor(#[from] ProcessorError),
    #[error("storage: {0}")]
    Storage(#[from] crate::storage::StorageError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryStats {
    /// Max `Strand.version` / WAL sequence found in the strand pool before replay.
    pub pool_high_water_before: u64,
    /// WAL entries applied during this replay.
    pub replayed: usize,
}

/// Open storage + WAL, then materialize every WAL entry whose sequence is greater than the pool high-water mark.
///
/// Matches `DNADB_SPEC.md` startup flow: find highest committed sequence in the strand pool, re-process WAL
/// entries above that number. Stage 1 treats `Strand.version` as the WAL sequence assigned at append time.
pub fn replay_pending_wal_after_open(
    root: &Path,
    collection: &str,
    collection_id: u32,
    initial_mmap: Option<usize>,
) -> Result<RecoveryStats, RecoveryError> {
    let mut storage = CollectionStorage::open_or_create(root, collection, initial_mmap)?;
    let pool_high_water_before = storage.materialized_high_water_sequence;

    let mut wal = Wal::open_or_create(root, collection)?;
    let entries = wal.read_all_entries()?;

    let codec = BincodeStrandCodec;
    let mut replayed = 0usize;
    for entry in &entries {
        if entry.sequence > pool_high_water_before {
            process_wal_entry(
                &mut storage,
                &codec,
                collection_id,
                entry.sequence,
                &entry.payload,
            )?;
            replayed += 1;
        }
    }

    Ok(RecoveryStats {
        pool_high_water_before,
        replayed,
    })
}

#[cfg(test)]
mod tests {
    use super::replay_pending_wal_after_open;
    use crate::codec::{BincodeStrandCodec, StrandCodec};
    use crate::processor::process_wal_entry;
    use crate::storage::CollectionStorage;
    use crate::wal::Wal;
    use tempfile::tempdir;

    #[test]
    fn replays_only_wal_entries_above_pool_high_water() {
        let dir = tempdir().expect("tempdir");
        let root = dir.path();

        let mut wal = Wal::open_or_create(root, "users").expect("wal");
        wal.append_and_fsync(b"p1").expect("w1");
        wal.append_and_fsync(b"p2").expect("w2");
        wal.append_and_fsync(b"p3").expect("w3");
        drop(wal);

        {
            let mut storage =
                CollectionStorage::open_or_create(root, "users", Some(256 * 1024)).expect("storage");
            let codec = BincodeStrandCodec;
            process_wal_entry(&mut storage, &codec, 1, 1, b"p1").expect("materialize 1 only");
        }

        let stats = replay_pending_wal_after_open(root, "users", 1, Some(256 * 1024)).expect("recover");
        assert_eq!(stats.pool_high_water_before, 1);
        assert_eq!(stats.replayed, 2);

        let paths = CollectionStorage::open_or_create(root, "users", Some(256 * 1024))
            .expect("reopen")
            .paths;
        let bytes = std::fs::read(paths.strands).expect("read strands");
        let codec = BincodeStrandCodec;
        let mut pos = 0usize;
        let mut versions = Vec::new();
        while pos + 6 <= bytes.len()
            && bytes[pos..pos + 4] == crate::codec::STRAND_FORMAT_MAGIC
        {
            let s = codec.decode_strand(&bytes[pos..]).expect("decode");
            versions.push(s.version);
            pos += codec.encode_strand(&s).expect("enc").len();
        }
        assert_eq!(versions, vec![1, 2, 3]);
    }
}
