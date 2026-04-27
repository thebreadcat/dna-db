//! Background WAL processor: encode payload → complement → intron weave → persist.
//!
//! Stage 1 uses a dedicated worker thread (not async/await) to match the spec’s
//! “asynchronous background encoding thread” model.

use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::JoinHandle;
use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;

use crate::codec::{BincodeStrandCodec, CodecError, StrandCodec};
use crate::complement::generate_complement;
use crate::encoding::encode_bytes_to_codons;
use crate::integrity::{append_crc_entry_for_strand, IntegrityError};
use crate::model::{Codon, Intron, RefreshPolicy, Strand, Tag, Telomere};
use crate::storage::{CollectionStorage, StorageError};

/// FNV-1a 64-bit over `data` (stable, deterministic intron hash for Stage 1).
pub fn fnv1a64(data: &[u8]) -> u64 {
    const OFFSET: u64 = 14695981039346656037;
    const PRIME: u64 = 1099511628211;
    let mut hash = OFFSET;
    for b in data {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

fn wall_time_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn strand_signature(sequence: u64) -> [u8; 8] {
    sequence.to_le_bytes()
}

/// Build a [`Strand`] from raw WAL bytes: SIMD path later (`S1-T09`); encoding + complement + introns now.
pub fn strand_from_wal_payload(
    collection_id: u32,
    sequence: u64,
    raw_payload: &[u8],
) -> Strand {
    let encoded = encode_bytes_to_codons(raw_payload);
    let codons = encoded.codons;
    let complement = generate_complement(&codons);

    let codon_len = codons.len().min(u16::MAX as usize) as u16;
    let introns = vec![Intron {
        field_name: "_payload".to_string(),
        codon_offset: 0,
        codon_length: codon_len,
        value_hash: fnv1a64(raw_payload),
        references_strand: None,
    }];

    let now = wall_time_ns();
    Strand {
        signature: strand_signature(sequence),
        collection_id,
        codons,
        complement,
        introns,
        telomere: Telomere {
            count: 0,
            immortal: true,
            last_refresh: now,
            refresh_policy: RefreshPolicy::Immortal,
        },
        epigenetic_tags: vec![Tag {
            key: "wal_sequence".to_string(),
            value: sequence.to_string(),
        }],
        version: sequence,
        created_at: now,
        updated_at: now,
    }
}

/// Serialize complement codons for the complement pool (length-prefixed bincode).
pub fn encode_complement_blob(codons: &[Codon]) -> Result<Vec<u8>, bincode::Error> {
    let inner = bincode::serialize(codons)?;
    let mut out = Vec::with_capacity(4 + inner.len());
    out.extend_from_slice(&(inner.len() as u32).to_le_bytes());
    out.extend_from_slice(&inner);
    Ok(out)
}

#[derive(Debug, Error)]
pub enum ProcessorError {
    #[error("storage: {0}")]
    Storage(#[from] StorageError),
    #[error("codec: {0}")]
    Codec(#[from] CodecError),
    #[error("bincode: {0}")]
    Bincode(#[from] bincode::Error),
    #[error("integrity: {0}")]
    Integrity(#[from] IntegrityError),
    #[error("worker channel closed")]
    ChannelClosed,
    #[error("worker thread failed: {0}")]
    WorkerFailed(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistMode {
    /// Flush mmap + sync files for every processed WAL entry.
    SyncEveryWrite,
    /// No flush inside [`process_wal_entry_with_mode`]; caller runs
    /// [`CollectionStorage::flush`] / [`CollectionStorage::flush_maps`] after a batch
    /// (e.g. one commit). WAL `sync` remains the caller's durability policy.
    Buffered,
}

/// Encode → complement → introns → append strand frame + complement blob; flush to disk.
pub fn process_wal_entry(
    storage: &mut CollectionStorage,
    codec: &BincodeStrandCodec,
    collection_id: u32,
    sequence: u64,
    raw_payload: &[u8],
) -> Result<(), ProcessorError> {
    process_wal_entry_with_mode(
        storage,
        codec,
        collection_id,
        sequence,
        raw_payload,
        PersistMode::SyncEveryWrite,
    )
}

/// Same as [`process_wal_entry`] but allows durability policy control.
///
/// [`PersistMode::Buffered`] does not call [`CollectionStorage::flush_maps`]; avoid
/// thousands of full-map `msync`s per batch — the caller must flush after the WAL
/// entries for that batch are processed (see `DurableTransactionStore::commit_inner`).
pub fn process_wal_entry_with_mode(
    storage: &mut CollectionStorage,
    codec: &BincodeStrandCodec,
    collection_id: u32,
    sequence: u64,
    raw_payload: &[u8],
    mode: PersistMode,
) -> Result<(), ProcessorError> {
    let strand = strand_from_wal_payload(collection_id, sequence, raw_payload);
    let strand_bytes = codec.encode_strand(&strand)?;
    storage.append_strands(&strand_bytes)?;
    append_crc_entry_for_strand(storage, sequence, &strand_bytes)?;

    let comp_blob = encode_complement_blob(&strand.complement)?;
    storage.append_complement(&comp_blob)?;

    match mode {
        PersistMode::SyncEveryWrite => storage.flush()?,
        PersistMode::Buffered => {}
    }
    storage.record_materialized_sequence(sequence);
    Ok(())
}

#[derive(Debug)]
enum WalJob {
    Entry { sequence: u64, payload: Vec<u8> },
    Shutdown,
}

/// Handle to a background thread that drains WAL work and persists to collection files.
pub struct WalProcessorHandle {
    tx: Sender<WalJob>,
    join: Option<JoinHandle<Result<(), ProcessorError>>>,
}

impl WalProcessorHandle {
    /// Spawns a worker that owns [`CollectionStorage`] for `collection` under `root`.
    pub fn spawn(
        root: PathBuf,
        collection: String,
        collection_id: u32,
        initial_mmap: Option<usize>,
    ) -> Result<Self, ProcessorError> {
        let (tx, rx) = mpsc::channel();
        let join = std::thread::Builder::new()
            .name("dnadb-wal-processor".to_string())
            .spawn(move || worker_loop(rx, root, collection, collection_id, initial_mmap))
            .map_err(|e| ProcessorError::WorkerFailed(format!("spawn worker: {e}")))?;

        Ok(Self {
            tx,
            join: Some(join),
        })
    }

    /// Queue one WAL entry for processing (same `sequence` and payload as written to WAL).
    pub fn submit(&self, sequence: u64, payload: Vec<u8>) -> Result<(), ProcessorError> {
        self.tx
            .send(WalJob::Entry { sequence, payload })
            .map_err(|_| ProcessorError::ChannelClosed)
    }

    /// Stop the worker and join; returns the worker’s final `Result`.
    pub fn shutdown(mut self) -> Result<(), ProcessorError> {
        let _ = self.tx.send(WalJob::Shutdown);
        let join = self.join.take().ok_or_else(|| {
            ProcessorError::WorkerFailed("processor already shut down".to_string())
        })?;
        match join.join() {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(ProcessorError::WorkerFailed(format!("{e:?}"))),
        }
    }
}

fn worker_loop(
    rx: Receiver<WalJob>,
    root: PathBuf,
    collection: String,
    collection_id: u32,
    initial_mmap: Option<usize>,
) -> Result<(), ProcessorError> {
    let mut storage = CollectionStorage::open_or_create(&root, &collection, initial_mmap)?;
    let codec = BincodeStrandCodec;

    while let Ok(job) = rx.recv() {
        match job {
            WalJob::Entry { sequence, payload } => {
                process_wal_entry(&mut storage, &codec, collection_id, sequence, &payload)?;
            }
            WalJob::Shutdown => break,
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{process_wal_entry, strand_from_wal_payload, WalProcessorHandle};
    use crate::codec::{BincodeStrandCodec, StrandCodec};
    use crate::complement::is_valid_pairing;
    use crate::storage::CollectionStorage;
    use tempfile::tempdir;

    #[test]
    fn strand_from_payload_encodes_and_pairs_complement() {
        let raw = b"hello-wal";
        let s = strand_from_wal_payload(7, 42, raw);
        assert_eq!(s.collection_id, 7);
        assert_eq!(s.version, 42);
        assert!(is_valid_pairing(&s.codons, &s.complement));
        assert_eq!(s.introns.len(), 1);
        assert_eq!(s.introns[0].field_name, "_payload");
    }

    #[test]
    fn process_wal_entry_persists_strand_round_trip() {
        let dir = tempdir().expect("tempdir");
        let mut storage =
            CollectionStorage::open_or_create(dir.path(), "users", Some(256 * 1024)).expect("open");
        let codec = BincodeStrandCodec;

        process_wal_entry(&mut storage, &codec, 1, 1, b"first").expect("process");
        process_wal_entry(&mut storage, &codec, 1, 2, b"second").expect("process");

        let bytes = std::fs::read(storage.paths.strands.as_path()).expect("read strands");
        let mut pos = 0usize;
        let first = codec.decode_strand(&bytes[pos..]).expect("decode first");
        pos += codec.encode_strand(&first).expect("encode first").len();
        let second = codec.decode_strand(&bytes[pos..]).expect("decode second");

        assert_eq!(first.signature, super::strand_signature(1));
        assert_eq!(first.introns[0].value_hash, super::fnv1a64(b"first"));
        assert_eq!(second.signature, super::strand_signature(2));
        assert_eq!(second.introns[0].value_hash, super::fnv1a64(b"second"));
    }

    #[test]
    fn background_processor_drains_jobs() {
        let dir = tempdir().expect("tempdir");
        let root = dir.path().to_path_buf();
        let handle = WalProcessorHandle::spawn(root.clone(), "users".to_string(), 9, Some(256 * 1024))
            .expect("spawn");

        handle.submit(1, b"a".to_vec()).expect("submit");
        handle.submit(2, b"b".to_vec()).expect("submit");
        handle.shutdown().expect("shutdown");

        let paths = CollectionStorage::open_or_create(&root, "users", Some(256 * 1024))
            .expect("reopen")
            .paths;
        let bytes = std::fs::read(paths.strands).expect("read");
        let codec = BincodeStrandCodec;
        let s1 = codec.decode_strand(&bytes).expect("s1");
        let l1 = codec.encode_strand(&s1).expect("e1").len();
        let s2 = codec.decode_strand(&bytes[l1..]).expect("s2");
        assert_eq!(s1.collection_id, 9);
        assert_eq!(s2.collection_id, 9);
    }
}
