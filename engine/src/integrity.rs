//! Layer 3 integrity primitives: write-time CRC and background scrubber.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use thiserror::Error;

use crate::codec::{BincodeStrandCodec, CodecError, StrandCodec, STRAND_FORMAT_MAGIC};

pub const CRC_ENTRY_SIZE: usize = 12; // sequence u64 + crc32 u32

#[derive(Debug, Error)]
pub enum IntegrityError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("codec: {0}")]
    Codec(#[from] CodecError),
    #[error("storage: {0}")]
    Storage(#[from] crate::storage::StorageError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScrubReport {
    pub checked: usize,
    pub missing_crc: usize,
    pub mismatched_crc: usize,
}

pub fn compute_crc32(bytes: &[u8]) -> u32 {
    crc32fast::hash(bytes)
}

pub fn encode_crc_entry(sequence: u64, crc: u32) -> [u8; CRC_ENTRY_SIZE] {
    let mut out = [0u8; CRC_ENTRY_SIZE];
    out[0..8].copy_from_slice(&sequence.to_le_bytes());
    out[8..12].copy_from_slice(&crc.to_le_bytes());
    out
}

pub fn decode_crc_entries(meta_bytes: &[u8]) -> HashMap<u64, u32> {
    let mut out = HashMap::new();
    let end = meta_bytes
        .iter()
        .rposition(|b| *b != 0)
        .map(|i| i + 1)
        .unwrap_or(0);
    let mut pos = 0usize;
    while pos + CRC_ENTRY_SIZE <= end {
        let mut seq_buf = [0u8; 8];
        seq_buf.copy_from_slice(&meta_bytes[pos..pos + 8]);
        let sequence = u64::from_le_bytes(seq_buf);
        let mut crc_buf = [0u8; 4];
        crc_buf.copy_from_slice(&meta_bytes[pos + 8..pos + 12]);
        let crc = u32::from_le_bytes(crc_buf);
        out.insert(sequence, crc);
        pos += CRC_ENTRY_SIZE;
    }
    out
}

pub fn append_crc_entry_for_strand(
    storage: &mut crate::storage::CollectionStorage,
    sequence: u64,
    strand_frame: &[u8],
) -> Result<(), IntegrityError> {
    let crc = compute_crc32(strand_frame);
    let entry = encode_crc_entry(sequence, crc);
    storage.append_meta(&entry)?;
    Ok(())
}

/// Background scrubber: recompute CRC for every strand frame and compare to stored meta CRC entries.
pub fn scrub_collection(
    root: &Path,
    collection: &str,
    initial_mmap: Option<usize>,
) -> Result<ScrubReport, IntegrityError> {
    let storage = crate::storage::CollectionStorage::open_or_create(root, collection, initial_mmap)?;
    let strands_bytes = fs::read(storage.paths.strands.as_path())?;
    let meta_bytes = fs::read(storage.paths.meta.as_path())?;
    let crc_map = decode_crc_entries(&meta_bytes);
    let codec = BincodeStrandCodec;
    let mut pos = 0usize;
    let mut checked = 0usize;
    let mut missing_crc = 0usize;
    let mut mismatched_crc = 0usize;
    while pos + 6 <= strands_bytes.len() && strands_bytes[pos..pos + 4] == STRAND_FORMAT_MAGIC {
        let strand = codec.decode_strand(&strands_bytes[pos..])?;
        let frame_len = codec.encode_strand(&strand)?.len();
        let end = pos + frame_len;
        if end > strands_bytes.len() {
            break;
        }
        let frame = &strands_bytes[pos..end];
        let actual = compute_crc32(frame);
        checked += 1;
        match crc_map.get(&strand.version).copied() {
            Some(expected) if expected == actual => {}
            Some(_) => mismatched_crc += 1,
            None => missing_crc += 1,
        }
        pos = end;
    }
    Ok(ScrubReport {
        checked,
        missing_crc,
        mismatched_crc,
    })
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::codec::{BincodeStrandCodec, StrandCodec};
    use crate::processor::strand_from_wal_payload;
    use crate::storage::CollectionStorage;

    use super::{append_crc_entry_for_strand, compute_crc32, scrub_collection};

    #[test]
    fn crc_detects_single_bit_flip() {
        let codec = BincodeStrandCodec;
        let s = strand_from_wal_payload(1, 1, b"payload");
        let mut frame = codec.encode_strand(&s).expect("encode");
        let original = compute_crc32(&frame);
        frame[10] ^= 0x01;
        let changed = compute_crc32(&frame);
        assert_ne!(original, changed);
    }

    #[test]
    fn scrubber_reports_clean_collection() {
        let dir = tempdir().expect("tempdir");
        let mut storage =
            CollectionStorage::open_or_create(dir.path(), "users", Some(1024 * 1024)).expect("open");
        let codec = BincodeStrandCodec;
        let s1 = strand_from_wal_payload(1, 1, b"a");
        let s2 = strand_from_wal_payload(1, 2, b"b");
        let f1 = codec.encode_strand(&s1).expect("encode");
        let f2 = codec.encode_strand(&s2).expect("encode");
        storage.append_strands(&f1).expect("append");
        append_crc_entry_for_strand(&mut storage, s1.version, &f1).expect("crc");
        storage.append_strands(&f2).expect("append");
        append_crc_entry_for_strand(&mut storage, s2.version, &f2).expect("crc");
        storage.flush().expect("flush");
        let report = scrub_collection(dir.path(), "users", Some(1024 * 1024)).expect("scrub");
        assert_eq!(report.checked, 2);
        assert_eq!(report.missing_crc, 0);
        assert_eq!(report.mismatched_crc, 0);
    }
}

