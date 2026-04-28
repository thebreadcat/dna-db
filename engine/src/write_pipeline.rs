//! Layer 2 write pipeline: WAL + memtable + immutable segment flush (**raw JSON payloads**, no strands).
//!
//! Used by [`crate::runtime::EngineRuntime::execute_raw_segment_bulk_insert`] under `data_dir/raw_journal/<collection>/`
//! (via inner name `"raw"`) so WAL files never collide with [`crate::transaction_durable`] `*.wal` at the data root.
//! Strand materialization is deferred to a future segment seal / compaction step.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::wal::{Wal, WalError};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct SegmentManifest {
    next_segment_id: u32,
    sealed_wal_sequence: u64,
    segments: Vec<SegmentInfo>,
}

impl Default for SegmentManifest {
    fn default() -> Self {
        Self {
            next_segment_id: 1,
            sealed_wal_sequence: 0,
            segments: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SegmentInfo {
    pub segment_id: u32,
    pub path: String,
    pub min_record_id: u64,
    pub max_record_id: u64,
    pub records: usize,
    pub max_wal_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct SegmentRecord {
    record_id: u64,
    payload: Vec<u8>,
    wal_sequence: u64,
}

/// One decoded raw WAL append (after [`Wal`] framing): `(record_id, json_payload)` bincode.
#[derive(Debug, Clone)]
pub struct RawWalDecoded {
    pub wal_sequence: u64,
    pub record_id: u64,
    pub payload_json: Vec<u8>,
}

fn decode_raw_wal_inner(payload: &[u8]) -> Result<(u64, Vec<u8>), WritePipelineError> {
    let (record_id, payload_json): (u64, Vec<u8>) = bincode::deserialize(payload)?;
    Ok((record_id, payload_json))
}

#[derive(Debug, Clone, Default)]
pub struct Memtable {
    rows: BTreeMap<u64, SegmentRecord>,
}

impl Memtable {
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    fn upsert(&mut self, record_id: u64, payload: Vec<u8>, wal_sequence: u64) {
        self.rows.insert(
            record_id,
            SegmentRecord {
                record_id,
                payload,
                wal_sequence,
            },
        );
    }

    fn take_all(&mut self) -> Vec<SegmentRecord> {
        self.rows.values().cloned().collect()
    }
}

#[derive(Debug, Error)]
pub enum WritePipelineError {
    #[error("wal: {0}")]
    Wal(#[from] WalError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("serde json: {0}")]
    SerdeJson(#[from] serde_json::Error),
    #[error("bincode: {0}")]
    Bincode(#[from] bincode::Error),
}

pub struct LsmWritePipeline {
    root: PathBuf,
    collection: String,
    wal: Wal,
    memtable: Memtable,
    memtable_capacity_records: usize,
    manifest: SegmentManifest,
    segment_index: HashMap<u64, SegmentRecord>,
}

impl LsmWritePipeline {
    pub fn open_or_create(
        root: &Path,
        collection: &str,
        memtable_capacity_records: usize,
    ) -> Result<Self, WritePipelineError> {
        fs::create_dir_all(root)?;
        let wal = Wal::open_or_create(root, collection)?;
        let mut out = Self {
            root: root.to_path_buf(),
            collection: collection.to_string(),
            wal,
            memtable: Memtable::default(),
            memtable_capacity_records: memtable_capacity_records.max(1),
            manifest: load_manifest(root, collection)?,
            segment_index: HashMap::new(),
        };
        out.rebuild_segment_index()?;
        Ok(out)
    }

    pub fn upsert(&mut self, record_id: u64, payload: &[u8]) -> Result<u64, WritePipelineError> {
        let wal_payload = encode_wal_record(record_id, payload.to_vec())?;
        let seq = self.wal.append(&wal_payload)?;
        self.memtable.upsert(record_id, payload.to_vec(), seq);
        if self.memtable.len() >= self.memtable_capacity_records {
            self.flush_memtable()?;
        }
        Ok(seq)
    }

    pub fn flush_memtable(&mut self) -> Result<(), WritePipelineError> {
        if self.memtable.is_empty() {
            return Ok(());
        }
        let rows = self.memtable.take_all();
        let segment_id = self.manifest.next_segment_id;
        self.manifest.next_segment_id = self.manifest.next_segment_id.saturating_add(1);
        let segment_dir = self.root.join(format!("{}.segments", self.collection));
        fs::create_dir_all(&segment_dir)?;
        let segment_path = segment_dir.join(format!("seg_{segment_id:05}.dat"));

        let mut bytes = Vec::new();
        let mut min_record_id = u64::MAX;
        let mut max_record_id = 0u64;
        let mut max_wal_sequence = 0u64;
        for row in &rows {
            min_record_id = min_record_id.min(row.record_id);
            max_record_id = max_record_id.max(row.record_id);
            max_wal_sequence = max_wal_sequence.max(row.wal_sequence);
            let encoded = bincode::serialize(row)?;
            bytes.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&encoded);
            self.segment_index.insert(row.record_id, row.clone());
        }
        fs::write(&segment_path, bytes)?;

        let info = SegmentInfo {
            segment_id,
            path: segment_path
                .strip_prefix(&self.root)
                .unwrap_or(&segment_path)
                .to_string_lossy()
                .to_string(),
            min_record_id,
            max_record_id,
            records: rows.len(),
            max_wal_sequence,
        };
        self.manifest.sealed_wal_sequence = self.manifest.sealed_wal_sequence.max(max_wal_sequence);
        self.manifest.segments.push(info);
        save_manifest(&self.root, &self.collection, &self.manifest)?;

        self.wal.sync()?;
        self.memtable = Memtable::default();
        Ok(())
    }

    pub fn get(&self, record_id: u64) -> Option<Vec<u8>> {
        self.memtable
            .rows
            .get(&record_id)
            .map(|r| r.payload.clone())
            .or_else(|| self.segment_index.get(&record_id).map(|r| r.payload.clone()))
    }

    pub fn sealed_wal_sequence(&self) -> u64 {
        self.manifest.sealed_wal_sequence
    }

    pub fn segment_count(&self) -> usize {
        self.manifest.segments.len()
    }

    fn rebuild_segment_index(&mut self) -> Result<(), WritePipelineError> {
        self.segment_index.clear();
        for seg in &self.manifest.segments {
            let full = self.root.join(&seg.path);
            let data = fs::read(full)?;
            for row in decode_segment_records(&data)? {
                self.segment_index.insert(row.record_id, row);
            }
        }
        Ok(())
    }

    /// Decode raw-journal WAL entries with sequence **≥ `from_sequence`** (typically `last_applied + 1`).
    /// Caps at `limit` entries so callers can batch background materialization.
    pub fn decode_raw_wal_from_sequence(
        &mut self,
        from_sequence: u64,
        limit: usize,
    ) -> Result<Vec<RawWalDecoded>, WritePipelineError> {
        let entries = self.wal.read_entries_from(from_sequence, Some(limit.max(1)))?;
        let mut out = Vec::with_capacity(entries.len());
        for e in entries {
            let (record_id, payload_json) = decode_raw_wal_inner(&e.payload)?;
            out.push(RawWalDecoded {
                wal_sequence: e.sequence,
                record_id,
                payload_json,
            });
        }
        Ok(out)
    }

    /// Highest WAL sequence present in the raw journal file (0 if empty/unreadable tail).
    pub fn raw_wal_high_water_sequence(&mut self) -> Result<u64, WritePipelineError> {
        let entries = self.wal.read_all_entries()?;
        Ok(entries.last().map(|e| e.sequence).unwrap_or(0))
    }
}

fn manifest_path(root: &Path, collection: &str) -> PathBuf {
    root.join(format!("{collection}.segments/manifest.json"))
}

fn load_manifest(root: &Path, collection: &str) -> Result<SegmentManifest, WritePipelineError> {
    let path = manifest_path(root, collection);
    if !path.exists() {
        return Ok(SegmentManifest::default());
    }
    let bytes = fs::read(path)?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn save_manifest(
    root: &Path,
    collection: &str,
    manifest: &SegmentManifest,
) -> Result<(), WritePipelineError> {
    let path = manifest_path(root, collection);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_vec_pretty(manifest)?)?;
    Ok(())
}

fn encode_wal_record(record_id: u64, payload: Vec<u8>) -> Result<Vec<u8>, WritePipelineError> {
    Ok(bincode::serialize(&(record_id, payload))?)
}

fn decode_segment_records(bytes: &[u8]) -> Result<Vec<SegmentRecord>, WritePipelineError> {
    let mut out = Vec::new();
    let mut pos = 0usize;
    while pos + 4 <= bytes.len() {
        let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().expect("4 bytes")) as usize;
        pos += 4;
        if pos + len > bytes.len() {
            break;
        }
        let row: SegmentRecord = bincode::deserialize(&bytes[pos..pos + len])?;
        out.push(row);
        pos += len;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::LsmWritePipeline;

    #[test]
    fn memtable_flush_creates_segment_and_seals_wal_progress() {
        let dir = tempdir().expect("tempdir");
        let mut pipe = LsmWritePipeline::open_or_create(dir.path(), "users", 2).expect("open");
        let s1 = pipe.upsert(1, br#"{"id":1}"#).expect("upsert1");
        let s2 = pipe.upsert(2, br#"{"id":2}"#).expect("upsert2");
        assert!(s2 > s1);
        assert_eq!(pipe.segment_count(), 1);
        assert!(pipe.sealed_wal_sequence() >= s2);
        assert_eq!(pipe.get(1).as_deref(), Some(br#"{"id":1}"#.as_ref()));
    }

    #[test]
    fn reopen_recovers_segment_index_and_reads_records() {
        let dir = tempdir().expect("tempdir");
        {
            let mut pipe = LsmWritePipeline::open_or_create(dir.path(), "users", 2).expect("open");
            pipe.upsert(7, br#"{"id":7,"email":"a@b.com"}"#)
                .expect("upsert7");
            pipe.upsert(8, br#"{"id":8,"email":"x@y.com"}"#)
                .expect("upsert8");
        }
        let reopened = LsmWritePipeline::open_or_create(dir.path(), "users", 2).expect("reopen");
        assert_eq!(
            reopened.get(7).as_deref(),
            Some(br#"{"id":7,"email":"a@b.com"}"#.as_ref())
        );
        assert_eq!(reopened.segment_count(), 1);
    }

    #[test]
    fn manual_flush_writes_pending_memtable() {
        let dir = tempdir().expect("tempdir");
        let mut pipe = LsmWritePipeline::open_or_create(dir.path(), "users", 10).expect("open");
        pipe.upsert(11, br#"{"id":11}"#).expect("upsert11");
        assert_eq!(pipe.segment_count(), 0);
        pipe.flush_memtable().expect("manual flush");
        assert_eq!(pipe.segment_count(), 1);
    }
}

