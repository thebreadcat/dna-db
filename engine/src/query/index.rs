//! Global hash index helpers for exact-match lookups (in-memory + persisted).

use std::collections::HashMap;
use std::path::Path;

use crate::encoding::{decode_codons_to_bytes, EncodedPayload};
use crate::model::{Intron, Strand};
use super::ast::QueryLiteral;
use super::guide::RangeOp;
use thiserror::Error;

fn intron_payload_bytes(strand: &Strand, intron: &Intron) -> Option<Vec<u8>> {
    let start = intron.codon_offset as usize;
    let len = intron.codon_length as usize;
    let end = start.checked_add(len)?;
    if end > strand.codons.len() {
        return None;
    }
    let payload = EncodedPayload {
        codons: strand.codons[start..end].to_vec(),
        original_len: (len * 3) / 4,
    };
    decode_codons_to_bytes(&payload).ok()
}

pub fn build_exact_hash_index(strands: &[Strand], field: &str) -> HashMap<Vec<u8>, Vec<[u8; 8]>> {
    let mut out: HashMap<Vec<u8>, Vec<[u8; 8]>> = HashMap::new();
    for s in strands {
        if field == "_signature" {
            out.entry(s.signature.to_vec()).or_default().push(s.signature);
            continue;
        }
        for i in &s.introns {
            if i.field_name != field {
                continue;
            }
            if let Some(v) = intron_payload_bytes(s, i) {
                out.entry(v).or_default().push(s.signature);
            }
        }
    }
    out
}

pub fn exact_lookup_signatures(
    index: &HashMap<Vec<u8>, Vec<[u8; 8]>>,
    value: &[u8],
) -> Vec<[u8; 8]> {
    index.get(value).cloned().unwrap_or_default()
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GlobalHashIndex {
    pub field: String,
    pub map: HashMap<Vec<u8>, Vec<[u8; 8]>>,
}

#[derive(Debug, Error)]
pub enum GlobalHashIndexError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialize: {0}")]
    Serialize(#[from] bincode::Error),
}

impl GlobalHashIndex {
    pub fn build(field: impl Into<String>, strands: &[Strand]) -> Self {
        let field = field.into();
        Self {
            map: build_exact_hash_index(strands, &field),
            field,
        }
    }

    pub fn lookup(&self, value: &[u8]) -> Vec<[u8; 8]> {
        exact_lookup_signatures(&self.map, value)
    }

    pub fn save_to_path(&self, path: &Path) -> Result<(), GlobalHashIndexError> {
        let bytes = bincode::serialize(self)?;
        std::fs::write(path, bytes)?;
        Ok(())
    }

    pub fn load_from_path(path: &Path) -> Result<Self, GlobalHashIndexError> {
        let bytes = std::fs::read(path)?;
        Ok(bincode::deserialize(&bytes)?)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GlobalRangeIndex {
    pub field: String,
    /// Sorted by numeric value for range lookups.
    pub entries: Vec<(u64, [u8; 8])>,
}

impl GlobalRangeIndex {
    pub fn build(field: impl Into<String>, strands: &[Strand]) -> Self {
        let field = field.into();
        let mut entries = Vec::new();
        for s in strands {
            for i in &s.introns {
                if i.field_name != field {
                    continue;
                }
                if let Some(v) = intron_payload_bytes(s, i)
                    .and_then(|b| as_numeric_u64(&b))
                {
                    entries.push((v, s.signature));
                }
            }
        }
        entries.sort_by_key(|(v, _)| *v);
        Self { field, entries }
    }

    pub fn lookup(&self, op: RangeOp, needle: u64) -> Vec<[u8; 8]> {
        self.entries
            .iter()
            .filter_map(|(v, sig)| {
                let ok = match op {
                    RangeOp::GreaterThan => *v > needle,
                    RangeOp::GreaterOrEqual => *v >= needle,
                    RangeOp::LessThan => *v < needle,
                    RangeOp::LessOrEqual => *v <= needle,
                    RangeOp::NotEqual => *v != needle,
                };
                if ok { Some(*sig) } else { None }
            })
            .collect()
    }

    pub fn save_to_path(&self, path: &Path) -> Result<(), GlobalHashIndexError> {
        let bytes = bincode::serialize(self)?;
        std::fs::write(path, bytes)?;
        Ok(())
    }

    pub fn load_from_path(path: &Path) -> Result<Self, GlobalHashIndexError> {
        let bytes = std::fs::read(path)?;
        Ok(bincode::deserialize(&bytes)?)
    }
}

fn as_numeric_u64(value: &[u8]) -> Option<u64> {
    if let Ok(lit) = bincode::deserialize::<QueryLiteral>(value) {
        return match lit {
            QueryLiteral::U64(v) => Some(v),
            QueryLiteral::I64(v) if v >= 0 => Some(v as u64),
            _ => None,
        };
    }
    if let Ok(s) = std::str::from_utf8(value) {
        return s.parse::<u64>().ok();
    }
    None
}

#[cfg(test)]
mod tests {
    use crate::processor::strand_from_wal_payload;
    use crate::query::RangeOp;

    use super::{
        build_exact_hash_index, exact_lookup_signatures, GlobalHashIndex, GlobalRangeIndex,
    };

    #[test]
    fn hash_index_returns_expected_signatures_for_payload() {
        let s1 = strand_from_wal_payload(1, 1, b"alice@example.com");
        let s2 = strand_from_wal_payload(1, 2, b"bob@example.com");
        let s3 = strand_from_wal_payload(1, 3, b"alice@example.com");
        let idx = build_exact_hash_index(&[s1.clone(), s2.clone(), s3.clone()], "_payload");
        let alice = exact_lookup_signatures(&idx, b"alice@example.com");
        assert_eq!(alice.len(), 2);
        assert!(alice.contains(&s1.signature));
        assert!(alice.contains(&s3.signature));
        let none = exact_lookup_signatures(&idx, b"none@example.com");
        assert!(none.is_empty());
    }

    #[test]
    fn global_hash_index_persists_and_loads() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("users.email.hashidx");
        let s1 = strand_from_wal_payload(1, 1, b"alice@example.com");
        let s2 = strand_from_wal_payload(1, 2, b"bob@example.com");
        let idx = GlobalHashIndex::build("_payload", &[s1.clone(), s2.clone()]);
        idx.save_to_path(&path).expect("save");
        let loaded = GlobalHashIndex::load_from_path(&path).expect("load");
        assert_eq!(loaded.field, "_payload");
        let alice = loaded.lookup(b"alice@example.com");
        assert_eq!(alice.len(), 1);
        assert_eq!(alice[0], s1.signature);
    }

    #[test]
    fn global_range_index_supports_range_lookups() {
        let s1 = strand_from_wal_payload(1, 1, b"10");
        let s2 = strand_from_wal_payload(1, 2, b"20");
        let s3 = strand_from_wal_payload(1, 3, b"30");
        let idx = GlobalRangeIndex::build("_payload", &[s1.clone(), s2.clone(), s3.clone()]);
        let gt_15 = idx.lookup(RangeOp::GreaterThan, 15);
        assert_eq!(gt_15.len(), 2);
        assert!(gt_15.contains(&s2.signature));
        assert!(gt_15.contains(&s3.signature));
        let lte_20 = idx.lookup(RangeOp::LessOrEqual, 20);
        assert_eq!(lte_20.len(), 2);
    }
}

