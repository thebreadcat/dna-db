//! Segment metadata for guided scan skip logic (bloom filters + numeric block stats).

use std::collections::HashMap;

use crate::encoding::{decode_codons_to_bytes, EncodedPayload};
use crate::model::{Intron, Strand};
use crate::processor::fnv1a64;

use super::ast::QueryLiteral;
use super::guide::{Clause, GuidePattern, RangeOp};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BloomFilter {
    bits: Vec<u8>,
    pub hash_count: u8,
}

impl BloomFilter {
    pub fn new(bits_len: usize, hash_count: u8) -> Self {
        let n = bits_len.max(8);
        Self {
            bits: vec![0u8; n],
            hash_count: hash_count.max(1),
        }
    }

    pub fn add(&mut self, value: &[u8]) {
        for seed in 0..self.hash_count {
            let bit = hash_with_seed(value, seed as u64) % self.bits.len() as u64;
            self.bits[bit as usize] = 1;
        }
    }

    pub fn might_contain(&self, value: &[u8]) -> bool {
        (0..self.hash_count).all(|seed| {
            let bit = hash_with_seed(value, seed as u64) % self.bits.len() as u64;
            self.bits[bit as usize] == 1
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldStats {
    pub min_numeric: u64,
    pub max_numeric: u64,
    pub has_numeric: bool,
    pub bloom: BloomFilter,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentMeta {
    pub segment_id: u32,
    pub record_count: u64,
    pub field_stats: HashMap<u64, FieldStats>,
}

fn hash_with_seed(value: &[u8], seed: u64) -> u64 {
    let mut b = Vec::with_capacity(8 + value.len());
    b.extend_from_slice(&seed.to_le_bytes());
    b.extend_from_slice(value);
    let h = blake3::hash(&b);
    let mut out = [0u8; 8];
    out.copy_from_slice(&h.as_bytes()[..8]);
    u64::from_le_bytes(out)
}

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

fn as_numeric_u64(value: &[u8]) -> Option<u64> {
    if let Ok(lit) = bincode::deserialize::<QueryLiteral>(value) {
        return query_literal_to_u64(&lit);
    }
    if let Ok(s) = std::str::from_utf8(value) {
        return s.parse::<u64>().ok();
    }
    None
}

fn query_literal_to_u64(lit: &QueryLiteral) -> Option<u64> {
    match lit {
        QueryLiteral::U64(v) => Some(*v),
        QueryLiteral::I64(v) if *v >= 0 => Some(*v as u64),
        _ => None,
    }
}

fn field_hash(name: &str) -> u64 {
    fnv1a64(name.as_bytes())
}

impl SegmentMeta {
    pub fn build(
        segment_id: u32,
        strands: &[Strand],
        tracked_fields: &[&str],
        bloom_bits: usize,
        bloom_hashes: u8,
    ) -> Self {
        let mut field_stats = HashMap::<u64, FieldStats>::new();
        for field in tracked_fields {
            field_stats.insert(
                field_hash(field),
                FieldStats {
                    min_numeric: u64::MAX,
                    max_numeric: 0,
                    has_numeric: false,
                    bloom: BloomFilter::new(bloom_bits, bloom_hashes),
                },
            );
        }

        for s in strands {
            for i in &s.introns {
                let h = field_hash(&i.field_name);
                let Some(stats) = field_stats.get_mut(&h) else {
                    continue;
                };
                if let Some(bytes) = intron_payload_bytes(s, i) {
                    stats.bloom.add(&bytes);
                    if let Some(n) = as_numeric_u64(&bytes) {
                        stats.has_numeric = true;
                        stats.min_numeric = stats.min_numeric.min(n);
                        stats.max_numeric = stats.max_numeric.max(n);
                    }
                }
            }
        }
        Self {
            segment_id,
            record_count: strands.len() as u64,
            field_stats,
        }
    }
}

pub fn should_skip_segment(meta: &SegmentMeta, pattern: &GuidePattern) -> bool {
    pattern
        .clauses
        .iter()
        .any(|c| should_skip_clause(meta, c))
}

fn should_skip_clause(meta: &SegmentMeta, clause: &Clause) -> bool {
    let field = match clause {
        Clause::ExactMatch { field_intron, .. } => field_intron,
        Clause::RangeMatch { field_intron, .. } => field_intron,
        Clause::LikePattern { .. } => return false,
    };
    let Some(stats) = meta.field_stats.get(&field_hash(field)) else {
        return false;
    };
    match clause {
        Clause::ExactMatch { operand_wire, .. } => !stats.bloom.might_contain(operand_wire),
        Clause::RangeMatch {
            operator,
            operand_wire,
            ..
        } => {
            let Some(n) = as_numeric_u64(operand_wire) else {
                return false;
            };
            if !stats.has_numeric {
                return false;
            }
            match operator {
                RangeOp::GreaterThan => stats.max_numeric <= n,
                RangeOp::GreaterOrEqual => stats.max_numeric < n,
                RangeOp::LessThan => stats.min_numeric >= n,
                RangeOp::LessOrEqual => stats.min_numeric > n,
                RangeOp::NotEqual => stats.min_numeric == n && stats.max_numeric == n,
            }
        }
        Clause::LikePattern { .. } => false,
    }
}

#[cfg(test)]
mod tests {
    use crate::encoding::encode_bytes_to_codons;
    use crate::processor::strand_from_wal_payload;
    use crate::query::guide::{Clause, GuidePattern};
    use proptest::prelude::*;

    use super::{should_skip_segment, SegmentMeta};

    #[test]
    fn bloom_skips_eq_when_absent() {
        let seg = vec![
            strand_from_wal_payload(1, 1, b"alice@example.com"),
            strand_from_wal_payload(1, 2, b"bob@example.com"),
        ];
        let meta = SegmentMeta::build(1, &seg, &["_payload"], 512, 3);
        let p = GuidePattern {
            collection: "c".into(),
            clauses: vec![Clause::ExactMatch {
                field_intron: "_payload".into(),
                operand_wire: b"charlie@example.com".to_vec(),
                operand_codons: encode_bytes_to_codons(b"charlie@example.com"),
            }],
            includes: vec![],
            overlay: None,
            order_by: None,
            limit: None,
        };
        assert!(should_skip_segment(&meta, &p));
    }

    proptest! {
        #[test]
        fn bloom_never_false_negative(values in prop::collection::vec(any::<u64>(), 1..400)) {
            let mut bloom = super::BloomFilter::new(10_000, 4);
            for v in &values {
                bloom.add(&v.to_le_bytes());
            }
            for v in &values {
                prop_assert!(bloom.might_contain(&v.to_le_bytes()));
            }
        }
    }
}

