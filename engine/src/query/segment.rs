//! Segment metadata for guided scan skip logic (bloom filters + numeric block stats).

use std::collections::HashMap;

use crate::encoding::{decode_codons_to_bytes, EncodedPayload};
use crate::model::{Intron, Strand};
use crate::processor::fnv1a64;

use super::ast::QueryLiteral;
use super::guide::{Clause, GuidePattern, RangeOp};
const MAX_VALUE_CODES_PER_FIELD: usize = 254;

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
    /// Optional dictionary-style short-code map for frequent intron values (by hash).
    pub field_dictionaries: HashMap<u64, FieldCompressionDictionary>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentBlockMeta {
    pub block_id: u32,
    pub record_count: u32,
    pub field_stats: HashMap<u64, FieldStats>,
    pub field_dictionaries: HashMap<u64, FieldCompressionDictionary>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueCodebookEntry {
    pub code: u8,
    pub value_hash: u64,
    pub frequency: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldCompressionDictionary {
    pub total_values: u32,
    pub covered_values: u32,
    pub entries: Vec<ValueCodebookEntry>,
}

impl FieldCompressionDictionary {
    pub fn is_complete(&self) -> bool {
        self.total_values > 0 && self.covered_values == self.total_values
    }

    pub fn contains_hash(&self, hash: u64) -> bool {
        self.entries.iter().any(|e| e.value_hash == hash)
    }
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
        let mut value_freqs = HashMap::<u64, HashMap<u64, u32>>::new();
        for field in tracked_fields {
            let fh = field_hash(field);
            field_stats.insert(
                fh,
                FieldStats {
                    min_numeric: u64::MAX,
                    max_numeric: 0,
                    has_numeric: false,
                    bloom: BloomFilter::new(bloom_bits, bloom_hashes),
                },
            );
            value_freqs.insert(fh, HashMap::new());
        }

        for s in strands {
            for i in &s.introns {
                let h = field_hash(&i.field_name);
                let Some(stats) = field_stats.get_mut(&h) else {
                    continue;
                };
                if let Some(freqs) = value_freqs.get_mut(&h) {
                    *freqs.entry(i.value_hash).or_insert(0) += 1;
                }
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
        let field_dictionaries = build_field_dictionaries(&value_freqs);
        Self {
            segment_id,
            record_count: strands.len() as u64,
            field_stats,
            field_dictionaries,
        }
    }

    pub fn dictionary_entry_count(&self) -> usize {
        self.field_dictionaries.values().map(|d| d.entries.len()).sum()
    }

    pub fn dictionary_coverage_ratio(&self) -> f64 {
        let covered: u64 = self
            .field_dictionaries
            .values()
            .map(|d| d.covered_values as u64)
            .sum();
        let total: u64 = self
            .field_dictionaries
            .values()
            .map(|d| d.total_values as u64)
            .sum();
        if total == 0 {
            0.0
        } else {
            covered as f64 / total as f64
        }
    }
}

impl SegmentBlockMeta {
    pub fn build(
        block_id: u32,
        strands: &[Strand],
        tracked_fields: &[&str],
        bloom_bits: usize,
        bloom_hashes: u8,
    ) -> Self {
        let seg = SegmentMeta::build(0, strands, tracked_fields, bloom_bits, bloom_hashes);
        Self {
            block_id,
            record_count: strands.len() as u32,
            field_stats: seg.field_stats,
            field_dictionaries: seg.field_dictionaries,
        }
    }

    pub fn dictionary_entry_count(&self) -> usize {
        self.field_dictionaries.values().map(|d| d.entries.len()).sum()
    }

    pub fn dictionary_coverage_ratio(&self) -> f64 {
        let covered: u64 = self
            .field_dictionaries
            .values()
            .map(|d| d.covered_values as u64)
            .sum();
        let total: u64 = self
            .field_dictionaries
            .values()
            .map(|d| d.total_values as u64)
            .sum();
        if total == 0 {
            0.0
        } else {
            covered as f64 / total as f64
        }
    }
}

fn build_field_dictionaries(
    value_freqs: &HashMap<u64, HashMap<u64, u32>>,
) -> HashMap<u64, FieldCompressionDictionary> {
    let mut dictionaries = HashMap::new();
    for (field, freq_map) in value_freqs {
        let total_values: u32 = freq_map.values().copied().sum();
        let mut pairs: Vec<(u64, u32)> = freq_map.iter().map(|(h, c)| (*h, *c)).collect();
        pairs.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        let mut entries = Vec::new();
        let mut covered_values = 0u32;
        for (idx, (value_hash, frequency)) in pairs
            .into_iter()
            .take(MAX_VALUE_CODES_PER_FIELD)
            .enumerate()
        {
            let code = (idx + 1) as u8;
            covered_values += frequency;
            entries.push(ValueCodebookEntry {
                code,
                value_hash,
                frequency,
            });
        }
        dictionaries.insert(
            *field,
            FieldCompressionDictionary {
                total_values,
                covered_values,
                entries,
            },
        );
    }
    dictionaries
}

pub fn should_skip_segment(meta: &SegmentMeta, pattern: &GuidePattern) -> bool {
    pattern
        .clauses
        .iter()
        .any(|c| should_skip_clause(meta, c))
}

/// Segment skip decision using bloom/range stats only (dictionary disabled).
pub fn should_skip_segment_bloom_only(meta: &SegmentMeta, pattern: &GuidePattern) -> bool {
    pattern
        .clauses
        .iter()
        .any(|c| should_skip_clause_for_stats(&meta.field_stats, None, c))
}

pub fn should_skip_block(meta: &SegmentBlockMeta, pattern: &GuidePattern) -> bool {
    pattern
        .clauses
        .iter()
        .any(|c| should_skip_clause_for_stats(
            &meta.field_stats,
            Some(&meta.field_dictionaries),
            c,
        ))
}

/// Block skip decision using bloom/range stats only (dictionary disabled).
pub fn should_skip_block_bloom_only(meta: &SegmentBlockMeta, pattern: &GuidePattern) -> bool {
    pattern
        .clauses
        .iter()
        .any(|c| should_skip_clause_for_stats(&meta.field_stats, None, c))
}

fn should_skip_clause(meta: &SegmentMeta, clause: &Clause) -> bool {
    should_skip_clause_for_stats(&meta.field_stats, Some(&meta.field_dictionaries), clause)
}

fn should_skip_clause_for_stats(
    field_stats: &HashMap<u64, FieldStats>,
    field_dictionaries: Option<&HashMap<u64, FieldCompressionDictionary>>,
    clause: &Clause,
) -> bool {
    let field = match clause {
        Clause::ExactMatch { field_intron, .. } => field_intron,
        Clause::RangeMatch { field_intron, .. } => field_intron,
        Clause::LikePattern { .. } => return false,
    };
    let field_h = field_hash(field);
    let Some(stats) = field_stats.get(&field_h) else {
        return false;
    };
    match clause {
        Clause::ExactMatch { operand_wire, .. } => {
            // Dictionary fast-path: if a dictionary fully covers all values in this
            // segment/block and the operand hash is absent, skip immediately.
            if let Some(dict) = field_dictionaries
                .and_then(|m| m.get(&field_h))
                .filter(|d| d.is_complete())
            {
                let needle = fnv1a64(operand_wire);
                if !dict.contains_hash(needle) {
                    return true;
                }
            }
            !stats.bloom.might_contain(operand_wire)
        }
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

    use super::{should_skip_block, should_skip_segment, SegmentBlockMeta, SegmentMeta};

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

    #[test]
    fn block_bloom_skips_eq_when_absent() {
        let seg = vec![
            strand_from_wal_payload(1, 1, b"alice@example.com"),
            strand_from_wal_payload(1, 2, b"bob@example.com"),
        ];
        let meta = SegmentBlockMeta::build(1, &seg, &["_payload"], 512, 3);
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
        assert!(should_skip_block(&meta, &p));
    }

    #[test]
    fn dictionary_stats_capture_frequency_coverage() {
        let seg = vec![
            strand_from_wal_payload(1, 1, b"A"),
            strand_from_wal_payload(1, 2, b"A"),
            strand_from_wal_payload(1, 3, b"B"),
            strand_from_wal_payload(1, 4, b"C"),
        ];
        let meta = SegmentMeta::build(1, &seg, &["_payload"], 512, 3);
        let field = super::field_hash("_payload");
        let dict = meta
            .field_dictionaries
            .get(&field)
            .expect("dictionary for tracked field");
        assert!(dict.total_values >= 4);
        assert!(!dict.entries.is_empty());
        assert!(dict.covered_values > 0);
        assert!(meta.dictionary_entry_count() >= 1);
        assert!(meta.dictionary_coverage_ratio() > 0.0);
    }

    #[test]
    fn dictionary_complete_when_unique_values_fit_codebook() {
        let seg = vec![
            strand_from_wal_payload(1, 1, b"A"),
            strand_from_wal_payload(1, 2, b"A"),
            strand_from_wal_payload(1, 3, b"B"),
        ];
        let meta = SegmentMeta::build(1, &seg, &["_payload"], 512, 3);
        let field = super::field_hash("_payload");
        let dict = meta
            .field_dictionaries
            .get(&field)
            .expect("dictionary for tracked field");
        assert!(dict.is_complete());
        assert!(dict.contains_hash(crate::processor::fnv1a64(b"A")));
        assert!(!dict.contains_hash(crate::processor::fnv1a64(b"missing")));
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

