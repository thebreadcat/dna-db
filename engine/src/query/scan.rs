//! Parallel CRISPR-style strand scan: partition work across Rayon’s thread pool (CPU cores).
//!
//! Stage 2 v1 applies only the intron [`guide_introns_fast_match`](super::fast_match::guide_introns_fast_match)
//! prefilter; full decode + value verify belongs in later executor work.

use rayon::prelude::*;
use std::collections::{HashMap, HashSet};

use crate::model::Strand;

use super::fast_match::guide_introns_fast_match;
use super::guide::GuidePattern;
use super::index::GlobalHashIndex;
use super::index::GlobalRangeIndex;
use super::segment::{should_skip_segment, SegmentMeta};

/// Minimum number of strands before using Rayon’s parallel iterator (below this, sequential scan).
pub const PARALLEL_STRAND_THRESHOLD: usize = 32;

/// Optional filters for a scan (collection id only for now; name resolution is SDK/server concern).
#[derive(Debug, Clone, Default)]
pub struct ScanConfig {
    /// When set, only strands with this `Strand.collection_id` are considered.
    pub collection_id: Option<u32>,
    /// Optional segment metadata for skip-logic. Must align with `segment_ranges` by index.
    pub segment_metas: Option<Vec<SegmentMeta>>,
    /// Optional strand index ranges (`start..end`) per segment.
    pub segment_ranges: Option<Vec<(usize, usize)>>,
    /// Optional indexed field hints used by planner path selection.
    pub indexed_fields: Option<HashSet<String>>,
    /// Optional pre-built exact hash indexes keyed by field name.
    pub global_hash_indexes: Option<HashMap<String, GlobalHashIndex>>,
    /// Optional pre-built numeric range indexes keyed by field name.
    pub global_range_indexes: Option<HashMap<String, GlobalRangeIndex>>,
}

fn strand_in_scope(s: &Strand, config: &ScanConfig) -> bool {
    match config.collection_id {
        Some(id) => s.collection_id == id,
        None => true,
    }
}

fn segment_ranges_for_scan(
    pattern: &GuidePattern,
    strands_len: usize,
    config: &ScanConfig,
) -> Vec<(usize, usize)> {
    let Some(ranges) = config.segment_ranges.as_ref() else {
        return vec![(0, strands_len)];
    };
    let Some(metas) = config.segment_metas.as_ref() else {
        return ranges.clone();
    };
    ranges
        .iter()
        .enumerate()
        .filter_map(|(idx, range)| {
            let skip = metas
                .get(idx)
                .is_some_and(|m| should_skip_segment(m, pattern));
            if skip { None } else { Some(*range) }
        })
        .collect()
}

/// Sequential scan (single thread).
pub fn scan_strands_sequential(
    pattern: &GuidePattern,
    strands: &[Strand],
    config: &ScanConfig,
) -> Vec<[u8; 8]> {
    let mut out = Vec::new();
    for (start, end) in segment_ranges_for_scan(pattern, strands.len(), config) {
        for s in &strands[start.min(strands.len())..end.min(strands.len())] {
            if !strand_in_scope(s, config) {
                continue;
            }
            if guide_introns_fast_match(pattern, &s.introns) {
                out.push(s.signature);
            }
        }
    }
    out
}

/// Parallel scan using Rayon (work-stealing across cores).
pub fn scan_strands_parallel(
    pattern: &GuidePattern,
    strands: &[Strand],
    config: &ScanConfig,
) -> Vec<[u8; 8]> {
    let ranges = segment_ranges_for_scan(pattern, strands.len(), config);
    ranges
        .par_iter()
        .flat_map_iter(|(start, end)| {
            strands[(*start).min(strands.len())..(*end).min(strands.len())]
                .iter()
                .filter(|s| strand_in_scope(s, config))
                .filter(|s| guide_introns_fast_match(pattern, &s.introns))
                .map(|s| s.signature)
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Picks parallel or sequential implementation based on [`PARALLEL_STRAND_THRESHOLD`].
pub fn scan_strands(pattern: &GuidePattern, strands: &[Strand], config: &ScanConfig) -> Vec<[u8; 8]> {
    if strands.len() >= PARALLEL_STRAND_THRESHOLD {
        scan_strands_parallel(pattern, strands, config)
    } else {
        scan_strands_sequential(pattern, strands, config)
    }
}

#[cfg(test)]
mod tests {
    use super::{scan_strands, scan_strands_parallel, scan_strands_sequential, ScanConfig};
    use crate::encoding::encode_bytes_to_codons;
    use crate::processor::strand_from_wal_payload;
    use crate::query::guide::{Clause, GuidePattern, RangeOp};
    use crate::query::segment::SegmentMeta;

    fn pattern_payload_exact(wire: &[u8]) -> GuidePattern {
        GuidePattern {
            collection: "c".into(),
            clauses: vec![Clause::ExactMatch {
                field_intron: "_payload".into(),
                operand_wire: wire.to_vec(),
                operand_codons: encode_bytes_to_codons(wire),
            }],
            includes: vec![],
            overlay: None,
            order_by: None,
            limit: None,
        }
    }

    #[test]
    fn parallel_matches_sequential_sorted() {
        let mut strands: Vec<_> = (0u64..48)
            .map(|i| strand_from_wal_payload(7, i + 1, format!("s{i}").as_bytes()))
            .collect();
        // One strand uses a payload we will query for
        strands[12] = strand_from_wal_payload(7, 99, b"needle");

        let pat = pattern_payload_exact(b"needle");
        let cfg = ScanConfig {
            collection_id: Some(7),
            ..ScanConfig::default()
        };

        let mut a = scan_strands_sequential(&pat, &strands, &cfg);
        a.sort_unstable();
        let mut b = scan_strands_parallel(&pat, &strands, &cfg);
        b.sort_unstable();
        let mut c = scan_strands(&pat, &strands, &cfg);
        c.sort_unstable();

        assert_eq!(a, b);
        assert_eq!(a, c);
        assert_eq!(a.len(), 1);
        assert_eq!(a[0], strand_from_wal_payload(7, 99, b"needle").signature);
    }

    #[test]
    fn collection_filter_excludes_other_ids() {
        let s1 = strand_from_wal_payload(1, 1, b"a");
        let s2 = strand_from_wal_payload(2, 2, b"a");
        let strands = vec![s1, s2];
        let pat = pattern_payload_exact(b"a");
        let cfg = ScanConfig {
            collection_id: Some(1),
            ..ScanConfig::default()
        };
        let hits = scan_strands_parallel(&pat, &strands, &cfg);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0], strands[0].signature);
    }

    #[test]
    fn empty_strands_empty_hits() {
        let pat = pattern_payload_exact(b"x");
        let hits = scan_strands(&pat, &[], &ScanConfig::default());
        assert!(hits.is_empty());
    }

    #[test]
    fn range_clause_requires_intron_field() {
        let s = strand_from_wal_payload(1, 1, b"data");
        let pat = GuidePattern {
            collection: "c".into(),
            clauses: vec![Clause::RangeMatch {
                field_intron: "_payload".into(),
                operator: RangeOp::GreaterThan,
                operand_wire: vec![0],
                operand_codons: encode_bytes_to_codons(&[0u8]),
            }],
            includes: vec![],
            overlay: None,
            order_by: None,
            limit: None,
        };
        let hits = scan_strands_sequential(&pat, std::slice::from_ref(&s), &ScanConfig::default());
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn segment_meta_can_skip_non_matching_segment() {
        let mut strands: Vec<_> = (0u64..20)
            .map(|i| strand_from_wal_payload(1, i + 1, format!("user{i}@example.com").as_bytes()))
            .collect();
        strands[12] = strand_from_wal_payload(1, 13, b"target@gmail.com");
        let ranges = vec![(0, 10), (10, 20)];
        let metas = vec![
            SegmentMeta::build(1, &strands[0..10], &["_payload"], 512, 3),
            SegmentMeta::build(2, &strands[10..20], &["_payload"], 512, 3),
        ];
        let pat = GuidePattern {
            collection: "c".into(),
            clauses: vec![Clause::ExactMatch {
                field_intron: "_payload".into(),
                operand_wire: b"target@gmail.com".to_vec(),
                operand_codons: encode_bytes_to_codons(b"target@gmail.com"),
            }],
            includes: vec![],
            overlay: None,
            order_by: None,
            limit: None,
        };
        let cfg = ScanConfig {
            collection_id: Some(1),
            segment_metas: Some(metas),
            segment_ranges: Some(ranges),
            ..ScanConfig::default()
        };
        let hits = scan_strands(&pat, &strands, &cfg);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0], strand_from_wal_payload(1, 13, b"target@gmail.com").signature);
    }
}
