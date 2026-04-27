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
use super::segment::{should_skip_block, should_skip_segment, SegmentBlockMeta, SegmentMeta};

/// Minimum number of strands before using Rayon’s parallel iterator (below this, sequential scan).
pub const PARALLEL_STRAND_THRESHOLD: usize = 32;
/// Lower bound for adaptive parallel scan chunk size.
pub const PARALLEL_SCAN_CHUNK_MIN: usize = 512;
/// Upper bound for adaptive parallel scan chunk size.
pub const PARALLEL_SCAN_CHUNK_MAX: usize = 8192;
/// Target number of work chunks per Rayon worker thread.
pub const PARALLEL_TASKS_PER_THREAD_TARGET: usize = 4;

/// Optional filters for a scan (collection id only for now; name resolution is SDK/server concern).
#[derive(Debug, Clone, Default)]
pub struct ScanConfig {
    /// When set, only strands with this `Strand.collection_id` are considered.
    pub collection_id: Option<u32>,
    /// Optional segment metadata for skip-logic. Must align with `segment_ranges` by index.
    pub segment_metas: Option<Vec<SegmentMeta>>,
    /// Optional strand index ranges (`start..end`) per segment.
    pub segment_ranges: Option<Vec<(usize, usize)>>,
    /// Optional block metadata per segment for in-segment skip pruning.
    /// Outer index must align with `segment_ranges`.
    pub segment_block_metas: Option<Vec<Vec<SegmentBlockMeta>>>,
    /// Optional block index ranges per segment.
    /// Outer index must align with `segment_ranges`.
    pub segment_block_ranges: Option<Vec<Vec<(usize, usize)>>>,
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

fn candidate_ranges_for_scan(
    pattern: &GuidePattern,
    strands_len: usize,
    config: &ScanConfig,
) -> Vec<(usize, usize)> {
    let segment_ranges = segment_ranges_for_scan(pattern, strands_len, config);
    let Some(block_metas_per_segment) = config.segment_block_metas.as_ref() else {
        return segment_ranges;
    };
    let Some(block_ranges_per_segment) = config.segment_block_ranges.as_ref() else {
        return segment_ranges;
    };
    let mut out = Vec::new();
    for (seg_idx, seg_range) in segment_ranges.into_iter().enumerate() {
        let Some(block_metas) = block_metas_per_segment.get(seg_idx) else {
            out.push(seg_range);
            continue;
        };
        let Some(block_ranges) = block_ranges_per_segment.get(seg_idx) else {
            out.push(seg_range);
            continue;
        };
        if block_metas.is_empty() || block_ranges.is_empty() {
            out.push(seg_range);
            continue;
        }
        let seg_start = seg_range.0.min(strands_len);
        let seg_end = seg_range.1.min(strands_len);
        for (block_idx, block_range) in block_ranges.iter().enumerate() {
            let Some(block_meta) = block_metas.get(block_idx) else {
                continue;
            };
            if should_skip_block(block_meta, pattern) {
                continue;
            }
            // Block ranges are segment-relative.
            let start = (seg_start + block_range.0).min(seg_end).min(strands_len);
            let end = (seg_start + block_range.1).min(seg_end).min(strands_len);
            if start < end {
                out.push((start, end));
            }
        }
    }
    out
}

fn split_ranges_for_parallel(
    ranges: &[(usize, usize)],
    chunk_size: usize,
    strands_len: usize,
) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let chunk = chunk_size.max(1);
    for (start, end) in ranges {
        let mut s = (*start).min(strands_len);
        let e = (*end).min(strands_len);
        while s < e {
            let next = (s + chunk).min(e);
            out.push((s, next));
            s = next;
        }
    }
    out
}

fn adaptive_parallel_chunk_size(total_candidates: usize, worker_threads: usize) -> usize {
    let threads = worker_threads.max(1);
    let target_tasks = threads * PARALLEL_TASKS_PER_THREAD_TARGET.max(1);
    let baseline = (total_candidates / target_tasks).max(1);
    baseline.clamp(PARALLEL_SCAN_CHUNK_MIN, PARALLEL_SCAN_CHUNK_MAX)
}

/// Sequential scan (single thread).
pub fn scan_strands_sequential(
    pattern: &GuidePattern,
    strands: &[Strand],
    config: &ScanConfig,
) -> Vec<[u8; 8]> {
    let mut out = Vec::new();
    for (start, end) in candidate_ranges_for_scan(pattern, strands.len(), config) {
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
    let ranges = candidate_ranges_for_scan(pattern, strands.len(), config);
    let total_candidates: usize = ranges
        .iter()
        .map(|(start, end)| end.saturating_sub(*start))
        .sum();
    let worker_threads = rayon::current_num_threads();
    let chunk_size = adaptive_parallel_chunk_size(total_candidates, worker_threads);
    let ranges = split_ranges_for_parallel(&ranges, chunk_size, strands.len());
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
    use super::{
        adaptive_parallel_chunk_size, scan_strands, scan_strands_parallel, scan_strands_sequential,
        split_ranges_for_parallel, ScanConfig, PARALLEL_SCAN_CHUNK_MAX, PARALLEL_SCAN_CHUNK_MIN,
    };
    use crate::encoding::encode_bytes_to_codons;
    use crate::processor::strand_from_wal_payload;
    use crate::query::guide::{Clause, GuidePattern, RangeOp};
    use crate::query::segment::{SegmentBlockMeta, SegmentMeta};

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

    #[test]
    fn block_meta_can_skip_non_matching_blocks_within_segment() {
        let mut strands: Vec<_> = (0u64..20)
            .map(|i| strand_from_wal_payload(1, i + 1, format!("user{i}@example.com").as_bytes()))
            .collect();
        strands[15] = strand_from_wal_payload(1, 16, b"target@gmail.com");
        let ranges = vec![(0, 20)];
        let metas = vec![SegmentMeta::build(1, &strands[0..20], &["_payload"], 512, 3)];
        let block_ranges = vec![vec![(0, 10), (10, 20)]];
        let block_metas = vec![vec![
            SegmentBlockMeta::build(1, &strands[0..10], &["_payload"], 512, 3),
            SegmentBlockMeta::build(2, &strands[10..20], &["_payload"], 512, 3),
        ]];
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
            segment_block_metas: Some(block_metas),
            segment_block_ranges: Some(block_ranges),
            ..ScanConfig::default()
        };
        let hits = scan_strands(&pat, &strands, &cfg);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0], strand_from_wal_payload(1, 16, b"target@gmail.com").signature);
    }

    #[test]
    fn parallel_range_split_chunks_large_ranges() {
        let ranges = vec![(0, 10_000), (20_000, 25_000)];
        let split = split_ranges_for_parallel(&ranges, 4096, 30_000);
        assert_eq!(
            split,
            vec![
                (0, 4096),
                (4096, 8192),
                (8192, 10_000),
                (20_000, 24_096),
                (24_096, 25_000),
            ]
        );
    }

    #[test]
    fn adaptive_chunk_size_is_bounded_for_standard_hardware() {
        let small = adaptive_parallel_chunk_size(1_000, 4);
        let medium = adaptive_parallel_chunk_size(100_000, 8);
        let large = adaptive_parallel_chunk_size(10_000_000, 16);
        assert!(small >= PARALLEL_SCAN_CHUNK_MIN);
        assert!(small <= PARALLEL_SCAN_CHUNK_MAX);
        assert!(medium >= PARALLEL_SCAN_CHUNK_MIN);
        assert!(medium <= PARALLEL_SCAN_CHUNK_MAX);
        assert!(large >= PARALLEL_SCAN_CHUNK_MIN);
        assert!(large <= PARALLEL_SCAN_CHUNK_MAX);
        assert!(large >= medium);
    }
}
