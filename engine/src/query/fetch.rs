//! In-memory `fetch` / `fetchOne`: scan → order → limit, plus basic `.include()` via intron refs.

use std::collections::HashMap;

use crate::encoding::{decode_codons_to_bytes, EncodedPayload};
use crate::model::Intron;
use crate::model::Strand;

use super::ast::SortDirection;
use super::guide::{Clause, GuidePattern, RangeOp};
use super::index::{build_exact_hash_index, exact_lookup_signatures, GlobalRangeIndex};
use super::planner::{choose_path, CollectionStats, QueryPath};
use super::scan::{scan_strands, ScanConfig};
use super::vector::{filter_rows_vectorized, DEFAULT_VECTOR_BATCH_SIZE};

/// Ordered, limited root strands matching `pattern` (no includes expanded).
#[derive(Debug)]
pub struct FetchResult<'a> {
    pub rows: Vec<&'a Strand>,
}

/// One root strand plus related strands for each requested include path.
#[derive(Debug)]
pub struct FetchRow<'a> {
    pub root: &'a Strand,
    /// Pairs of `(include_path, related_strands)` in declaration order.
    pub included: Vec<(String, Vec<&'a Strand>)>,
}

#[derive(Debug)]
pub struct FetchWithIncludes<'a> {
    pub rows: Vec<FetchRow<'a>>,
}

fn strand_by_signature<'a>(strands: &'a [Strand]) -> HashMap<[u8; 8], &'a Strand> {
    strands.iter().map(|s| (s.signature, s)).collect()
}

fn hits_to_strands<'a>(
    index: &HashMap<[u8; 8], &'a Strand>,
    sigs: &[[u8; 8]],
) -> Vec<&'a Strand> {
    sigs.iter()
        .filter_map(|sig| index.get(sig).copied())
        .collect()
}

fn intron_payload_bytes(strand: &Strand, intron: &Intron) -> Option<Vec<u8>> {
    let start = intron.codon_offset as usize;
    let len = intron.codon_length as usize;
    let end = start.checked_add(len)?;
    if end > strand.codons.len() {
        return None;
    }
    // Encode path emits 4 symbols per byte and packs 3 symbols per codon.
    // Given codon count c, original length is uniquely floor(3c/4) for non-empty payloads.
    let original_len = (len * 3) / 4;
    let payload = EncodedPayload {
        codons: strand.codons[start..end].to_vec(),
        original_len,
    };
    decode_codons_to_bytes(&payload).ok()
}

fn like_matches(value: &str, pattern: &str) -> bool {
    let v: Vec<char> = value.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    let mut dp = vec![vec![false; v.len() + 1]; p.len() + 1];
    dp[0][0] = true;
    for i in 1..=p.len() {
        if p[i - 1] == '%' {
            dp[i][0] = dp[i - 1][0];
        }
    }
    for i in 1..=p.len() {
        for j in 1..=v.len() {
            dp[i][j] = match p[i - 1] {
                '%' => dp[i - 1][j] || dp[i][j - 1],
                '_' => dp[i - 1][j - 1],
                c => dp[i - 1][j - 1] && c == v[j - 1],
            };
        }
    }
    dp[p.len()][v.len()]
}

fn clause_matches_strand(strand: &Strand, clause: &Clause) -> bool {
    match clause {
        Clause::ExactMatch {
            field_intron,
            operand_wire,
            operand_codons,
        } => {
            if field_intron == "_signature" {
                return strand.signature.as_slice() == operand_wire.as_slice();
            }
            strand
                .introns
                .iter()
                .filter(|i| i.field_name == *field_intron)
                .any(|i| {
                    if field_intron == "_payload" {
                        intron_payload_bytes(strand, i).is_some_and(|v| v == *operand_wire)
                    } else {
                        let start = i.codon_offset as usize;
                        let end = start.saturating_add(i.codon_length as usize);
                        end <= strand.codons.len() && strand.codons[start..end] == operand_codons.codons
                    }
                })
        }
        Clause::RangeMatch {
            field_intron,
            operator,
            operand_wire,
            ..
        } => strand
            .introns
            .iter()
            .filter(|i| i.field_name == *field_intron)
            .any(|i| {
                let Some(value) = intron_payload_bytes(strand, i) else {
                    return false;
                };
                match operator {
                    RangeOp::GreaterThan => value > *operand_wire,
                    RangeOp::GreaterOrEqual => value >= *operand_wire,
                    RangeOp::LessThan => value < *operand_wire,
                    RangeOp::LessOrEqual => value <= *operand_wire,
                    RangeOp::NotEqual => value != *operand_wire,
                }
            }),
        Clause::LikePattern {
            field_intron,
            pattern,
        } => strand
            .introns
            .iter()
            .filter(|i| i.field_name == *field_intron)
            .any(|i| {
                intron_payload_bytes(strand, i).is_some_and(|v| {
                    let s = String::from_utf8_lossy(&v);
                    like_matches(&s, pattern)
                })
            }),
    }
}

fn verify_clauses<'a>(rows: Vec<&'a Strand>, pattern: &GuidePattern) -> Vec<&'a Strand> {
    filter_rows_vectorized(rows, pattern, DEFAULT_VECTOR_BATCH_SIZE, clause_matches_strand)
}

fn sort_key(s: &Strand, field: &str) -> u64 {
    match field {
        "version" => s.version,
        "created_at" => s.created_at,
        "updated_at" => s.updated_at,
        _ => u64::from_le_bytes(s.signature),
    }
}

fn apply_order_by(rows: &mut Vec<&Strand>, pattern: &GuidePattern) {
    let Some((field, dir)) = &pattern.order_by else {
        return;
    };
    rows.sort_by(|a, b| {
        let ord = sort_key(a, field).cmp(&sort_key(b, field));
        match dir {
            SortDirection::Asc => ord,
            SortDirection::Desc => ord.reverse(),
        }
    });
}

fn apply_limit(rows: &mut Vec<&Strand>, pattern: &GuidePattern) {
    let Some(lim) = pattern.limit else {
        return;
    };
    let n = lim as usize;
    if rows.len() > n {
        rows.truncate(n);
    }
}

fn direct_signature_lookup<'a>(
    pattern: &GuidePattern,
    index: &HashMap<[u8; 8], &'a Strand>,
) -> Option<Vec<&'a Strand>> {
    let Clause::ExactMatch {
        field_intron,
        operand_wire,
        ..
    } = pattern.clauses.first()?
    else {
        return None;
    };
    if field_intron != "_signature" || operand_wire.len() != 8 {
        return None;
    }
    let mut sig = [0u8; 8];
    sig.copy_from_slice(operand_wire);
    Some(index.get(&sig).copied().into_iter().collect())
}

fn build_collection_stats(strands: &[Strand], config: &ScanConfig, pattern: &GuidePattern) -> CollectionStats {
    let mut stats = CollectionStats::fake();
    stats.record_count = strands.len();
    if let Some(indexed) = &config.indexed_fields {
        for f in indexed {
            stats.add_index(f.clone());
            let idx = build_exact_hash_index(strands, f);
            let distinct = idx.len().max(1);
            stats
                .index_selectivity
                .insert(f.clone(), 1.0 / distinct as f64);
        }
    }
    if let (Some(metas), Some(_ranges)) = (&config.segment_metas, &config.segment_ranges) {
        if !metas.is_empty() {
            let skipped = metas
                .iter()
                .filter(|m| super::segment::should_skip_segment(m, pattern))
                .count();
            stats.estimated_bloom_skip_rate = skipped as f64 / metas.len() as f64;
        }
    }
    stats
}

fn indexed_exact_lookup<'a>(
    pattern: &GuidePattern,
    strands: &'a [Strand],
    field: &str,
    config: &ScanConfig,
    index: &HashMap<[u8; 8], &'a Strand>,
) -> Option<Vec<&'a Strand>> {
    let Clause::ExactMatch {
        field_intron,
        operand_wire,
        ..
    } = pattern.clauses.iter().find(|c| matches!(c, Clause::ExactMatch { .. }))?
    else {
        return None;
    };
    if field_intron != field {
        return None;
    }
    let sigs = if let Some(global) = config
        .global_hash_indexes
        .as_ref()
        .and_then(|m| m.get(field))
    {
        global.lookup(operand_wire)
    } else {
        let hash_idx = build_exact_hash_index(strands, field);
        exact_lookup_signatures(&hash_idx, operand_wire)
    };
    Some(
        sigs.iter()
            .filter_map(|sig| index.get(sig).copied())
            .collect(),
    )
}

fn parse_range_operand_u64(wire: &[u8]) -> Option<u64> {
    if let Ok(lit) = bincode::deserialize::<super::ast::QueryLiteral>(wire) {
        return match lit {
            super::ast::QueryLiteral::U64(v) => Some(v),
            super::ast::QueryLiteral::I64(v) if v >= 0 => Some(v as u64),
            _ => None,
        };
    }
    if let Ok(s) = std::str::from_utf8(wire) {
        return s.parse::<u64>().ok();
    }
    None
}

fn indexed_range_lookup<'a>(
    pattern: &GuidePattern,
    strands: &'a [Strand],
    field: &str,
    config: &ScanConfig,
    index: &HashMap<[u8; 8], &'a Strand>,
) -> Option<Vec<&'a Strand>> {
    let Clause::RangeMatch {
        field_intron,
        operator,
        operand_wire,
        ..
    } = pattern.clauses.iter().find(|c| matches!(c, Clause::RangeMatch { .. }))?
    else {
        return None;
    };
    if field_intron != field {
        return None;
    }
    let needle = parse_range_operand_u64(operand_wire)?;
    let sigs = if let Some(global) = config
        .global_range_indexes
        .as_ref()
        .and_then(|m| m.get(field))
    {
        global.lookup(*operator, needle)
    } else {
        let idx = GlobalRangeIndex::build(field.to_string(), strands);
        idx.lookup(*operator, needle)
    };
    Some(
        sigs.iter()
            .filter_map(|sig| index.get(sig).copied())
            .collect(),
    )
}

/// Full `fetch`: CRISPR scan, then `orderBy` / `limit` from [`GuidePattern`].
pub fn fetch<'a>(
    pattern: &GuidePattern,
    strands: &'a [Strand],
    config: &ScanConfig,
) -> FetchResult<'a> {
    let index = strand_by_signature(strands);
    let stats = build_collection_stats(strands, config, pattern);
    let path = choose_path(pattern, &stats);
    let base_rows = match path {
        QueryPath::Direct => direct_signature_lookup(pattern, &index)
            .unwrap_or_else(|| hits_to_strands(&index, &scan_strands(pattern, strands, config))),
        QueryPath::Index { field } => {
            let rows = indexed_exact_lookup(pattern, strands, &field, config, &index)
                .or_else(|| indexed_range_lookup(pattern, strands, &field, config, &index));
            rows.unwrap_or_else(|| hits_to_strands(&index, &scan_strands(pattern, strands, config)))
        }
        QueryPath::GuidedScan => {
            let sigs = scan_strands(pattern, strands, config);
            hits_to_strands(&index, &sigs)
        }
    };
    let mut rows = verify_clauses(base_rows, pattern);
    apply_order_by(&mut rows, pattern);
    apply_limit(&mut rows, pattern);
    FetchResult { rows }
}

/// `fetchOne`: same pipeline as [`fetch`], then return the first row (if any).
pub fn fetch_one<'a>(
    pattern: &GuidePattern,
    strands: &'a [Strand],
    config: &ScanConfig,
) -> Option<&'a Strand> {
    fetch(pattern, strands, config).rows.into_iter().next()
}

/// Follow `pattern.includes` paths using [`Intron::references_strand`](crate::model::Intron::references_strand)
/// (dot-separated multi-hop, e.g. `orders.products`).
pub fn resolve_include_path<'a>(
    root: &'a Strand,
    path: &str,
    index: &HashMap<[u8; 8], &'a Strand>,
) -> Vec<&'a Strand> {
    let segments: Vec<&str> = path.split('.').filter(|s| !s.is_empty()).collect();
    if segments.is_empty() {
        return Vec::new();
    }

    let mut frontier = vec![root];
    for seg in segments {
        let mut next = Vec::new();
        for node in frontier {
            for intron in &node.introns {
                if intron.field_name == seg {
                    if let Some(sig) = intron.references_strand {
                        if let Some(t) = index.get(&sig) {
                            next.push(*t);
                        }
                    }
                }
            }
        }
        frontier = next;
        if frontier.is_empty() {
            return Vec::new();
        }
    }
    frontier
}

/// [`fetch`] plus include expansion for every root row.
pub fn fetch_with_includes<'a>(
    pattern: &GuidePattern,
    strands: &'a [Strand],
    config: &ScanConfig,
) -> FetchWithIncludes<'a> {
    let base = fetch(pattern, strands, config);
    let index = strand_by_signature(strands);
    let rows = base
        .rows
        .into_iter()
        .map(|root| {
            let included = pattern
                .includes
                .iter()
                .map(|path| {
                    let related = resolve_include_path(root, path, &index);
                    (path.clone(), related)
                })
                .collect();
            FetchRow { root, included }
        })
        .collect();
    FetchWithIncludes { rows }
}

#[cfg(test)]
mod tests {
    use super::{fetch, fetch_one, fetch_with_includes, resolve_include_path};
    use crate::encoding::encode_bytes_to_codons;
    use crate::model::{Intron, RefreshPolicy, Strand, Telomere};
    use crate::processor::{fnv1a64, strand_from_wal_payload};
    use crate::query::ast::SortDirection;
    use crate::query::guide::{Clause, GuidePattern, RangeOp};
    use crate::query::planner::{choose_path, CollectionStats, QueryPath};
    use crate::query::ScanConfig;

    fn strand_with_payload(collection_id: u32, seq: u64, payload: &[u8], created: u64) -> Strand {
        let mut s = strand_from_wal_payload(collection_id, seq, payload);
        s.created_at = created;
        s
    }

    fn pattern_exact_payload(wire: &[u8]) -> GuidePattern {
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
    fn order_by_created_at_and_limit() {
        let strands = vec![
            strand_with_payload(1, 1, b"a", 100),
            strand_with_payload(1, 2, b"b", 300),
            strand_with_payload(1, 3, b"a", 200),
        ];
        let mut pat = pattern_exact_payload(b"a");
        pat.order_by = Some(("created_at".into(), SortDirection::Desc));
        pat.limit = Some(1);

        let out = fetch(&pat, &strands, &ScanConfig::default());
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.rows[0].version, 3);

        let one = fetch_one(&pat, &strands, &ScanConfig::default()).expect("one");
        assert_eq!(one.version, 3);
    }

    #[test]
    fn include_multi_hop() {
        let order_sig = 10u64.to_le_bytes();
        let product_sig = 20u64.to_le_bytes();

        let product = Strand {
            signature: product_sig,
            collection_id: 1,
            codons: vec![],
            complement: vec![],
            introns: vec![],
            telomere: Telomere {
                count: 0,
                immortal: true,
                last_refresh: 0,
                refresh_policy: RefreshPolicy::Immortal,
            },
            epigenetic_tags: vec![],
            version: 20,
            created_at: 0,
            updated_at: 0,
        };

        let order = Strand {
            signature: order_sig,
            collection_id: 1,
            codons: vec![],
            complement: vec![],
            introns: vec![Intron {
                field_name: "products".into(),
                codon_offset: 0,
                codon_length: 0,
                value_hash: 0,
                references_strand: Some(product_sig),
            }],
            telomere: Telomere {
                count: 0,
                immortal: true,
                last_refresh: 0,
                refresh_policy: RefreshPolicy::Immortal,
            },
            epigenetic_tags: vec![],
            version: 10,
            created_at: 0,
            updated_at: 0,
        };

        let user_sig = 1u64.to_le_bytes();
        let wire = b"user1";
        let mut user = strand_from_wal_payload(1, 1, wire);
        user.signature = user_sig;
        user.introns.push(Intron {
            field_name: "orders".into(),
            codon_offset: 0,
            codon_length: 0,
            value_hash: fnv1a64(wire),
            references_strand: Some(order_sig),
        });

        let strands = vec![user, order, product];
        let index = super::strand_by_signature(&strands);

        let chain = resolve_include_path(&strands[0], "orders.products", &index);
        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0].signature, product_sig);

        let mut pat = pattern_exact_payload(wire);
        pat.includes = vec!["orders.products".into()];

        let with = fetch_with_includes(&pat, &strands, &ScanConfig::default());
        assert_eq!(with.rows.len(), 1);
        let inc = &with.rows[0].included[0];
        assert_eq!(inc.0, "orders.products");
        assert_eq!(inc.1.len(), 1);
        assert_eq!(inc.1[0].signature, product_sig);
    }

    #[test]
    fn like_pattern_is_verified_after_fast_match() {
        let strands = vec![
            strand_with_payload(1, 1, b"user1@gmail.com", 100),
            strand_with_payload(1, 2, b"user2@example.com", 200),
            strand_with_payload(1, 3, b"user3@gmail.com", 300),
        ];
        let pattern = GuidePattern {
            collection: "c".into(),
            clauses: vec![Clause::LikePattern {
                field_intron: "_payload".into(),
                pattern: "%@gmail.com".into(),
            }],
            includes: vec![],
            overlay: None,
            order_by: None,
            limit: None,
        };
        let out = fetch(&pattern, &strands, &ScanConfig::default());
        assert_eq!(out.rows.len(), 2);
        assert_eq!(out.rows[0].version, 1);
        assert_eq!(out.rows[1].version, 3);
    }

    #[test]
    fn range_not_equal_is_verified_after_fast_match() {
        let strands = vec![
            strand_with_payload(1, 1, b"alpha", 100),
            strand_with_payload(1, 2, b"beta", 200),
        ];
        let pattern = GuidePattern {
            collection: "c".into(),
            clauses: vec![Clause::RangeMatch {
                field_intron: "_payload".into(),
                operator: RangeOp::NotEqual,
                operand_wire: b"beta".to_vec(),
                operand_codons: encode_bytes_to_codons(b"beta"),
            }],
            includes: vec![],
            overlay: None,
            order_by: None,
            limit: None,
        };
        let out = fetch(&pattern, &strands, &ScanConfig::default());
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.rows[0].version, 1);
    }

    #[test]
    fn direct_signature_path_returns_expected_row() {
        let strands = vec![
            strand_with_payload(1, 1, b"a", 100),
            strand_with_payload(1, 2, b"b", 200),
        ];
        let p = GuidePattern {
            collection: "c".into(),
            clauses: vec![Clause::ExactMatch {
                field_intron: "_signature".into(),
                operand_wire: strands[1].signature.to_vec(),
                operand_codons: encode_bytes_to_codons(&strands[1].signature),
            }],
            includes: vec![],
            overlay: None,
            order_by: None,
            limit: Some(1),
        };
        let stats = CollectionStats::fake();
        assert!(matches!(choose_path(&p, &stats), QueryPath::Direct));
        let out = fetch(&p, &strands, &ScanConfig::default());
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.rows[0].signature, strands[1].signature);
    }

    #[test]
    fn indexed_exact_path_returns_expected_rows() {
        let strands = vec![
            strand_with_payload(1, 1, b"a@example.com", 100),
            strand_with_payload(1, 2, b"b@example.com", 200),
            strand_with_payload(1, 3, b"a@example.com", 300),
        ];
        let p = pattern_exact_payload(b"a@example.com");
        let cfg = ScanConfig {
            collection_id: Some(1),
            indexed_fields: Some(["_payload".to_string()].into_iter().collect()),
            ..ScanConfig::default()
        };
        let out = fetch(&p, &strands, &cfg);
        assert_eq!(out.rows.len(), 2);
        assert!(out.rows.iter().any(|s| s.version == 1));
        assert!(out.rows.iter().any(|s| s.version == 3));
    }

    #[test]
    fn indexed_range_path_returns_expected_rows() {
        let strands = vec![
            strand_with_payload(1, 1, b"10", 100),
            strand_with_payload(1, 2, b"20", 200),
            strand_with_payload(1, 3, b"30", 300),
        ];
        let p = GuidePattern {
            collection: "c".into(),
            clauses: vec![Clause::RangeMatch {
                field_intron: "_payload".into(),
                operator: RangeOp::GreaterThan,
                operand_wire: b"15".to_vec(),
                operand_codons: encode_bytes_to_codons(b"15"),
            }],
            includes: vec![],
            overlay: None,
            order_by: None,
            limit: None,
        };
        let cfg = ScanConfig {
            collection_id: Some(1),
            indexed_fields: Some(["_payload".to_string()].into_iter().collect()),
            ..ScanConfig::default()
        };
        let out = fetch(&p, &strands, &cfg);
        assert_eq!(out.rows.len(), 2);
        assert!(out.rows.iter().any(|s| s.version == 2));
        assert!(out.rows.iter().any(|s| s.version == 3));
    }
}
