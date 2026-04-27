//! Query planner path selection (direct / index / guided scan), with a simple cost model.

use std::collections::{HashMap, HashSet};

use super::ast::QueryLiteral;
use super::guide::{Clause, GuidePattern, RangeOp};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryPath {
    Direct,
    Index { field: String },
    GuidedScan,
}

#[derive(Debug, Clone)]
pub struct CollectionStats {
    pub record_count: usize,
    pub indexed_fields: HashSet<String>,
    pub direct_lookup_fields: HashSet<String>,
    /// Fraction of records expected to be skipped by segment-level filters in scan mode.
    pub estimated_bloom_skip_rate: f64,
    /// Field -> expected match fraction for equality lookups (0..1). Lower means more selective.
    pub index_selectivity: HashMap<String, f64>,
}

impl CollectionStats {
    pub fn fake() -> Self {
        Self {
            record_count: 100_000,
            indexed_fields: HashSet::new(),
            direct_lookup_fields: ["_signature".to_string()].into_iter().collect(),
            estimated_bloom_skip_rate: 0.0,
            index_selectivity: HashMap::new(),
        }
    }

    pub fn add_index(&mut self, field: impl Into<String>) {
        let f = field.into();
        self.indexed_fields.insert(f.clone());
        self.index_selectivity.entry(f).or_insert(0.01);
    }
}

const INDEX_LOOKUP_COST: f64 = 1.0;
const RECORD_LOAD_COST: f64 = 2.0;
const SCAN_COST_PER_RECORD: f64 = 0.1;
const INCLUDE_EDGE_COST: f64 = 25.0;
const ORDER_BY_COST_PER_RECORD: f64 = 0.02;
const VERIFY_CLAUSE_COST: f64 = 0.03;

fn estimate_index_cost(field: &str, stats: &CollectionStats) -> f64 {
    let sel = stats.index_selectivity.get(field).copied().unwrap_or(0.05).clamp(0.0, 1.0);
    let expected_rows = stats.record_count as f64 * sel;
    INDEX_LOOKUP_COST + expected_rows * RECORD_LOAD_COST
}

fn parse_range_operand_u64(wire: &[u8]) -> Option<u64> {
    if let Ok(lit) = bincode::deserialize::<QueryLiteral>(wire) {
        return match lit {
            QueryLiteral::U64(v) => Some(v),
            QueryLiteral::I64(v) if v >= 0 => Some(v as u64),
            _ => None,
        };
    }
    std::str::from_utf8(wire).ok()?.parse().ok()
}

/// Two range clauses on the same field: lower (GT/GTE) + upper (LT/LTE), inclusive span non-empty.
fn indexed_between_field(pattern: &GuidePattern) -> Option<String> {
    if pattern.clauses.len() != 2 {
        return None;
    }
    let mut lower = None::<(bool, u64)>;
    let mut upper = None::<(bool, u64)>;
    let mut field: Option<String> = None;
    for c in &pattern.clauses {
        let Clause::RangeMatch {
            field_intron,
            operator,
            operand_wire,
            ..
        } = c
        else {
            return None;
        };
        match field.as_ref() {
            None => field = Some(field_intron.clone()),
            Some(f) if f == field_intron => {}
            Some(_) => return None,
        }
        let n = parse_range_operand_u64(operand_wire)?;
        match operator {
            RangeOp::GreaterThan => lower = Some((true, n)),
            RangeOp::GreaterOrEqual => lower = Some((false, n)),
            RangeOp::LessThan => upper = Some((true, n)),
            RangeOp::LessOrEqual => upper = Some((false, n)),
            RangeOp::NotEqual => return None,
        }
    }
    let (lo_strict, lo) = lower?;
    let (hi_strict, hi) = upper?;
    let lo_inc = if lo_strict { lo.saturating_add(1) } else { lo };
    let hi_inc = if hi_strict { hi.saturating_sub(1) } else { hi };
    if lo_inc <= hi_inc {
        field
    } else {
        None
    }
}

fn estimate_scan_cost(pattern: &GuidePattern, stats: &CollectionStats) -> f64 {
    let skip = stats.estimated_bloom_skip_rate.clamp(0.0, 1.0);
    let remaining = stats.record_count as f64 * (1.0 - skip);
    let verify = remaining * (pattern.clauses.len() as f64 * VERIFY_CLAUSE_COST);
    let include = pattern.includes.len() as f64 * INCLUDE_EDGE_COST;
    let order = if pattern.order_by.is_some() {
        remaining * ORDER_BY_COST_PER_RECORD
    } else {
        0.0
    };
    remaining * SCAN_COST_PER_RECORD + verify + include + order
}

pub fn choose_path(pattern: &GuidePattern, stats: &CollectionStats) -> QueryPath {
    if let Some(field_intron) = pattern.clauses.iter().find_map(|c| match c {
        Clause::ExactMatch { field_intron, .. } => Some(field_intron),
        _ => None,
    }) {
        if pattern.clauses.len() == 1
            && pattern.includes.is_empty()
            && pattern.order_by.is_none()
            && (pattern.limit.is_none() || pattern.limit == Some(1))
            && stats.direct_lookup_fields.contains(field_intron)
        {
            return QueryPath::Direct;
        }
    }

    if pattern.includes.is_empty() && pattern.order_by.is_none() {
        if let Some(field) = indexed_between_field(pattern) {
            if stats.indexed_fields.contains(&field) {
                return QueryPath::Index { field };
            }
        }
    }

    let mut indexed_candidates: Vec<String> = pattern
        .clauses
        .iter()
        .filter_map(|c| match c {
            Clause::ExactMatch { field_intron, .. } if stats.indexed_fields.contains(field_intron) => {
                Some(field_intron.clone())
            }
            Clause::RangeMatch { field_intron, .. } if stats.indexed_fields.contains(field_intron) => {
                Some(field_intron.clone())
            }
            _ => None,
        })
        .collect();
    indexed_candidates.sort();
    indexed_candidates.dedup();

    // Heuristic: point lookups with exact predicate + limit 1 should strongly
    // prefer index path when available.
    if pattern.clauses.len() == 1
        && pattern.order_by.is_none()
        && pattern.includes.is_empty()
        && (pattern.limit.is_none() || pattern.limit == Some(1))
    {
        if let Some(Clause::ExactMatch { field_intron, .. }) = pattern.clauses.first() {
            if stats.indexed_fields.contains(field_intron) {
                let sel = stats
                    .index_selectivity
                    .get(field_intron)
                    .copied()
                    .unwrap_or(0.05);
                if sel <= 0.05 {
                    return QueryPath::Index {
                        field: field_intron.clone(),
                    };
                }
            }
        }
    }

    if !indexed_candidates.is_empty() {
        let scan_cost = estimate_scan_cost(pattern, stats);
        let best = indexed_candidates
            .iter()
            .map(|f| (f, estimate_index_cost(f, stats)))
            .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        if let Some((field, idx_cost)) = best {
            if idx_cost < scan_cost {
                return QueryPath::Index {
                    field: field.clone(),
                };
            }
        }
    }

    QueryPath::GuidedScan
}

#[cfg(test)]
mod tests {
    use crate::encoding::encode_bytes_to_codons;
    use crate::query::guide::{Clause, GuidePattern, RangeOp};

    use super::{choose_path, CollectionStats, QueryPath};

    #[test]
    fn planner_chooses_direct_for_signature_lookup() {
        let p = GuidePattern {
            collection: "c".into(),
            clauses: vec![Clause::ExactMatch {
                field_intron: "_signature".into(),
                operand_wire: 42u64.to_le_bytes().to_vec(),
                operand_codons: encode_bytes_to_codons(&42u64.to_le_bytes()),
            }],
            includes: vec![],
            overlay: None,
            order_by: None,
            limit: Some(1),
        };
        let stats = CollectionStats::fake();
        assert!(matches!(choose_path(&p, &stats), QueryPath::Direct));
    }

    #[test]
    fn planner_chooses_index_when_available() {
        let p = GuidePattern {
            collection: "c".into(),
            clauses: vec![Clause::ExactMatch {
                field_intron: "email".into(),
                operand_wire: b"alice@x.com".to_vec(),
                operand_codons: encode_bytes_to_codons(b"alice@x.com"),
            }],
            includes: vec![],
            overlay: None,
            order_by: None,
            limit: None,
        };
        let mut stats = CollectionStats::fake();
        stats.add_index("email");
        assert!(matches!(
            choose_path(&p, &stats),
            QueryPath::Index { field } if field == "email"
        ));
    }

    #[test]
    fn planner_falls_back_to_guided_scan_without_index() {
        let p = GuidePattern {
            collection: "c".into(),
            clauses: vec![Clause::ExactMatch {
                field_intron: "unlisted_field".into(),
                operand_wire: b"value".to_vec(),
                operand_codons: encode_bytes_to_codons(b"value"),
            }],
            includes: vec![],
            overlay: None,
            order_by: None,
            limit: None,
        };
        let stats = CollectionStats::fake();
        assert!(matches!(choose_path(&p, &stats), QueryPath::GuidedScan));
    }

    #[test]
    fn planner_prefers_guided_scan_when_index_is_non_selective() {
        let p = GuidePattern {
            collection: "c".into(),
            clauses: vec![Clause::ExactMatch {
                field_intron: "email".into(),
                operand_wire: b"alice@x.com".to_vec(),
                operand_codons: encode_bytes_to_codons(b"alice@x.com"),
            }],
            includes: vec![],
            overlay: None,
            order_by: None,
            limit: None,
        };
        let mut stats = CollectionStats::fake();
        stats.record_count = 1_000_000;
        stats.estimated_bloom_skip_rate = 0.95;
        stats.add_index("email");
        stats.index_selectivity.insert("email".into(), 0.9);
        assert!(matches!(choose_path(&p, &stats), QueryPath::GuidedScan));
    }

    #[test]
    fn planner_picks_most_selective_index_from_multiple_candidates() {
        let p = GuidePattern {
            collection: "c".into(),
            clauses: vec![
                Clause::ExactMatch {
                    field_intron: "status".into(),
                    operand_wire: b"active".to_vec(),
                    operand_codons: encode_bytes_to_codons(b"active"),
                },
                Clause::ExactMatch {
                    field_intron: "email".into(),
                    operand_wire: b"alice@x.com".to_vec(),
                    operand_codons: encode_bytes_to_codons(b"alice@x.com"),
                },
            ],
            includes: vec![],
            overlay: None,
            order_by: None,
            limit: None,
        };
        let mut stats = CollectionStats::fake();
        stats.record_count = 1_000_000;
        stats.add_index("status");
        stats.add_index("email");
        stats.index_selectivity.insert("status".into(), 0.5);
        stats.index_selectivity.insert("email".into(), 0.001);
        assert!(matches!(
            choose_path(&p, &stats),
            QueryPath::Index { field } if field == "email"
        ));
    }

    #[test]
    fn planner_direct_lookup_works_when_signature_clause_not_first() {
        let p = GuidePattern {
            collection: "c".into(),
            clauses: vec![
                Clause::ExactMatch {
                    field_intron: "_signature".into(),
                    operand_wire: 7u64.to_le_bytes().to_vec(),
                    operand_codons: encode_bytes_to_codons(&7u64.to_le_bytes()),
                },
            ],
            includes: vec![],
            overlay: None,
            order_by: None,
            limit: Some(1),
        };
        let stats = CollectionStats::fake();
        assert!(matches!(choose_path(&p, &stats), QueryPath::Direct));
    }

    #[test]
    fn planner_prefers_index_for_between_on_same_field() {
        let p = GuidePattern {
            collection: "c".into(),
            clauses: vec![
                Clause::RangeMatch {
                    field_intron: "score".into(),
                    operator: RangeOp::GreaterOrEqual,
                    operand_wire: b"10".to_vec(),
                    operand_codons: encode_bytes_to_codons(b"10"),
                },
                Clause::RangeMatch {
                    field_intron: "score".into(),
                    operator: RangeOp::LessOrEqual,
                    operand_wire: b"20".to_vec(),
                    operand_codons: encode_bytes_to_codons(b"20"),
                },
            ],
            includes: vec![],
            overlay: None,
            order_by: None,
            limit: None,
        };
        let mut stats = CollectionStats::fake();
        stats.record_count = 3;
        stats.add_index("score");
        assert!(matches!(
            choose_path(&p, &stats),
            QueryPath::Index { field } if field == "score"
        ));
    }

    #[test]
    fn planner_prefers_index_for_point_lookup_with_limit_one() {
        let p = GuidePattern {
            collection: "c".into(),
            clauses: vec![Clause::ExactMatch {
                field_intron: "email".into(),
                operand_wire: b"alice@x.com".to_vec(),
                operand_codons: encode_bytes_to_codons(b"alice@x.com"),
            }],
            includes: vec![],
            overlay: None,
            order_by: None,
            limit: Some(1),
        };
        let mut stats = CollectionStats::fake();
        stats.add_index("email");
        assert!(matches!(
            choose_path(&p, &stats),
            QueryPath::Index { field } if field == "email"
        ));
    }
}

