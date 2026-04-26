//! Layer 8 vector execution primitives: batch predicate evaluation over candidate rows.

use crate::model::Strand;

use super::guide::GuidePattern;

/// Default vector batch size for predicate evaluation.
pub const DEFAULT_VECTOR_BATCH_SIZE: usize = 1024;

pub type ClauseMatcher = fn(&Strand, &super::guide::Clause) -> bool;

/// Evaluate pattern predicates in fixed-size batches and return rows that pass all clauses.
pub fn filter_rows_vectorized<'a>(
    rows: Vec<&'a Strand>,
    pattern: &GuidePattern,
    batch_size: usize,
    matcher: ClauseMatcher,
) -> Vec<&'a Strand> {
    if pattern.clauses.is_empty() {
        return rows;
    }
    let mut out = Vec::new();
    for chunk in rows.chunks(batch_size.max(1)) {
        let mut mask = vec![true; chunk.len()];
        for clause in &pattern.clauses {
            for (idx, row) in chunk.iter().enumerate() {
                if mask[idx] && !matcher(row, clause) {
                    mask[idx] = false;
                }
            }
        }
        for (idx, row) in chunk.iter().enumerate() {
            if mask[idx] {
                out.push(*row);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use crate::encoding::encode_bytes_to_codons;
    use crate::processor::strand_from_wal_payload;
    use crate::query::guide::{Clause, GuidePattern};

    use super::filter_rows_vectorized;

    fn match_payload(s: &crate::model::Strand, c: &Clause) -> bool {
        match c {
            Clause::ExactMatch {
                field_intron,
                operand_wire,
                ..
            } => {
                if field_intron != "_payload" {
                    return false;
                }
                s.introns.iter().any(|i| {
                    i.field_name == "_payload"
                        && crate::processor::fnv1a64(operand_wire) == i.value_hash
                })
            }
            _ => false,
        }
    }

    #[test]
    fn vector_filter_matches_expected_rows() {
        let rows: Vec<_> = (0..20u64)
            .map(|i| strand_from_wal_payload(1, i + 1, format!("u{i}@x.com").as_bytes()))
            .collect();
        let refs: Vec<&crate::model::Strand> = rows.iter().collect();
        let needle = b"u7@x.com";
        let pattern = GuidePattern {
            collection: "c".into(),
            clauses: vec![Clause::ExactMatch {
                field_intron: "_payload".into(),
                operand_wire: needle.to_vec(),
                operand_codons: encode_bytes_to_codons(needle),
            }],
            includes: vec![],
            overlay: None,
            order_by: None,
            limit: None,
        };
        let out = filter_rows_vectorized(refs, &pattern, 8, match_payload);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].signature, strand_from_wal_payload(1, 8, needle).signature);
    }
}

