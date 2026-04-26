//! Intron hash fast path: cheap prefilter before full strand decode (CRISPR layer).
//!
//! **Hash convention (Stage 2):** for equality, `Intron.value_hash` must equal
//! [`crate::processor::fnv1a64`] of the same byte sequence used as
//! [`Clause::ExactMatch::operand_wire`](super::guide::Clause::ExactMatch). Writers and the
//! compiler must agree on those bytes (today WAL `_payload` uses raw record bytes, not
//! `bincode` literals—use matching wire when querying `_payload`).

use crate::model::Intron;
use crate::processor::fnv1a64;

use super::guide::{Clause, GuidePattern};

/// `true` if some intron indexes `field` and its `value_hash` equals `fnv1a64(wire_for_hash)`.
#[inline]
pub fn intron_hash_matches_field(field: &str, wire_for_hash: &[u8], introns: &[Intron]) -> bool {
    let h = fnv1a64(wire_for_hash);
    introns
        .iter()
        .any(|i| i.field_name == field && i.value_hash == h)
}

/// Fast-path filter for a single clause against a strand's intron list.
///
/// - [`Clause::ExactMatch`]: requires a matching field **and** hash hit on `operand_wire`.
/// - [`Clause::RangeMatch`] / [`Clause::LikePattern`]: requires an intron on that field (hash
///   is not used for ordering / pattern in this v1 fast path).
pub fn clause_introns_fast_match(clause: &Clause, introns: &[Intron]) -> bool {
    match clause {
        Clause::ExactMatch {
            field_intron,
            operand_wire,
            ..
        } => intron_hash_matches_field(field_intron, operand_wire, introns),
        Clause::RangeMatch { field_intron, .. } => introns.iter().any(|i| i.field_name == *field_intron),
        Clause::LikePattern { field_intron, .. } => introns.iter().any(|i| i.field_name == *field_intron),
    }
}

/// `true` when **every** clause passes [`clause_introns_fast_match`] (logical AND across clauses).
///
/// An empty clause list is treated as vacuously true (matches all strands at this stage).
pub fn guide_introns_fast_match(pattern: &GuidePattern, introns: &[Intron]) -> bool {
    pattern
        .clauses
        .iter()
        .all(|c| clause_introns_fast_match(c, introns))
}

#[cfg(test)]
mod tests {
    use super::{clause_introns_fast_match, guide_introns_fast_match, intron_hash_matches_field};
    use crate::encoding::encode_bytes_to_codons;
    use crate::processor::strand_from_wal_payload;
    use crate::query::guide::{Clause, GuidePattern, RangeOp};

    #[test]
    fn exact_payload_matches_wal_strand_intron() {
        let strand = strand_from_wal_payload(1, 1, b"events");
        let introns = &strand.introns;
        assert!(intron_hash_matches_field("_payload", b"events", introns));

        let clause = Clause::ExactMatch {
            field_intron: "_payload".into(),
            operand_wire: b"events".to_vec(),
            operand_codons: encode_bytes_to_codons(b"events"),
        };
        assert!(clause_introns_fast_match(&clause, introns));
    }

    #[test]
    fn exact_payload_rejects_wrong_wire() {
        let strand = strand_from_wal_payload(1, 1, b"events");
        assert!(!intron_hash_matches_field("_payload", b"other", &strand.introns));
    }

    #[test]
    fn range_requires_field_intron_present() {
        let strand = strand_from_wal_payload(1, 1, b"x");
        let clause = Clause::RangeMatch {
            field_intron: "_payload".into(),
            operator: RangeOp::GreaterThan,
            operand_wire: vec![0],
            operand_codons: encode_bytes_to_codons(&[0]),
        };
        assert!(clause_introns_fast_match(&clause, &strand.introns));

        let clause_missing = Clause::RangeMatch {
            field_intron: "age".into(),
            operator: RangeOp::GreaterThan,
            operand_wire: vec![1],
            operand_codons: encode_bytes_to_codons(&[1]),
        };
        assert!(!clause_introns_fast_match(&clause_missing, &strand.introns));
    }

    #[test]
    fn guide_and_semantics_empty_clauses() {
        let strand = strand_from_wal_payload(1, 1, b"a");
        let g = GuidePattern {
            collection: "c".into(),
            clauses: vec![],
            includes: vec![],
            overlay: None,
            order_by: None,
            limit: None,
        };
        assert!(guide_introns_fast_match(&g, &strand.introns));

        let g2 = GuidePattern {
            collection: "c".into(),
            clauses: vec![
                Clause::ExactMatch {
                    field_intron: "_payload".into(),
                    operand_wire: b"a".to_vec(),
                    operand_codons: encode_bytes_to_codons(b"a"),
                },
                Clause::RangeMatch {
                    field_intron: "_payload".into(),
                    operator: RangeOp::GreaterOrEqual,
                    operand_wire: vec![0],
                    operand_codons: encode_bytes_to_codons(&[0]),
                },
            ],
            includes: vec![],
            overlay: None,
            order_by: None,
            limit: None,
        };
        assert!(guide_introns_fast_match(&g2, &strand.introns));
    }
}
