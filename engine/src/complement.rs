use crate::model::Codon;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Corruption {
    pub codon_index: usize,
    pub symbol_index: usize,
    pub strand_value: u8,
    pub complement_value: u8,
}

#[inline]
fn complement_symbol(value: u8) -> u8 {
    3 - value
}

pub fn generate_complement(codons: &[Codon]) -> Vec<Codon> {
    codons
        .iter()
        .map(|c| {
            let [a, b, c2] = c.values();
            Codon::new(
                complement_symbol(a),
                complement_symbol(b),
                complement_symbol(c2),
            )
            .expect("complement values are always in base-4 range")
        })
        .collect()
}

pub fn find_corruption(strand: &[Codon], complement: &[Codon]) -> Vec<Corruption> {
    let mut issues = Vec::new();
    let pair_count = strand.len().min(complement.len());

    for idx in 0..pair_count {
        let s_vals = strand[idx].values();
        let c_vals = complement[idx].values();
        for symbol_idx in 0..3 {
            if s_vals[symbol_idx] + c_vals[symbol_idx] != 3 {
                issues.push(Corruption {
                    codon_index: idx,
                    symbol_index: symbol_idx,
                    strand_value: s_vals[symbol_idx],
                    complement_value: c_vals[symbol_idx],
                });
            }
        }
    }

    issues
}

pub fn is_valid_pairing(strand: &[Codon], complement: &[Codon]) -> bool {
    strand.len() == complement.len() && find_corruption(strand, complement).is_empty()
}

#[cfg(test)]
mod tests {
    use super::{find_corruption, generate_complement, is_valid_pairing};
    use crate::model::Codon;

    #[test]
    fn generated_complement_is_valid() {
        let strand = vec![
            Codon::new(0, 1, 2).unwrap(),
            Codon::new(3, 0, 1).unwrap(),
            Codon::new(2, 2, 3).unwrap(),
        ];
        let complement = generate_complement(&strand);
        assert!(is_valid_pairing(&strand, &complement));
    }

    #[test]
    fn corruption_is_detected_with_position() {
        let strand = vec![Codon::new(0, 1, 2).unwrap(), Codon::new(3, 0, 1).unwrap()];
        let mut complement = generate_complement(&strand);
        complement[1] = Codon::new(1, 3, 2).unwrap(); // corrupt first symbol only

        let issues = find_corruption(&strand, &complement);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].codon_index, 1);
        assert_eq!(issues[0].symbol_index, 0);
        assert!(!is_valid_pairing(&strand, &complement));
    }
}
