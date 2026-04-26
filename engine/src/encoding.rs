use crate::model::Codon;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EncodedPayload {
    pub codons: Vec<Codon>,
    pub original_len: usize,
}

#[derive(Debug, Error)]
pub enum EncodingError {
    #[error("invalid codon symbol: {0}")]
    InvalidSymbol(u8),
    #[error("insufficient encoded symbols")]
    InsufficientSymbols,
}

/// Minimum input length before the AVX2 fast path is considered (x86_64 only).
pub const SIMD_ENCODE_THRESHOLD: usize = 64;

/// Scalar base-4 expansion (reference implementation, always available).
pub fn encode_bytes_to_codons_scalar(bytes: &[u8]) -> EncodedPayload {
    let mut symbols = Vec::with_capacity(bytes.len().saturating_mul(4));
    expand_bytes_to_symbols_scalar(bytes, &mut symbols);
    EncodedPayload {
        codons: symbols_to_codons(&symbols),
        original_len: bytes.len(),
    }
}

pub(crate) fn expand_bytes_to_symbols_scalar(bytes: &[u8], symbols: &mut Vec<u8>) {
    for byte in bytes {
        symbols.push((byte >> 6) & 0b11);
        symbols.push((byte >> 4) & 0b11);
        symbols.push((byte >> 2) & 0b11);
        symbols.push(byte & 0b11);
    }
}

fn symbols_to_codons(symbols: &[u8]) -> Vec<Codon> {
    let mut codons = Vec::with_capacity((symbols.len() + 2) / 3);
    for chunk in symbols.chunks(3) {
        let a = *chunk.first().unwrap_or(&0);
        let b = *chunk.get(1).unwrap_or(&0);
        let c = *chunk.get(2).unwrap_or(&0);
        codons.push(Codon::new(a, b, c).expect("symbol values are in base-4 range"));
    }
    codons
}

/// x86_64 AVX2 path (widening + lane shifts + interleaved symbol emission). Falls back to scalar
/// when AVX2 is unavailable or on other architectures.
#[cfg(target_arch = "x86_64")]
pub fn encode_bytes_to_codons_avx2(bytes: &[u8]) -> EncodedPayload {
    if !std::arch::is_x86_feature_detected!("avx2") {
        return encode_bytes_to_codons_scalar(bytes);
    }
    let mut symbols = Vec::with_capacity(bytes.len().saturating_mul(4));
    unsafe {
        avx2::expand_bytes_to_symbols_avx2(bytes, &mut symbols);
    }
    EncodedPayload {
        codons: symbols_to_codons(&symbols),
        original_len: bytes.len(),
    }
}

#[cfg(not(target_arch = "x86_64"))]
pub fn encode_bytes_to_codons_avx2(bytes: &[u8]) -> EncodedPayload {
    encode_bytes_to_codons_scalar(bytes)
}

/// Default encoder: AVX2 on x86_64 when available and payload is large enough; scalar otherwise.
pub fn encode_bytes_to_codons(bytes: &[u8]) -> EncodedPayload {
    #[cfg(target_arch = "x86_64")]
    {
        if bytes.len() >= SIMD_ENCODE_THRESHOLD && std::arch::is_x86_feature_detected!("avx2") {
            return encode_bytes_to_codons_avx2(bytes);
        }
    }
    encode_bytes_to_codons_scalar(bytes)
}

#[cfg(target_arch = "x86_64")]
mod avx2 {
    use std::arch::x86_64::*;

    use super::expand_bytes_to_symbols_scalar;

    /// # Safety
    /// Call only when AVX2 is supported (`is_x86_feature_detected!("avx2")`).
    #[target_feature(enable = "avx2")]
    pub unsafe fn expand_bytes_to_symbols_avx2(bytes: &[u8], symbols: &mut Vec<u8>) {
        let len = bytes.len();
        let mut i = 0usize;
        while i + 16 <= len {
            let v = _mm_loadu_si128(bytes.as_ptr().add(i) as *const __m128i);
            let w = _mm256_cvtepu8_epi16(v);
            let m3 = _mm256_set1_epi16(3);
            let s0 = _mm256_and_si256(_mm256_srli_epi16(w, 6), m3);
            let s1 = _mm256_and_si256(_mm256_srli_epi16(w, 4), m3);
            let s2 = _mm256_and_si256(_mm256_srli_epi16(w, 2), m3);
            let s3 = _mm256_and_si256(w, m3);

            #[repr(align(32))]
            struct A([i16; 16]);

            let mut a = A([0_i16; 16]);
            let mut b = A([0_i16; 16]);
            let mut c = A([0_i16; 16]);
            let mut d = A([0_i16; 16]);

            _mm256_storeu_si256(a.0.as_mut_ptr() as *mut __m256i, s0);
            _mm256_storeu_si256(b.0.as_mut_ptr() as *mut __m256i, s1);
            _mm256_storeu_si256(c.0.as_mut_ptr() as *mut __m256i, s2);
            _mm256_storeu_si256(d.0.as_mut_ptr() as *mut __m256i, s3);

            symbols.reserve(64);
            for idx in 0..16 {
                symbols.push(a.0[idx] as u8);
                symbols.push(b.0[idx] as u8);
                symbols.push(c.0[idx] as u8);
                symbols.push(d.0[idx] as u8);
            }
            i += 16;
        }
        expand_bytes_to_symbols_scalar(&bytes[i..], symbols);
    }
}

pub fn decode_codons_to_bytes(payload: &EncodedPayload) -> Result<Vec<u8>, EncodingError> {
    let required_symbols = payload.original_len * 4;
    let mut symbols = Vec::with_capacity(payload.codons.len() * 3);

    for codon in &payload.codons {
        for value in codon.values() {
            if value >= 4 {
                return Err(EncodingError::InvalidSymbol(value));
            }
            symbols.push(value);
        }
    }

    if symbols.len() < required_symbols {
        return Err(EncodingError::InsufficientSymbols);
    }

    symbols.truncate(required_symbols);

    let mut bytes = Vec::with_capacity(payload.original_len);
    for group in symbols.chunks_exact(4) {
        let b = (group[0] << 6) | (group[1] << 4) | (group[2] << 2) | group[3];
        bytes.push(b);
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::{
        decode_codons_to_bytes, encode_bytes_to_codons, encode_bytes_to_codons_scalar,
        SIMD_ENCODE_THRESHOLD,
    };
    #[cfg(target_arch = "x86_64")]
    use super::encode_bytes_to_codons_avx2;

    #[test]
    fn round_trip_empty_payload() {
        let input = Vec::<u8>::new();
        let encoded = encode_bytes_to_codons(&input);
        let decoded = decode_codons_to_bytes(&encoded).expect("decode");
        assert_eq!(decoded, input);
    }

    #[test]
    fn round_trip_text_payload() {
        let input = b"dna-db: stage-1 encoding".to_vec();
        let encoded = encode_bytes_to_codons(&input);
        let decoded = decode_codons_to_bytes(&encoded).expect("decode");
        assert_eq!(decoded, input);
    }

    #[test]
    fn round_trip_full_byte_range() {
        let input: Vec<u8> = (0..=255).collect();
        let encoded = encode_bytes_to_codons(&input);
        let decoded = decode_codons_to_bytes(&encoded).expect("decode");
        assert_eq!(decoded, input);
    }

    #[test]
    fn simd_matches_scalar_random_sizes() {
        let mut buf = Vec::new();
        for len in 0usize..512 {
            buf.clear();
            buf.extend((0..len).map(|i| ((i * 131 + 17) ^ (len * 3)) as u8));
            let s = encode_bytes_to_codons_scalar(buf.as_slice());
            let d = encode_bytes_to_codons(buf.as_slice());
            assert_eq!(s, d, "len {len} default vs scalar");
            #[cfg(target_arch = "x86_64")]
            if std::arch::is_x86_feature_detected!("avx2") {
                let a = encode_bytes_to_codons_avx2(buf.as_slice());
                assert_eq!(s, a, "len {len} avx2 vs scalar");
            }
        }
    }

    #[test]
    fn threshold_routes_to_scalar_on_small_payload() {
        let buf: Vec<u8> = (0..SIMD_ENCODE_THRESHOLD.saturating_sub(1)).map(|i| i as u8).collect();
        let s = encode_bytes_to_codons_scalar(&buf);
        let d = encode_bytes_to_codons(&buf);
        assert_eq!(s, d);
    }
}
