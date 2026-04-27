use crate::model::Strand;
use thiserror::Error;

pub const STRAND_FORMAT_MAGIC: [u8; 4] = *b"DNAS";
pub const STRAND_FORMAT_VERSION: u16 = 1;

#[derive(Debug, Error)]
pub enum CodecError {
    #[error("invalid strand magic header")]
    InvalidMagic,
    #[error("unsupported strand format version: {0}")]
    UnsupportedVersion(u16),
    #[error("payload too short")]
    PayloadTooShort,
    #[error("serialization error: {0}")]
    Serialize(#[from] bincode::Error),
}

pub trait StrandCodec {
    fn encode_strand(&self, strand: &Strand) -> Result<Vec<u8>, CodecError>;
    fn decode_strand(&self, bytes: &[u8]) -> Result<Strand, CodecError>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct BincodeStrandCodec;

impl StrandCodec for BincodeStrandCodec {
    fn encode_strand(&self, strand: &Strand) -> Result<Vec<u8>, CodecError> {
        let payload = bincode::serialize(strand)?;
        let mut out = Vec::with_capacity(6 + payload.len());
        out.extend_from_slice(&STRAND_FORMAT_MAGIC);
        out.extend_from_slice(&STRAND_FORMAT_VERSION.to_le_bytes());
        out.extend_from_slice(&payload);
        Ok(out)
    }

    fn decode_strand(&self, bytes: &[u8]) -> Result<Strand, CodecError> {
        if bytes.len() < 6 {
            return Err(CodecError::PayloadTooShort);
        }
        if bytes[0..4] != STRAND_FORMAT_MAGIC {
            return Err(CodecError::InvalidMagic);
        }

        let version = u16::from_le_bytes([bytes[4], bytes[5]]);
        if version != STRAND_FORMAT_VERSION {
            return Err(CodecError::UnsupportedVersion(version));
        }

        let strand = bincode::deserialize(&bytes[6..])?;
        Ok(strand)
    }
}

impl BincodeStrandCodec {
    /// Decode one strand frame and return `(strand, frame_len_bytes)`.
    ///
    /// This avoids re-encoding solely to compute frame length in scan loops.
    pub fn decode_strand_with_len(&self, bytes: &[u8]) -> Result<(Strand, usize), CodecError> {
        let strand = self.decode_strand(bytes)?;
        let payload_len = bincode::serialized_size(&strand)? as usize;
        Ok((strand, 6 + payload_len))
    }
}

#[cfg(test)]
mod tests {
    use super::{BincodeStrandCodec, CodecError, StrandCodec, STRAND_FORMAT_MAGIC};
    use crate::model::{Codon, Intron, RefreshPolicy, Strand, Tag, Telomere};

    fn sample_strand() -> Strand {
        Strand {
            signature: *b"STRAND01",
            collection_id: 42,
            codons: vec![Codon::new(0, 1, 2).unwrap(), Codon::new(3, 2, 1).unwrap()],
            complement: vec![Codon::new(3, 2, 1).unwrap(), Codon::new(0, 1, 2).unwrap()],
            introns: vec![Intron {
                field_name: "email".to_string(),
                codon_offset: 3,
                codon_length: 12,
                value_hash: 12345,
                references_strand: None,
            }],
            telomere: Telomere {
                count: 100,
                immortal: true,
                last_refresh: 1_717_000_000,
                refresh_policy: RefreshPolicy::Immortal,
            },
            epigenetic_tags: vec![Tag {
                key: "overlay".to_string(),
                value: "admin".to_string(),
            }],
            version: 1,
            created_at: 1_717_000_000,
            updated_at: 1_717_000_100,
        }
    }

    #[test]
    fn round_trip_serialization_preserves_strand() {
        let codec = BincodeStrandCodec;
        let strand = sample_strand();

        let encoded = codec.encode_strand(&strand).expect("encode");
        assert_eq!(&encoded[0..4], STRAND_FORMAT_MAGIC.as_slice());

        let decoded = codec.decode_strand(&encoded).expect("decode");
        assert_eq!(decoded, strand);
    }

    #[test]
    fn decode_fails_on_bad_magic() {
        let codec = BincodeStrandCodec;
        let mut payload = vec![0_u8; 6];
        payload[4] = 1;

        let err = codec.decode_strand(&payload).expect_err("bad magic should fail");
        assert!(matches!(err, CodecError::InvalidMagic));
    }
}
