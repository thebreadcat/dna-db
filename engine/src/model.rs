use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Codon(pub u8, pub u8, pub u8);

impl Codon {
    pub fn new(a: u8, b: u8, c: u8) -> Option<Self> {
        if a < 4 && b < 4 && c < 4 {
            Some(Self(a, b, c))
        } else {
            None
        }
    }

    pub fn values(self) -> [u8; 3] {
        [self.0, self.1, self.2]
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Intron {
    pub field_name: String,
    pub codon_offset: u32,
    pub codon_length: u16,
    pub value_hash: u64,
    /// Optional structural pointer to another strand (for `.include("…")` resolution).
    #[serde(default)]
    pub references_strand: Option<[u8; 8]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RefreshPolicy {
    AutoOnRead,
    Manual,
    Immortal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Telomere {
    pub count: u16,
    pub immortal: bool,
    pub last_refresh: u64,
    pub refresh_policy: RefreshPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tag {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Strand {
    pub signature: [u8; 8],
    pub collection_id: u32,
    pub codons: Vec<Codon>,
    pub complement: Vec<Codon>,
    pub introns: Vec<Intron>,
    pub telomere: Telomere,
    pub epigenetic_tags: Vec<Tag>,
    pub version: u64,
    pub created_at: u64,
    pub updated_at: u64,
}
