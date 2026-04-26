//! Internal guide pattern (CRISPR execution input) produced by the query compiler.

use serde::{Deserialize, Serialize};

use crate::encoding::EncodedPayload;

/// Range comparison for [`Clause::RangeMatch`](Clause::RangeMatch).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RangeOp {
    GreaterThan,
    GreaterOrEqual,
    LessThan,
    LessOrEqual,
    NotEqual,
}

/// Compiled clause against intron metadata / payload encoding.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Clause {
    /// Field must equal the encoded operand (full value check after intron hash hit).
    ExactMatch {
        field_intron: String,
        /// Canonical serialized literal (`bincode`) for deterministic equality checks.
        operand_wire: Vec<u8>,
        /// Same bytes run through base-4 codon expansion (spec `encode(...)`).
        operand_codons: EncodedPayload,
    },
    RangeMatch {
        field_intron: String,
        operator: RangeOp,
        operand_wire: Vec<u8>,
        operand_codons: EncodedPayload,
    },
    /// SQL-style `LIKE`; pattern evaluation happens in the executor (Stage 2+).
    LikePattern {
        field_intron: String,
        pattern: String,
    },
}

/// Internal representation dispatched to the CRISPR executor.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GuidePattern {
    pub collection: String,
    pub clauses: Vec<Clause>,
    pub includes: Vec<String>,
    /// Overlay name from auth context; `None` means “no overlay resolved yet”.
    pub overlay: Option<String>,
    pub order_by: Option<(String, super::ast::SortDirection)>,
    pub limit: Option<u32>,
}
