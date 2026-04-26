//! Developer-facing query AST (what a builder / wire adapter produces before compilation).

use serde::{Deserialize, Serialize};

/// Sort direction for `orderBy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SortDirection {
    Asc,
    Desc,
}

/// Comparison operator in a `where` clause.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WhereOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    Like,
}

/// Literal value in a `where` clause (restricted set for Stage 2 compiler v1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum QueryLiteral {
    String(String),
    I64(i64),
    U64(u64),
    F64(f64),
    Bool(bool),
}

/// One `where(field, op, value)` constraint.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WhereClause {
    pub field: String,
    pub op: WhereOp,
    pub value: QueryLiteral,
}

/// Optional ordering after filtering.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderByClause {
    pub field: String,
    pub direction: SortDirection,
}

/// Fluent-style query before compilation to a [`GuidePattern`](super::guide::GuidePattern).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryAst {
    pub collection: String,
    pub wheres: Vec<WhereClause>,
    pub includes: Vec<String>,
    pub order_by: Option<OrderByClause>,
    pub limit: Option<u32>,
}

impl QueryAst {
    pub fn new(collection: impl Into<String>) -> Self {
        Self {
            collection: collection.into(),
            wheres: Vec::new(),
            includes: Vec::new(),
            order_by: None,
            limit: None,
        }
    }

    pub fn r#where(mut self, field: impl Into<String>, op: WhereOp, value: QueryLiteral) -> Self {
        self.wheres.push(WhereClause {
            field: field.into(),
            op,
            value,
        });
        self
    }

    pub fn include(mut self, path: impl Into<String>) -> Self {
        self.includes.push(path.into());
        self
    }

    pub fn order_by(mut self, field: impl Into<String>, direction: SortDirection) -> Self {
        self.order_by = Some(OrderByClause {
            field: field.into(),
            direction,
        });
        self
    }

    pub fn limit(mut self, n: u32) -> Self {
        self.limit = Some(n);
        self
    }
}
