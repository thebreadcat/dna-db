//! Compile [`QueryAst`](super::ast::QueryAst) → [`GuidePattern`](super::guide::GuidePattern).

use thiserror::Error;

use super::ast::{QueryAst, QueryLiteral, WhereOp};
use super::guide::{Clause, GuidePattern, RangeOp};
use crate::encoding::encode_bytes_to_codons;

#[derive(Debug, Error)]
pub enum CompileError {
    #[error("collection name must be non-empty")]
    EmptyCollection,
    #[error("unsupported where operator for field `{0}`: LIKE is only supported as LikePattern")]
    UnsupportedOperator(String),
    #[error("serialization error: {0}")]
    Serialize(#[from] bincode::Error),
}

fn literal_wire(value: &QueryLiteral) -> Result<Vec<u8>, CompileError> {
    Ok(bincode::serialize(value)?)
}

fn wire_to_codons(wire: &[u8]) -> Result<crate::encoding::EncodedPayload, CompileError> {
    Ok(encode_bytes_to_codons(wire))
}

/// Turn a parsed [`QueryAst`] into a [`GuidePattern`] ready for the CRISPR layer.
pub fn compile_query(ast: &QueryAst, overlay: Option<String>) -> Result<GuidePattern, CompileError> {
    if ast.collection.is_empty() {
        return Err(CompileError::EmptyCollection);
    }

    let mut clauses = Vec::with_capacity(ast.wheres.len());

    for w in &ast.wheres {
        let field = w.field.clone();
        match w.op {
            WhereOp::Eq => {
                let wire = literal_wire(&w.value)?;
                let operand_codons = wire_to_codons(&wire)?;
                clauses.push(Clause::ExactMatch {
                    field_intron: field,
                    operand_wire: wire,
                    operand_codons,
                });
            }
            WhereOp::Ne => {
                let wire = literal_wire(&w.value)?;
                let operand_codons = wire_to_codons(&wire)?;
                clauses.push(Clause::RangeMatch {
                    field_intron: field,
                    operator: RangeOp::NotEqual,
                    operand_wire: wire,
                    operand_codons,
                });
            }
            WhereOp::Gt => {
                let wire = literal_wire(&w.value)?;
                let operand_codons = wire_to_codons(&wire)?;
                clauses.push(Clause::RangeMatch {
                    field_intron: field,
                    operator: RangeOp::GreaterThan,
                    operand_wire: wire,
                    operand_codons,
                });
            }
            WhereOp::Gte => {
                let wire = literal_wire(&w.value)?;
                let operand_codons = wire_to_codons(&wire)?;
                clauses.push(Clause::RangeMatch {
                    field_intron: field,
                    operator: RangeOp::GreaterOrEqual,
                    operand_wire: wire,
                    operand_codons,
                });
            }
            WhereOp::Lt => {
                let wire = literal_wire(&w.value)?;
                let operand_codons = wire_to_codons(&wire)?;
                clauses.push(Clause::RangeMatch {
                    field_intron: field,
                    operator: RangeOp::LessThan,
                    operand_wire: wire,
                    operand_codons,
                });
            }
            WhereOp::Lte => {
                let wire = literal_wire(&w.value)?;
                let operand_codons = wire_to_codons(&wire)?;
                clauses.push(Clause::RangeMatch {
                    field_intron: field,
                    operator: RangeOp::LessOrEqual,
                    operand_wire: wire,
                    operand_codons,
                });
            }
            WhereOp::Like => {
                let pattern = match &w.value {
                    QueryLiteral::String(s) => s.clone(),
                    _ => {
                        return Err(CompileError::UnsupportedOperator(format!(
                            "{} (LIKE requires string literal)",
                            w.field
                        )));
                    }
                };
                clauses.push(Clause::LikePattern {
                    field_intron: field,
                    pattern,
                });
            }
        }
    }

    let order_by = ast
        .order_by
        .as_ref()
        .map(|o| (o.field.clone(), o.direction));

    Ok(GuidePattern {
        collection: ast.collection.clone(),
        clauses,
        includes: ast.includes.clone(),
        overlay,
        order_by,
        limit: ast.limit,
    })
}

#[cfg(test)]
mod tests {
    use super::compile_query;
    use crate::query::ast::{QueryAst, QueryLiteral, SortDirection, WhereOp};
    use crate::query::guide::{Clause, RangeOp};

    #[test]
    fn compiles_email_eq_and_age_gt() {
        let ast = QueryAst::new("users")
            .r#where(
                "email",
                WhereOp::Eq,
                QueryLiteral::String("alice@example.com".into()),
            )
            .r#where("age", WhereOp::Gt, QueryLiteral::I64(25))
            .include("orders");

        let guide = compile_query(&ast, Some("admin".into())).expect("compile");

        assert_eq!(guide.collection, "users");
        assert_eq!(guide.overlay.as_deref(), Some("admin"));
        assert_eq!(guide.includes, vec!["orders".to_string()]);
        assert_eq!(guide.clauses.len(), 2);

        match &guide.clauses[0] {
            Clause::ExactMatch {
                field_intron,
                operand_wire,
                operand_codons,
            } => {
                assert_eq!(field_intron, "email");
                let lit: QueryLiteral = bincode::deserialize(operand_wire).expect("wire");
                assert_eq!(
                    lit,
                    QueryLiteral::String("alice@example.com".into())
                );
                assert_eq!(operand_codons.original_len, operand_wire.len());
            }
            _ => panic!("expected ExactMatch"),
        }

        match &guide.clauses[1] {
            Clause::RangeMatch {
                field_intron,
                operator,
                operand_wire,
                ..
            } => {
                assert_eq!(field_intron, "age");
                assert_eq!(*operator, RangeOp::GreaterThan);
                let lit: QueryLiteral = bincode::deserialize(operand_wire).expect("wire");
                assert_eq!(lit, QueryLiteral::I64(25));
            }
            _ => panic!("expected RangeMatch"),
        }
    }

    #[test]
    fn compiles_like_clause() {
        let ast = QueryAst::new("users").r#where(
            "email",
            WhereOp::Like,
            QueryLiteral::String("%@gmail.com".into()),
        );
        let guide = compile_query(&ast, None).expect("compile");
        match &guide.clauses[0] {
            Clause::LikePattern {
                field_intron,
                pattern,
            } => {
                assert_eq!(field_intron, "email");
                assert_eq!(pattern, "%@gmail.com");
            }
            _ => panic!("expected LikePattern"),
        }
    }

    #[test]
    fn rejects_empty_collection() {
        let ast = QueryAst {
            collection: String::new(),
            wheres: vec![],
            includes: vec![],
            order_by: None,
            limit: None,
        };
        assert!(compile_query(&ast, None).is_err());
    }

    #[test]
    fn order_by_and_limit_round_trip() {
        let ast = QueryAst::new("orders")
            .order_by("created_at", SortDirection::Desc)
            .limit(50);
        let guide = compile_query(&ast, None).expect("compile");
        assert_eq!(
            guide.order_by,
            Some(("created_at".into(), SortDirection::Desc))
        );
        assert_eq!(guide.limit, Some(50));
    }
}
