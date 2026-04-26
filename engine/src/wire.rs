//! Stage 6 wire compatibility translation primitives.
//!
//! Baseline Mongo command adapters that translate wire-facing payloads into
//! internal engine operations.

use std::collections::HashMap;

use serde_json::{Map, Value};
use thiserror::Error;

use crate::query::{QueryAst, QueryLiteral, SortDirection, WhereOp};

#[derive(Debug, Clone, PartialEq)]
pub struct MongoFindCommand {
    pub collection: String,
    pub filter: Map<String, Value>,
    pub sort: Option<Map<String, Value>>,
    pub limit: Option<u32>,
    pub include_paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MongoInsertOneCommand {
    pub collection: String,
    pub document: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MongoUpdateOneCommand {
    pub collection: String,
    pub filter: Map<String, Value>,
    pub update: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MongoDeleteOneCommand {
    pub collection: String,
    pub filter: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MongoCommand {
    Find(MongoFindCommand),
    InsertOne(MongoInsertOneCommand),
    UpdateOne(MongoUpdateOneCommand),
    DeleteOne(MongoDeleteOneCommand),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresQuery {
    pub sql: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertOp {
    pub collection: String,
    pub record: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateOp {
    pub collection: String,
    pub filter: QueryAst,
    pub set_fields: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteOp {
    pub collection: String,
    pub filter: QueryAst,
    pub soft_delete: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WireOperation {
    Query(QueryAst),
    Insert(InsertOp),
    Update(UpdateOp),
    Delete(DeleteOp),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolFlavor {
    Mongo,
    Postgres,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompatibilityExpectation {
    Supported,
    Unsupported,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CompatibilityInput {
    Mongo(MongoCommand),
    Postgres(PostgresQuery),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompatibilityCase {
    pub name: String,
    pub protocol: ProtocolFlavor,
    pub input: CompatibilityInput,
    pub expectation: CompatibilityExpectation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityCaseResult {
    pub name: String,
    pub protocol: ProtocolFlavor,
    pub expectation: CompatibilityExpectation,
    pub passed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityMatrixReport {
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    pub results: Vec<CompatibilityCaseResult>,
}

#[derive(Debug, Error, PartialEq)]
pub enum WireTranslateError {
    #[error("collection cannot be empty")]
    EmptyCollection,
    #[error("unsupported mongo operator: {0}")]
    UnsupportedOperator(String),
    #[error("unsupported literal for field `{field}`")]
    UnsupportedLiteral { field: String },
    #[error("find sort allows one field in baseline translator")]
    MultiFieldSortUnsupported,
    #[error("update requires `$set` object in baseline translator")]
    InvalidUpdateShape,
    #[error("unsupported sql statement in baseline translator")]
    UnsupportedSqlStatement,
    #[error("invalid sql statement in baseline translator")]
    InvalidSqlStatement,
}

pub fn translate_mongo_command(cmd: MongoCommand) -> Result<WireOperation, WireTranslateError> {
    match cmd {
        MongoCommand::Find(find) => Ok(WireOperation::Query(translate_find(find)?)),
        MongoCommand::InsertOne(insert) => translate_insert(insert),
        MongoCommand::UpdateOne(update) => translate_update(update),
        MongoCommand::DeleteOne(delete) => translate_delete(delete),
    }
}

pub fn translate_postgres_query(q: PostgresQuery) -> Result<WireOperation, WireTranslateError> {
    let sql = q.sql.trim();
    let upper = sql.to_ascii_uppercase();
    if upper.starts_with("SELECT ") {
        return translate_sql_select(sql);
    }
    if upper.starts_with("INSERT ") {
        return translate_sql_insert(sql);
    }
    if upper.starts_with("UPDATE ") {
        return translate_sql_update(sql);
    }
    if upper.starts_with("DELETE ") {
        return translate_sql_delete(sql);
    }
    Err(WireTranslateError::UnsupportedSqlStatement)
}

/// Execute a baseline compatibility matrix using in-process translators.
pub fn run_baseline_compatibility_matrix(cases: &[CompatibilityCase]) -> CompatibilityMatrixReport {
    let mut results = Vec::with_capacity(cases.len());
    for case in cases {
        let translated = match &case.input {
            CompatibilityInput::Mongo(cmd) => translate_mongo_command(cmd.clone()),
            CompatibilityInput::Postgres(query) => translate_postgres_query(query.clone()),
        };
        let passed = matches!(
            (case.expectation, translated.is_ok()),
            (CompatibilityExpectation::Supported, true)
                | (CompatibilityExpectation::Unsupported, false)
        );
        results.push(CompatibilityCaseResult {
            name: case.name.clone(),
            protocol: case.protocol,
            expectation: case.expectation,
            passed,
        });
    }
    let passed = results.iter().filter(|r| r.passed).count();
    let total = results.len();
    CompatibilityMatrixReport {
        total,
        passed,
        failed: total.saturating_sub(passed),
        results,
    }
}

/// Stage 6 baseline matrix targeting representative client query shapes.
pub fn default_baseline_compatibility_cases() -> Vec<CompatibilityCase> {
    vec![
        CompatibilityCase {
            name: "mongoose-find-basic".into(),
            protocol: ProtocolFlavor::Mongo,
            input: CompatibilityInput::Mongo(MongoCommand::Find(MongoFindCommand {
                collection: "users".into(),
                filter: Map::new(),
                sort: None,
                limit: Some(20),
                include_paths: vec![],
            })),
            expectation: CompatibilityExpectation::Supported,
        },
        CompatibilityCase {
            name: "mongodb-where-javascript".into(),
            protocol: ProtocolFlavor::Mongo,
            input: CompatibilityInput::Mongo(MongoCommand::Find(MongoFindCommand {
                collection: "users".into(),
                filter: {
                    let mut m = Map::new();
                    m.insert("$where".into(), Value::String("this.age > 10".into()));
                    m
                },
                sort: None,
                limit: None,
                include_paths: vec![],
            })),
            expectation: CompatibilityExpectation::Unsupported,
        },
        CompatibilityCase {
            name: "node-postgres-select".into(),
            protocol: ProtocolFlavor::Postgres,
            input: CompatibilityInput::Postgres(PostgresQuery {
                sql: "SELECT * FROM users WHERE email = 'a@b.com'".into(),
            }),
            expectation: CompatibilityExpectation::Supported,
        },
        CompatibilityCase {
            name: "postgres-ddl-create-table".into(),
            protocol: ProtocolFlavor::Postgres,
            input: CompatibilityInput::Postgres(PostgresQuery {
                sql: "CREATE TABLE users (id bigint)".into(),
            }),
            expectation: CompatibilityExpectation::Unsupported,
        },
    ]
}

fn translate_find(find: MongoFindCommand) -> Result<QueryAst, WireTranslateError> {
    ensure_collection(&find.collection)?;
    let mut ast = query_from_filter(find.collection, &find.filter)?;

    if let Some(sort) = find.sort {
        if sort.len() > 1 {
            return Err(WireTranslateError::MultiFieldSortUnsupported);
        }
        if let Some((field, dir)) = sort.into_iter().next() {
            let direction = match dir {
                Value::Number(n) if n.as_i64().unwrap_or(1) < 0 => SortDirection::Desc,
                _ => SortDirection::Asc,
            };
            ast = ast.order_by(field, direction);
        }
    }
    if let Some(limit) = find.limit {
        ast = ast.limit(limit);
    }
    for include in find.include_paths {
        ast = ast.include(include);
    }
    Ok(ast)
}

fn translate_insert(insert: MongoInsertOneCommand) -> Result<WireOperation, WireTranslateError> {
    ensure_collection(&insert.collection)?;
    Ok(WireOperation::Insert(InsertOp {
        collection: insert.collection,
        record: insert.document,
    }))
}

fn translate_update(update: MongoUpdateOneCommand) -> Result<WireOperation, WireTranslateError> {
    ensure_collection(&update.collection)?;
    let filter = query_from_filter(update.collection.clone(), &update.filter)?;
    let Some(set_value) = update.update.get("$set") else {
        return Err(WireTranslateError::InvalidUpdateShape);
    };
    let Value::Object(set_obj) = set_value else {
        return Err(WireTranslateError::InvalidUpdateShape);
    };
    let set_fields: HashMap<String, Value> = set_obj
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    Ok(WireOperation::Update(UpdateOp {
        collection: update.collection,
        filter,
        set_fields,
    }))
}

fn translate_delete(delete: MongoDeleteOneCommand) -> Result<WireOperation, WireTranslateError> {
    ensure_collection(&delete.collection)?;
    let filter = query_from_filter(delete.collection.clone(), &delete.filter)?;
    Ok(WireOperation::Delete(DeleteOp {
        collection: delete.collection,
        filter,
        soft_delete: true,
    }))
}

fn ensure_collection(collection: &str) -> Result<(), WireTranslateError> {
    if collection.trim().is_empty() {
        return Err(WireTranslateError::EmptyCollection);
    }
    Ok(())
}

fn query_from_filter(
    collection: String,
    filter: &Map<String, Value>,
) -> Result<QueryAst, WireTranslateError> {
    let mut ast = QueryAst::new(collection);

    for (field, raw) in filter {
        if field == "$where" {
            return Err(WireTranslateError::UnsupportedOperator("$where".into()));
        }
        match raw {
            Value::Object(obj) => {
                for (op, operand) in obj {
                    let where_op = parse_operator(op)?;
                    let lit = parse_literal(field, operand)?;
                    ast = ast.r#where(field.clone(), where_op, lit);
                }
            }
            _ => {
                let lit = parse_literal(field, raw)?;
                ast = ast.r#where(field.clone(), WhereOp::Eq, lit);
            }
        }
    }
    Ok(ast)
}

fn parse_operator(op: &str) -> Result<WhereOp, WireTranslateError> {
    match op {
        "$eq" => Ok(WhereOp::Eq),
        "$ne" => Ok(WhereOp::Ne),
        "$gt" => Ok(WhereOp::Gt),
        "$gte" => Ok(WhereOp::Gte),
        "$lt" => Ok(WhereOp::Lt),
        "$lte" => Ok(WhereOp::Lte),
        "$regex" => Ok(WhereOp::Like),
        other => Err(WireTranslateError::UnsupportedOperator(other.to_string())),
    }
}

fn parse_literal(field: &str, value: &Value) -> Result<QueryLiteral, WireTranslateError> {
    match value {
        Value::String(s) => Ok(QueryLiteral::String(s.clone())),
        Value::Bool(b) => Ok(QueryLiteral::Bool(*b)),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(QueryLiteral::I64(i))
            } else if let Some(u) = n.as_u64() {
                Ok(QueryLiteral::U64(u))
            } else if let Some(f) = n.as_f64() {
                Ok(QueryLiteral::F64(f))
            } else {
                Err(WireTranslateError::UnsupportedLiteral {
                    field: field.to_string(),
                })
            }
        }
        _ => Err(WireTranslateError::UnsupportedLiteral {
            field: field.to_string(),
        }),
    }
}

fn translate_sql_select(sql: &str) -> Result<WireOperation, WireTranslateError> {
    let upper = sql.to_ascii_uppercase();
    let from_idx = upper
        .find(" FROM ")
        .ok_or(WireTranslateError::InvalidSqlStatement)?;
    let collection_and_tail = sql[from_idx + " FROM ".len()..].trim();
    let (collection, tail) = split_first_word(collection_and_tail)?;
    ensure_collection(collection)?;

    let mut ast = QueryAst::new(collection.to_string());
    let tail_upper = tail.to_ascii_uppercase();
    if let Some(where_idx) = tail_upper.find(" WHERE ") {
        let clause = tail[where_idx + " WHERE ".len()..].trim();
        ast = apply_sql_where(ast, clause)?;
    }
    Ok(WireOperation::Query(ast))
}

fn translate_sql_insert(sql: &str) -> Result<WireOperation, WireTranslateError> {
    let upper = sql.to_ascii_uppercase();
    let into_idx = upper
        .find("INTO ")
        .ok_or(WireTranslateError::InvalidSqlStatement)?;
    let after_into = sql[into_idx + "INTO ".len()..].trim();
    let open_cols = after_into
        .find('(')
        .ok_or(WireTranslateError::InvalidSqlStatement)?;
    let collection = after_into[..open_cols].trim();
    ensure_collection(collection)?;
    let close_cols = after_into
        .find(')')
        .ok_or(WireTranslateError::InvalidSqlStatement)?;
    let columns_raw = &after_into[open_cols + 1..close_cols];
    let values_pos = after_into
        .to_ascii_uppercase()
        .find("VALUES")
        .ok_or(WireTranslateError::InvalidSqlStatement)?;
    let values_raw = after_into[values_pos + "VALUES".len()..].trim();
    let values_trimmed = values_raw
        .strip_prefix('(')
        .and_then(|v| v.strip_suffix(')'))
        .ok_or(WireTranslateError::InvalidSqlStatement)?;

    let columns: Vec<&str> = columns_raw
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();
    let values: Vec<&str> = values_trimmed.split(',').map(|s| s.trim()).collect();
    if columns.len() != values.len() || columns.is_empty() {
        return Err(WireTranslateError::InvalidSqlStatement);
    }
    let mut obj = Map::new();
    for (col, value) in columns.into_iter().zip(values.into_iter()) {
        obj.insert(col.to_string(), parse_sql_value(value));
    }
    Ok(WireOperation::Insert(InsertOp {
        collection: collection.to_string(),
        record: Value::Object(obj),
    }))
}

fn translate_sql_update(sql: &str) -> Result<WireOperation, WireTranslateError> {
    let upper = sql.to_ascii_uppercase();
    let set_idx = upper
        .find(" SET ")
        .ok_or(WireTranslateError::InvalidSqlStatement)?;
    let collection = sql["UPDATE ".len()..set_idx].trim();
    ensure_collection(collection)?;
    let after_set = sql[set_idx + " SET ".len()..].trim();
    let after_set_upper = after_set.to_ascii_uppercase();
    let where_idx = after_set_upper
        .find(" WHERE ")
        .ok_or(WireTranslateError::InvalidSqlStatement)?;
    let set_clause = after_set[..where_idx].trim();
    let where_clause = after_set[where_idx + " WHERE ".len()..].trim();

    let mut set_fields = HashMap::new();
    for assignment in set_clause.split(',') {
        let (field, value) = split_on_first(assignment, '=')?;
        set_fields.insert(field.trim().to_string(), parse_sql_value(value.trim()));
    }
    let filter = apply_sql_where(QueryAst::new(collection.to_string()), where_clause)?;
    Ok(WireOperation::Update(UpdateOp {
        collection: collection.to_string(),
        filter,
        set_fields,
    }))
}

fn translate_sql_delete(sql: &str) -> Result<WireOperation, WireTranslateError> {
    let upper = sql.to_ascii_uppercase();
    let from_idx = upper
        .find("FROM ")
        .ok_or(WireTranslateError::InvalidSqlStatement)?;
    let after_from = sql[from_idx + "FROM ".len()..].trim();
    let after_from_upper = after_from.to_ascii_uppercase();
    let where_idx = after_from_upper
        .find(" WHERE ")
        .ok_or(WireTranslateError::InvalidSqlStatement)?;
    let collection = after_from[..where_idx].trim();
    ensure_collection(collection)?;
    let where_clause = after_from[where_idx + " WHERE ".len()..].trim();
    let filter = apply_sql_where(QueryAst::new(collection.to_string()), where_clause)?;
    Ok(WireOperation::Delete(DeleteOp {
        collection: collection.to_string(),
        filter,
        soft_delete: true,
    }))
}

fn apply_sql_where(mut ast: QueryAst, clause: &str) -> Result<QueryAst, WireTranslateError> {
    for part in split_case_insensitive(clause, " AND ") {
        let (field, op, value) = parse_sql_comparison(part.trim())?;
        ast = ast.r#where(field, op, value);
    }
    Ok(ast)
}

fn parse_sql_comparison(input: &str) -> Result<(String, WhereOp, QueryLiteral), WireTranslateError> {
    for (token, op) in [
        (">=", WhereOp::Gte),
        ("<=", WhereOp::Lte),
        ("!=", WhereOp::Ne),
        ("=", WhereOp::Eq),
        (">", WhereOp::Gt),
        ("<", WhereOp::Lt),
    ] {
        if let Some(idx) = input.find(token) {
            let field = input[..idx].trim();
            let raw = input[idx + token.len()..].trim();
            if field.is_empty() || raw.is_empty() {
                return Err(WireTranslateError::InvalidSqlStatement);
            }
            return Ok((field.to_string(), op, sql_literal_to_query(raw)));
        }
    }
    if let Some(idx) = input.to_ascii_uppercase().find(" LIKE ") {
        let field = input[..idx].trim();
        let raw = input[idx + " LIKE ".len()..].trim();
        return Ok((field.to_string(), WhereOp::Like, sql_literal_to_query(raw)));
    }
    Err(WireTranslateError::InvalidSqlStatement)
}

fn sql_literal_to_query(raw: &str) -> QueryLiteral {
    match parse_sql_value(raw) {
        Value::String(s) => QueryLiteral::String(s),
        Value::Bool(b) => QueryLiteral::Bool(b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                QueryLiteral::I64(i)
            } else if let Some(u) = n.as_u64() {
                QueryLiteral::U64(u)
            } else {
                QueryLiteral::F64(n.as_f64().unwrap_or_default())
            }
        }
        _ => QueryLiteral::String(raw.to_string()),
    }
}

fn parse_sql_value(raw: &str) -> Value {
    let trimmed = raw.trim();
    if let Some(stripped) = trimmed.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')) {
        return Value::String(stripped.to_string());
    }
    if trimmed.eq_ignore_ascii_case("true") {
        return Value::Bool(true);
    }
    if trimmed.eq_ignore_ascii_case("false") {
        return Value::Bool(false);
    }
    if let Ok(i) = trimmed.parse::<i64>() {
        return Value::from(i);
    }
    if let Ok(f) = trimmed.parse::<f64>() {
        return Value::from(f);
    }
    Value::String(trimmed.to_string())
}

fn split_first_word(input: &str) -> Result<(&str, &str), WireTranslateError> {
    if let Some(idx) = input.find(char::is_whitespace) {
        Ok((&input[..idx], &input[idx..]))
    } else if !input.is_empty() {
        Ok((input, ""))
    } else {
        Err(WireTranslateError::InvalidSqlStatement)
    }
}

fn split_on_first(input: &str, needle: char) -> Result<(&str, &str), WireTranslateError> {
    let idx = input.find(needle).ok_or(WireTranslateError::InvalidSqlStatement)?;
    Ok((&input[..idx], &input[idx + 1..]))
}

fn split_case_insensitive<'a>(input: &'a str, delimiter: &str) -> Vec<&'a str> {
    let mut parts = Vec::new();
    let mut rest = input;
    loop {
        let rest_upper = rest.to_ascii_uppercase();
        if let Some(idx) = rest_upper.find(&delimiter.to_ascii_uppercase()) {
            parts.push(rest[..idx].trim());
            rest = &rest[idx + delimiter.len()..];
        } else {
            parts.push(rest.trim());
            break;
        }
    }
    parts
}

#[cfg(test)]
mod tests {
    use super::{
        default_baseline_compatibility_cases, run_baseline_compatibility_matrix,
        translate_mongo_command, translate_postgres_query, MongoCommand, MongoDeleteOneCommand,
        MongoFindCommand, MongoInsertOneCommand, MongoUpdateOneCommand, PostgresQuery,
        WireOperation, WireTranslateError,
    };
    use crate::query::{SortDirection, WhereOp};
    use serde_json::{json, Map, Value};

    fn map(v: Value) -> Map<String, Value> {
        match v {
            Value::Object(m) => m,
            _ => panic!("expected object"),
        }
    }

    #[test]
    fn translates_find_to_query_ast() {
        let find = MongoFindCommand {
            collection: "users".into(),
            filter: map(json!({"email":"a@b.com","age":{"$gt":21}})),
            sort: Some(map(json!({"created_at": -1}))),
            limit: Some(10),
            include_paths: vec!["profile".into()],
        };
        let op = translate_mongo_command(MongoCommand::Find(find)).expect("translate");
        let WireOperation::Query(ast) = op else {
            panic!("expected query op");
        };
        assert_eq!(ast.collection, "users");
        assert_eq!(ast.wheres.len(), 2);
        let age_clause = ast
            .wheres
            .iter()
            .find(|w| w.field == "age")
            .expect("age clause");
        assert_eq!(age_clause.op, WhereOp::Gt);
        assert_eq!(
            ast.order_by.as_ref().expect("sort").direction,
            SortDirection::Desc
        );
        assert_eq!(ast.limit, Some(10));
        assert_eq!(ast.includes, vec!["profile".to_string()]);
    }

    #[test]
    fn rejects_where_operator() {
        let find = MongoFindCommand {
            collection: "users".into(),
            filter: map(json!({"$where":"this.age > 10"})),
            sort: None,
            limit: None,
            include_paths: vec![],
        };
        let err = translate_mongo_command(MongoCommand::Find(find)).expect_err("must fail");
        assert_eq!(
            err,
            WireTranslateError::UnsupportedOperator("$where".to_string())
        );
    }

    #[test]
    fn translates_insert_update_delete() {
        let insert = translate_mongo_command(MongoCommand::InsertOne(MongoInsertOneCommand {
            collection: "users".into(),
            document: json!({"email":"x@y.com"}),
        }))
        .expect("insert");
        assert!(matches!(insert, WireOperation::Insert(_)));

        let update = translate_mongo_command(MongoCommand::UpdateOne(MongoUpdateOneCommand {
            collection: "users".into(),
            filter: map(json!({"email":"x@y.com"})),
            update: map(json!({"$set":{"name":"Alice"}})),
        }))
        .expect("update");
        let WireOperation::Update(update) = update else {
            panic!("expected update op");
        };
        assert_eq!(update.set_fields.get("name"), Some(&json!("Alice")));

        let delete = translate_mongo_command(MongoCommand::DeleteOne(MongoDeleteOneCommand {
            collection: "users".into(),
            filter: map(json!({"email":"x@y.com"})),
        }))
        .expect("delete");
        let WireOperation::Delete(delete) = delete else {
            panic!("expected delete op");
        };
        assert!(delete.soft_delete);
    }

    #[test]
    fn rejects_non_set_update_shape() {
        let err = translate_mongo_command(MongoCommand::UpdateOne(MongoUpdateOneCommand {
            collection: "users".into(),
            filter: map(json!({"id":1})),
            update: map(json!({"name":"Alice"})),
        }))
        .expect_err("must fail");
        assert_eq!(err, WireTranslateError::InvalidUpdateShape);
    }

    #[test]
    fn translates_postgres_select() {
        let op = translate_postgres_query(PostgresQuery {
            sql: "SELECT * FROM users WHERE email = 'a@b.com' AND age >= 21".into(),
        })
        .expect("select");
        let WireOperation::Query(ast) = op else {
            panic!("expected query op");
        };
        assert_eq!(ast.collection, "users");
        assert_eq!(ast.wheres.len(), 2);
    }

    #[test]
    fn translates_postgres_insert_update_delete() {
        let ins = translate_postgres_query(PostgresQuery {
            sql: "INSERT INTO users (email, age) VALUES ('a@b.com', 21)".into(),
        })
        .expect("insert");
        assert!(matches!(ins, WireOperation::Insert(_)));

        let upd = translate_postgres_query(PostgresQuery {
            sql: "UPDATE users SET name = 'Alice' WHERE email = 'a@b.com'".into(),
        })
        .expect("update");
        let WireOperation::Update(upd) = upd else {
            panic!("expected update op");
        };
        assert_eq!(upd.set_fields.get("name"), Some(&json!("Alice")));

        let del = translate_postgres_query(PostgresQuery {
            sql: "DELETE FROM users WHERE email = 'a@b.com'".into(),
        })
        .expect("delete");
        let WireOperation::Delete(del) = del else {
            panic!("expected delete op");
        };
        assert!(del.soft_delete);
    }

    #[test]
    fn rejects_unsupported_postgres_statement() {
        let err = translate_postgres_query(PostgresQuery {
            sql: "CREATE TABLE users (id bigint)".into(),
        })
        .expect_err("must fail");
        assert_eq!(err, WireTranslateError::UnsupportedSqlStatement);
    }

    #[test]
    fn baseline_compatibility_matrix_executes() {
        let cases = default_baseline_compatibility_cases();
        let report = run_baseline_compatibility_matrix(&cases);
        assert_eq!(report.total, 4);
        assert_eq!(report.failed, 0);
        assert_eq!(report.passed, 4);
        assert!(report.results.iter().all(|r| r.passed));
    }
}
