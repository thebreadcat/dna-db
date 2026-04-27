//! Minimal HTTP surface over `EngineRuntime` for local apps (CMS lab, scripts).
//!
//! Build: `cargo build --release --features http_server --bin dnadb_http`

use std::path::PathBuf;
use std::time::Instant;

use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use clap::Parser;
use dnadb_engine::runtime::EngineRuntime;
use dnadb_engine::transaction_durable::ExecutionResult;
use dnadb_engine::wire::{MongoCommand, MongoFindCommand, MongoInsertOneCommand};
use serde_json::{json, Map, Value};
use tokio::sync::Mutex;
use tower_http::cors::{Any, CorsLayer};
use tower_http::services::ServeDir;

#[derive(Parser, Debug)]
#[command(name = "dnadb_http")]
struct Args {
    /// Root directory for collection files (WAL + strands).
    #[arg(long, default_value = "./cms-data")]
    data_dir: PathBuf,

    /// Optional initial mmap hint for strand storage (bytes).
    #[arg(long)]
    mmap_bytes: Option<usize>,

    /// Listen address (host:port).
    #[arg(long, default_value = "127.0.0.1:8787")]
    bind: String,

    /// Static files for the CMS UI (served at `/`). If missing, only `/api/*` works.
    #[arg(long)]
    static_dir: Option<PathBuf>,
}

#[derive(Clone)]
struct AppState {
    rt: std::sync::Arc<Mutex<EngineRuntime>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    std::fs::create_dir_all(&args.data_dir)?;

    let rt = EngineRuntime::open(args.data_dir.as_path(), args.mmap_bytes);
    let state = AppState {
        rt: std::sync::Arc::new(Mutex::new(rt)),
    };

    let api = Router::new()
        .route("/health", get(health))
        .route(
            "/collections/:collection/sort-indexes",
            get(get_sort_indexes).post(configure_sort_indexes),
        )
        .route(
            "/collections/:collection/sort-indexes/add",
            post(add_sort_index),
        )
        .route(
            "/collections/:collection/documents/bulk",
            post(bulk_insert),
        )
        .route(
            "/collections/:collection/documents",
            post(insert_one),
        )
        .route("/collections/:collection/query", post(query_find))
        .with_state(state.clone());

    let mut app = Router::new().nest("/api", api);

    if let Some(dir) = args.static_dir.as_ref() {
        if dir.is_dir() {
            app = app.fallback_service(
                ServeDir::new(dir).append_index_html_on_directories(true),
            );
        } else {
            eprintln!(
                "warning: --static-dir {:?} is not a directory; skipping static hosting",
                dir
            );
        }
    }

    let app = app.layer(
        CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any),
    );

    let listener = tokio::net::TcpListener::bind(&args.bind).await?;
    eprintln!(
        "dnadb_http listening on http://{}  (data_dir={})",
        args.bind,
        args.data_dir.display()
    );
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> Json<Value> {
    Json(json!({ "ok": true, "service": "dnadb_http" }))
}

async fn insert_one(
    State(state): State<AppState>,
    AxumPath(collection): AxumPath<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let t0 = Instant::now();
    let mut rt = state.rt.lock().await;
    let cmd = MongoCommand::InsertOne(MongoInsertOneCommand {
        collection,
        document: body,
    });
    let out = rt.execute_mongo_command(cmd).map_err(ApiError::runtime)?;
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    match out {
        ExecutionResult::AffectedRows(n) => Ok(Json(json!({ "ok": true, "affected": n, "ms": ms }))),
        other => Err(ApiError::bad_request(format!("unexpected result: {other:?}"))),
    }
}

#[derive(serde::Deserialize)]
struct BulkBody {
    documents: Vec<Value>,
}

async fn bulk_insert(
    State(state): State<AppState>,
    AxumPath(collection): AxumPath<String>,
    Json(body): Json<BulkBody>,
) -> Result<Json<Value>, ApiError> {
    const MAX_BATCH: usize = 2_000;
    if body.documents.len() > MAX_BATCH {
        return Err(ApiError::bad_request(format!(
            "batch too large (max {MAX_BATCH})"
        )));
    }
    let batch_size = body.documents.len();
    let t0 = Instant::now();
    let mut rt = state.rt.lock().await;
    let affected = match rt
        .execute_mongo_insert_many(&collection, body.documents)
        .map_err(ApiError::runtime)?
    {
        ExecutionResult::AffectedRows(n) => n,
        other => return Err(ApiError::bad_request(format!("unexpected result: {other:?}"))),
    };
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    Ok(Json(
        json!({ "ok": true, "affected": affected, "ms": ms, "batch_size": batch_size }),
    ))
}

#[derive(serde::Deserialize)]
struct QueryBody {
    #[serde(default)]
    filter: Map<String, Value>,
    limit: Option<u32>,
    #[serde(default)]
    sort: Option<Map<String, Value>>,
}

async fn query_find(
    State(state): State<AppState>,
    AxumPath(collection): AxumPath<String>,
    Json(body): Json<QueryBody>,
) -> Result<Json<Value>, ApiError> {
    let t0 = Instant::now();
    let mut rt = state.rt.lock().await;
    let cmd = MongoCommand::Find(MongoFindCommand {
        collection,
        filter: body.filter,
        sort: body.sort,
        limit: body.limit,
        include_paths: vec![],
    });
    let out = rt.execute_mongo_command(cmd).map_err(ApiError::runtime)?;
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    match out {
        ExecutionResult::QueryRows(rows) => {
            let rows: Vec<Value> = rows
                .into_iter()
                .map(|m| Value::Object(m.into_iter().collect()))
                .collect();
            let count = rows.len();
            Ok(Json(
                json!({ "ok": true, "rows": rows, "count": count, "ms": ms }),
            ))
        }
        other => Err(ApiError::bad_request(format!("unexpected result: {other:?}"))),
    }
}

#[derive(serde::Deserialize)]
struct SortIndexConfigBody {
    #[serde(default)]
    sort_indexes: Vec<SortIndexSpec>,
}

#[derive(serde::Deserialize)]
struct SortIndexSpec {
    field: String,
    #[allow(dead_code)]
    order: Option<String>,
}

#[derive(serde::Deserialize)]
struct AddSortIndexBody {
    field: String,
}

async fn get_sort_indexes(
    State(state): State<AppState>,
    AxumPath(collection): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    let mut rt = state.rt.lock().await;
    let fields = rt
        .sort_index_fields(&collection)
        .map_err(ApiError::runtime)?;
    Ok(Json(json!({ "ok": true, "collection": collection, "sort_indexes": fields })))
}

async fn configure_sort_indexes(
    State(state): State<AppState>,
    AxumPath(collection): AxumPath<String>,
    Json(body): Json<SortIndexConfigBody>,
) -> Result<Json<Value>, ApiError> {
    let fields: Vec<String> = body
        .sort_indexes
        .into_iter()
        .map(|s| s.field.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let mut rt = state.rt.lock().await;
    let active = rt
        .configure_sort_indexes(&collection, &fields)
        .map_err(ApiError::runtime)?;
    Ok(Json(
        json!({ "ok": true, "collection": collection, "sort_indexes": active, "configured": true }),
    ))
}

async fn add_sort_index(
    State(state): State<AppState>,
    AxumPath(collection): AxumPath<String>,
    Json(body): Json<AddSortIndexBody>,
) -> Result<Json<Value>, ApiError> {
    let field = body.field.trim();
    if field.is_empty() {
        return Err(ApiError::bad_request("field is required".to_string()));
    }
    let mut rt = state.rt.lock().await;
    let active = rt
        .add_sort_index(&collection, field)
        .map_err(ApiError::runtime)?;
    Ok(Json(
        json!({ "ok": true, "collection": collection, "sort_indexes": active, "added": field }),
    ))
}

struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: String) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message,
        }
    }

    fn runtime(e: dnadb_engine::runtime::RuntimeError) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: e.to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = Json(json!({ "ok": false, "error": self.message }));
        (self.status, body).into_response()
    }
}
