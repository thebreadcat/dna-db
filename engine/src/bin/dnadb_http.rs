//! Minimal HTTP surface over `EngineRuntime` for local apps (CMS lab, scripts).
//!
//! Build: `cargo build --release --features http_server --bin dnadb_http`

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axum::extract::{Path as AxumPath, State};
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::extract::Request;
use axum::{Json, Router};
use dnadb_engine::auth::{CreateIdentityRequest, IdentityStore, IdentityType};
use dnadb_engine::overlay::{OverlayAccess, OverlayDefinition, OverlayMutation, OverlayRegistry, ResolvedOverlay};
use dnadb_engine::privacy::{mask_records_for_overlay, PrivacyError};
use clap::Parser;
use dnadb_engine::runtime::EngineRuntime;
use dnadb_engine::transaction_durable::ExecutionResult;
use dnadb_engine::wire::{MongoCommand, MongoFindCommand, MongoInsertOneCommand};
use serde_json::{json, Map, Value};
use tokio::sync::{Mutex, Semaphore};
use tower_http::cors::{Any, CorsLayer};
use tower_http::services::ServeDir;
use tracing::{info, warn};

const LATENCY_BUCKETS_MS: [u64; 9] = [1, 5, 10, 25, 50, 100, 250, 500, 1000];

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

    /// Require bearer auth for API routes.
    #[arg(long, default_value_t = false)]
    auth_required: bool,

    /// Bootstrap admin API key identity (`name=admin`, overlay=full).
    #[arg(long)]
    admin_api_key: Option<String>,

    /// Bootstrap redacted API key identity (`name=redacted-user`, overlay=redacted).
    #[arg(long)]
    redacted_api_key: Option<String>,

    /// Pre-warm sort index pages before accepting traffic.
    #[arg(long, default_value_t = true)]
    prewarm_indexes: bool,

    /// Max in-flight HTTP requests before returning 503.
    #[arg(long, default_value_t = 256)]
    max_connections: usize,
}

#[derive(Clone)]
struct AppState {
    rt: std::sync::Arc<Mutex<EngineRuntime>>,
    auth_required: bool,
    auth: std::sync::Arc<Mutex<IdentityStore>>,
    overlays: std::sync::Arc<OverlayRegistry>,
    metrics: std::sync::Arc<HttpMetrics>,
    request_slots: std::sync::Arc<Semaphore>,
    slow_query_ms: u64,
}

#[derive(Default)]
struct HttpMetrics {
    requests_total: AtomicU64,
    errors_total: AtomicU64,
    overload_rejections_total: AtomicU64,
    inflight_requests: AtomicU64,
    request_duration_ms_total: AtomicU64,
    request_duration_count: AtomicU64,
    slow_queries_total: AtomicU64,
    latency_bucket_counts: [AtomicU64; LATENCY_BUCKETS_MS.len()],
}

impl HttpMetrics {
    fn observe_request(&self, status: StatusCode, elapsed: Duration) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
        if status.as_u16() >= 500 {
            self.errors_total.fetch_add(1, Ordering::Relaxed);
        }
        let elapsed_ms_u64 = elapsed.as_millis().min(u128::from(u64::MAX)) as u64;
        self.request_duration_ms_total
            .fetch_add(elapsed_ms_u64, Ordering::Relaxed);
        self.request_duration_count.fetch_add(1, Ordering::Relaxed);
        for (idx, bucket) in LATENCY_BUCKETS_MS.iter().enumerate() {
            if elapsed_ms_u64 <= *bucket {
                self.latency_bucket_counts[idx].fetch_add(1, Ordering::Relaxed);
                break;
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "dnadb_http=info".to_string()),
        )
        .with_target(false)
        .compact()
        .init();
    let args = Args::parse();
    std::fs::create_dir_all(&args.data_dir)?;
    let slow_query_ms = std::env::var("DNADB_SLOW_QUERY_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(25);
    let max_connections = std::env::var("DNADB_HTTP_MAX_CONNECTIONS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(args.max_connections.max(1));

    let rt = EngineRuntime::open(args.data_dir.as_path(), args.mmap_bytes);
    let mut auth_store = IdentityStore::new();
    let mut overlays = OverlayRegistry::new();
    overlays.define_overlay(OverlayDefinition::full("full"));
    overlays.define_overlay(OverlayDefinition {
        name: "redacted".to_string(),
        access: OverlayAccess::Partial,
        collections: vec!["posts".to_string()],
        include_fields: {
            let mut m = std::collections::HashMap::new();
            m.insert(
                "posts".to_string(),
                vec!["id".to_string(), "title".to_string(), "status".to_string()],
            );
            m
        },
        exclude_fields: {
            let mut m = std::collections::HashMap::new();
            m.insert(
                "posts".to_string(),
                vec!["body".to_string(), "slug".to_string(), "author_email".to_string()],
            );
            m
        },
        mutations: vec![OverlayMutation::Read],
        extends: None,
        additionally_include: std::collections::HashMap::new(),
    });
    if let Some(api_key) = args
        .admin_api_key
        .or_else(|| std::env::var("DNADB_ADMIN_API_KEY").ok())
    {
        let _ = auth_store.create_identity(CreateIdentityRequest {
            name: "admin".to_string(),
            identity_type: IdentityType::Admin,
            overlay: "full".to_string(),
            allowed_collections: vec!["*".to_string()],
            token_expiry_seconds: 86_400,
            mfa_required: false,
            password: None,
            api_key: Some(api_key.clone()),
        });
        if let Ok(token) = auth_store.authenticate_with_api_key("admin", &api_key) {
            info!(token = %token.token, "dnadb_http auth bootstrap admin");
        }
    }
    if let Some(api_key) = args
        .redacted_api_key
        .or_else(|| std::env::var("DNADB_REDACTED_API_KEY").ok())
    {
        let _ = auth_store.create_identity(CreateIdentityRequest {
            name: "redacted-user".to_string(),
            identity_type: IdentityType::ReadOnly,
            overlay: "redacted".to_string(),
            allowed_collections: vec!["posts".to_string()],
            token_expiry_seconds: 86_400,
            mfa_required: false,
            password: None,
            api_key: Some(api_key.clone()),
        });
        if let Ok(token) = auth_store.authenticate_with_api_key("redacted-user", &api_key) {
            info!(token = %token.token, "dnadb_http auth bootstrap redacted-user");
        }
    }

    let state = AppState {
        rt: std::sync::Arc::new(Mutex::new(rt)),
        auth_required: args.auth_required || std::env::var("DNADB_AUTH_REQUIRED").ok().as_deref() == Some("1"),
        auth: std::sync::Arc::new(Mutex::new(auth_store)),
        overlays: std::sync::Arc::new(overlays),
        metrics: std::sync::Arc::new(HttpMetrics::default()),
        request_slots: std::sync::Arc::new(Semaphore::new(max_connections)),
        slow_query_ms,
    };

    let api = Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .route(
            "/collections/:collection/sort-indexes",
            get(get_sort_indexes).post(configure_sort_indexes),
        )
        .route(
            "/collections/:collection/composite-sort-indexes",
            post(configure_composite_sort_indexes),
        )
        .route(
            "/collections/:collection/indexes",
            get(get_index_status),
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
            "/collections/:collection/indexes/rebuild",
            post(rebuild_indexes),
        )
        .route(
            "/collections/:collection/configure",
            post(configure_collection),
        )
        .route(
            "/collections/:collection/storage/sync",
            post(sync_collection_storage),
        )
        .route(
            "/collections/:collection/documents",
            post(insert_one),
        )
        .route("/collections/:collection/query", post(query_find))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            request_concurrency_middleware,
        ))
        .with_state(state.clone());

    let mut app = Router::new().nest("/api", api);

    if let Some(dir) = args.static_dir.as_ref() {
        if dir.is_dir() {
            app = app.fallback_service(
                ServeDir::new(dir).append_index_html_on_directories(true),
            );
        } else {
            warn!(
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

    if args.prewarm_indexes {
        prewarm_sort_indexes(&state).await;
    }

    if let (Ok(cert), Ok(key)) = (std::env::var("DNADB_TLS_CERT"), std::env::var("DNADB_TLS_KEY")) {
        let tls = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert.clone(), key.clone()).await?;
        info!(
            "dnadb_http listening on https://{}  (data_dir={}, tls_cert={cert}, tls_key={key})",
            args.bind,
            args.data_dir.display()
        );
        axum_server::bind_rustls(args.bind.parse()?, tls)
            .serve(app.into_make_service())
            .await?;
    } else {
        let listener = tokio::net::TcpListener::bind(&args.bind).await?;
        info!(
            "dnadb_http listening on http://{}  (data_dir={})",
            args.bind,
            args.data_dir.display()
        );
        axum::serve(listener, app).await?;
    }
    Ok(())
}

async fn health() -> Json<Value> {
    Json(json!({ "ok": true, "service": "dnadb_http" }))
}

async fn metrics(State(state): State<AppState>) -> Response {
    let m = &state.metrics;
    let mut out = String::new();
    out.push_str("# TYPE dnadb_http_requests_total counter\n");
    out.push_str(&format!(
        "dnadb_http_requests_total {}\n",
        m.requests_total.load(Ordering::Relaxed)
    ));
    out.push_str("# TYPE dnadb_http_errors_total counter\n");
    out.push_str(&format!(
        "dnadb_http_errors_total {}\n",
        m.errors_total.load(Ordering::Relaxed)
    ));
    out.push_str("# TYPE dnadb_http_overload_rejections_total counter\n");
    out.push_str(&format!(
        "dnadb_http_overload_rejections_total {}\n",
        m.overload_rejections_total.load(Ordering::Relaxed)
    ));
    out.push_str("# TYPE dnadb_http_inflight_requests gauge\n");
    let inflight = m.inflight_requests.load(Ordering::Relaxed).saturating_sub(1);
    out.push_str(&format!("dnadb_http_inflight_requests {}\n", inflight));
    out.push_str("# TYPE dnadb_http_slow_queries_total counter\n");
    out.push_str(&format!(
        "dnadb_http_slow_queries_total {}\n",
        m.slow_queries_total.load(Ordering::Relaxed)
    ));
    out.push_str("# TYPE dnadb_http_request_duration_ms_total counter\n");
    out.push_str(&format!(
        "dnadb_http_request_duration_ms_total {}\n",
        m.request_duration_ms_total.load(Ordering::Relaxed)
    ));
    out.push_str("# TYPE dnadb_http_request_duration_count counter\n");
    out.push_str(&format!(
        "dnadb_http_request_duration_count {}\n",
        m.request_duration_count.load(Ordering::Relaxed)
    ));
    out.push_str("# TYPE dnadb_http_request_duration_ms_bucket counter\n");
    let mut running = 0u64;
    for (idx, bucket) in LATENCY_BUCKETS_MS.iter().enumerate() {
        running = running.saturating_add(m.latency_bucket_counts[idx].load(Ordering::Relaxed));
        out.push_str(&format!(
            "dnadb_http_request_duration_ms_bucket{{le=\"{}\"}} {}\n",
            bucket, running
        ));
    }
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        out,
    )
        .into_response()
}

async fn request_concurrency_middleware(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    let start = Instant::now();
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let permit = match state.request_slots.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            state
                .metrics
                .overload_rejections_total
                .fetch_add(1, Ordering::Relaxed);
            state
                .metrics
                .observe_request(StatusCode::SERVICE_UNAVAILABLE, Duration::from_millis(0));
            warn!(method = %method, path = %path, "http overload rejection");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"ok": false, "error": "server overloaded; retry later"})),
            )
                .into_response();
        }
    };
    state.metrics.inflight_requests.fetch_add(1, Ordering::Relaxed);
    let resp = next.run(req).await;
    let status = resp.status();
    let elapsed = start.elapsed();
    state.metrics.observe_request(status, elapsed);
    state.metrics.inflight_requests.fetch_sub(1, Ordering::Relaxed);
    drop(permit);
    info!(
        method = %method,
        path = %path,
        status = status.as_u16(),
        elapsed_ms = elapsed.as_millis() as u64,
        "http request"
    );
    resp
}

async fn insert_one(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(collection): AxumPath<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let _ = resolve_overlay(&state, &headers).await?;
    let t0 = Instant::now();
    let mut rt = state.rt.lock().await;
    let cmd = MongoCommand::InsertOne(MongoInsertOneCommand {
        collection: collection.clone(),
        document: body,
    });
    let out = rt.execute_mongo_command(cmd).map_err(ApiError::runtime)?;
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    match out {
        ExecutionResult::AffectedRows(n) => Ok(Json(json!({ "ok": true, "affected": n, "ms": ms }))),
        other => Err(ApiError::bad_request(format!("unexpected result: {other:?}"))),
    }
}

/// Strand WAL durability policy for this bulk request (explicit contract vs implicit flags).
///
/// - **`strict`** — strand/complement/meta files `sync_data` before response returns (safest per request).
/// - **`deferred`** — mmap flush + WAL fsync only; strand files catch up on index rebuild or `POST …/storage/sync`.
/// - **`batch`** — same strand semantics as `deferred` today; reserved for automatic group-commit windows (like PostgreSQL).
#[derive(Debug, Clone, Copy, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
enum DurabilityMode {
    Strict,
    Batch,
    Deferred,
}

#[derive(serde::Deserialize)]
struct BulkBody {
    documents: Vec<Value>,
    #[serde(default)]
    defer_reindex: bool,
    /// Legacy; ignored when `durability` is set.
    #[serde(default)]
    defer_storage_fsync: Option<bool>,
    /// Prefer this over inferring from `defer_reindex` / `defer_storage_fsync`.
    #[serde(default)]
    durability: Option<DurabilityMode>,
}

fn resolve_bulk_strand_durability(body: &BulkBody) -> (bool, DurabilityMode) {
    if let Some(mode) = body.durability {
        let defer_strand_sync = !matches!(mode, DurabilityMode::Strict);
        return (defer_strand_sync, mode);
    }
    let defer_strand_sync = body
        .defer_storage_fsync
        .unwrap_or(body.defer_reindex);
    let legacy_mode = if defer_strand_sync {
        DurabilityMode::Deferred
    } else {
        DurabilityMode::Strict
    };
    (defer_strand_sync, legacy_mode)
}

async fn bulk_insert(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(collection): AxumPath<String>,
    Json(body): Json<BulkBody>,
) -> Result<Json<Value>, ApiError> {
    let _ = resolve_overlay(&state, &headers).await?;
    let max_batch = std::env::var("DNADB_HTTP_MAX_BULK")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(10_000);
    if body.documents.len() > max_batch {
        return Err(ApiError::bad_request(format!(
            "batch too large (max {max_batch})"
        )));
    }
    let batch_size = body.documents.len();

    let (defer_storage_fsync, durability_mode) = resolve_bulk_strand_durability(&body);
    let t0 = Instant::now();
    let mut rt = state.rt.lock().await;
    let affected = match rt
        .execute_mongo_insert_many_with_mode(
            &collection,
            body.documents,
            !body.defer_reindex,
            defer_storage_fsync,
        )
        .map_err(ApiError::runtime)?
    {
        ExecutionResult::AffectedRows(n) => n,
        other => return Err(ApiError::bad_request(format!("unexpected result: {other:?}"))),
    };
    let ms = t0.elapsed().as_secs_f64() * 1000.0;

    let mut durability_json = serde_json::json!({
        "mode": durability_mode,
        "strand_files_synced_before_response": !defer_storage_fsync,
        "wal_synced_before_response": true,
        "legacy_infer_used": body.durability.is_none(),
    });
    if matches!(durability_mode, DurabilityMode::Batch) {
        durability_json["implementation_note"] = serde_json::json!(
            "batch currently matches deferred strand fsync; automatic group-commit window is not wired yet (see group_commit.rs)"
        );
    }

    Ok(Json(
        json!({
            "ok": true,
            "affected": affected,
            "ms": ms,
            "batch_size": batch_size,
            "write_path": "durable",
            "defer_reindex": body.defer_reindex,
            "defer_storage_fsync": defer_storage_fsync,
            "durability": durability_json,
        }),
    ))
}

async fn sync_collection_storage(
    State(state): State<AppState>,
    AxumPath(collection): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    let t0 = Instant::now();
    let mut rt = state.rt.lock().await;
    rt.sync_collection_storage(&collection).map_err(ApiError::runtime)?;
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    Ok(Json(json!({ "ok": true, "collection": collection, "ms": ms })))
}

async fn rebuild_indexes(
    State(state): State<AppState>,
    AxumPath(collection): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    let t0 = Instant::now();
    let mut rt = state.rt.lock().await;
    rt.rebuild_indexes(&collection).map_err(ApiError::runtime)?;
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    Ok(Json(json!({ "ok": true, "collection": collection, "ms": ms })))
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
    headers: HeaderMap,
    AxumPath(collection): AxumPath<String>,
    Json(body): Json<QueryBody>,
) -> Result<Json<Value>, ApiError> {
    let filter = body.filter.clone();
    let sort = body.sort.clone();
    let limit = body.limit;
    let overlay = resolve_overlay(&state, &headers).await?;
    let t0 = Instant::now();
    let mut rt = state.rt.lock().await;
    let cmd = MongoCommand::Find(MongoFindCommand {
        collection: collection.clone(),
        filter: body.filter,
        sort: body.sort,
        limit: body.limit,
        include_paths: vec![],
    });
    let out = rt.execute_mongo_command(cmd).map_err(ApiError::runtime)?;
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    if ms > state.slow_query_ms as f64 {
        state
            .metrics
            .slow_queries_total
            .fetch_add(1, Ordering::Relaxed);
        let filter_json = Value::Object(filter.clone()).to_string();
        warn!(
            collection = %collection,
            duration_ms = ms,
            filter = %filter_json,
            sort = ?sort,
            limit = ?limit,
            "slow query"
        );
    }
    match out {
        ExecutionResult::QueryRows(rows) => {
            let rows = if let Some(ov) = overlay.as_ref() {
                mask_records_for_overlay(&collection, &rows, ov).map_err(ApiError::privacy)?
            } else {
                rows
            };
            let rows: Vec<Value> = rows.into_iter().map(|m| Value::Object(m.into_iter().collect())).collect();
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
    #[serde(default)]
    composite_sort_indexes: Vec<CompositeSortIndexSpec>,
    /// If set (including empty `[]`), replaces exact-string index fields for this collection.
    /// If omitted, exact-string fields are left unchanged.
    #[serde(default)]
    exact_string_fields: Option<Vec<String>>,
}

#[derive(serde::Deserialize)]
struct CollectionConfigureBody {
    #[serde(default)]
    sort_indexes: Vec<String>,
    #[serde(default)]
    composite_sort_indexes: Vec<CompositeSortIndexSpec>,
    /// If set (including empty `[]`), replaces exact-string index fields for this collection.
    /// If omitted, exact-string fields are left unchanged.
    #[serde(default)]
    exact_string_fields: Option<Vec<String>>,
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

#[derive(serde::Deserialize)]
struct CompositeSortIndexSpec {
    fields: Vec<String>,
}

async fn get_sort_indexes(
    State(state): State<AppState>,
    AxumPath(collection): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    let mut rt = state.rt.lock().await;
    let fields = rt
        .sort_index_fields(&collection)
        .map_err(ApiError::runtime)?;
    let exact = rt
        .exact_string_index_fields(&collection)
        .map_err(ApiError::runtime)?;
    Ok(Json(
        json!({ "ok": true, "collection": collection, "sort_indexes": fields, "exact_string_fields": exact }),
    ))
}

async fn get_index_status(
    State(state): State<AppState>,
    AxumPath(collection): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    let mut rt = state.rt.lock().await;
    let (fields, composites, exact_fields, state_name) = rt
        .sort_index_status(&collection)
        .map_err(ApiError::runtime)?;
    let composites: Vec<Value> = composites
        .into_iter()
        .map(|(a, b)| json!({ "fields": [a, b] }))
        .collect();
    Ok(Json(json!({
        "ok": true,
        "collection": collection,
        "indexes": {
            "sort": {
                "state": state_name,
                "fields": fields,
                "composite_fields": composites,
                "durability": "config_persisted_rebuild_on_startup"
            },
            "exact_string": {
                "fields": exact_fields,
                "durability": "config_persisted_rebuild_on_startup"
            }
        }
    })))
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
    let composite_defs: Vec<(String, String)> = body
        .composite_sort_indexes
        .into_iter()
        .filter_map(|spec| {
            if spec.fields.len() != 2 {
                return None;
            }
            let a = spec.fields[0].trim().to_string();
            let b = spec.fields[1].trim().to_string();
            if a.is_empty() || b.is_empty() {
                None
            } else {
                Some((a, b))
            }
        })
        .collect();
    let composites = rt
        .configure_composite_sort_indexes(&collection, &composite_defs)
        .map_err(ApiError::runtime)?;
    let exact_string_fields = if let Some(ref ef) = body.exact_string_fields {
        rt.configure_exact_string_index_fields(&collection, ef)
            .map_err(ApiError::runtime)?
    } else {
        rt.exact_string_index_fields(&collection)
            .map_err(ApiError::runtime)?
    };
    let composites_json: Vec<Value> = composites
        .into_iter()
        .map(|(a, b)| json!({ "fields": [a, b] }))
        .collect();
    Ok(Json(
        json!({
            "ok": true,
            "collection": collection,
            "sort_indexes": active,
            "composite_sort_indexes": composites_json,
            "exact_string_fields": exact_string_fields,
            "configured": true
        }),
    ))
}

async fn configure_collection(
    State(state): State<AppState>,
    AxumPath(collection): AxumPath<String>,
    Json(body): Json<CollectionConfigureBody>,
) -> Result<Json<Value>, ApiError> {
    let fields: Vec<String> = body
        .sort_indexes
        .into_iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let composite_defs: Vec<(String, String)> = body
        .composite_sort_indexes
        .into_iter()
        .filter_map(|spec| {
            if spec.fields.len() != 2 {
                return None;
            }
            let a = spec.fields[0].trim().to_string();
            let b = spec.fields[1].trim().to_string();
            if a.is_empty() || b.is_empty() {
                None
            } else {
                Some((a, b))
            }
        })
        .collect();

    let mut rt = state.rt.lock().await;
    let active = rt
        .configure_sort_indexes(&collection, &fields)
        .map_err(ApiError::runtime)?;
    let composites = rt
        .configure_composite_sort_indexes(&collection, &composite_defs)
        .map_err(ApiError::runtime)?;
    let exact_string_fields = if let Some(ref ef) = body.exact_string_fields {
        rt.configure_exact_string_index_fields(&collection, ef)
            .map_err(ApiError::runtime)?
    } else {
        rt.exact_string_index_fields(&collection)
            .map_err(ApiError::runtime)?
    };
    let composites_json: Vec<Value> = composites
        .into_iter()
        .map(|(a, b)| json!({ "fields": [a, b] }))
        .collect();
    Ok(Json(
        json!({
            "ok": true,
            "collection": collection,
            "sort_indexes": active,
            "composite_sort_indexes": composites_json,
            "exact_string_fields": exact_string_fields,
            "configured": true
        }),
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

async fn configure_composite_sort_indexes(
    State(state): State<AppState>,
    AxumPath(collection): AxumPath<String>,
    Json(body): Json<SortIndexConfigBody>,
) -> Result<Json<Value>, ApiError> {
    let composite_defs: Vec<(String, String)> = body
        .composite_sort_indexes
        .into_iter()
        .filter_map(|spec| {
            if spec.fields.len() != 2 {
                return None;
            }
            let a = spec.fields[0].trim().to_string();
            let b = spec.fields[1].trim().to_string();
            if a.is_empty() || b.is_empty() {
                None
            } else {
                Some((a, b))
            }
        })
        .collect();
    let mut rt = state.rt.lock().await;
    let composites = rt
        .configure_composite_sort_indexes(&collection, &composite_defs)
        .map_err(ApiError::runtime)?;
    let composites_json: Vec<Value> = composites
        .into_iter()
        .map(|(a, b)| json!({ "fields": [a, b] }))
        .collect();
    Ok(Json(
        json!({ "ok": true, "collection": collection, "composite_sort_indexes": composites_json }),
    ))
}

#[derive(Debug)]
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

    fn unauthorized(message: String) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message,
        }
    }

    fn privacy(e: PrivacyError) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
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

async fn resolve_overlay(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<Option<ResolvedOverlay>, ApiError> {
    if !state.auth_required {
        return Ok(None);
    }
    let authz = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| ApiError::unauthorized("unauthorized".to_string()))?;
    let token = authz
        .strip_prefix("Bearer ")
        .or_else(|| authz.strip_prefix("bearer "))
        .ok_or_else(|| ApiError::unauthorized("unauthorized".to_string()))?;
    let auth = state.auth.lock().await;
    let (.., overlay) = state
        .overlays
        .resolve_for_session(&auth, token)
        .map_err(|_| ApiError::unauthorized("unauthorized".to_string()))?;
    Ok(Some(overlay))
}

async fn prewarm_sort_indexes(state: &AppState) {
    let collections = {
        let rt = state.rt.lock().await;
        rt.list_known_collections()
    };
    if collections.is_empty() {
        info!("dnadb_http prewarm: no collections found");
        return;
    }
    let mut warmed = 0usize;
    for collection in collections {
        let fields = {
            let mut rt = state.rt.lock().await;
            rt.sort_index_fields(&collection).unwrap_or_default()
        };
        if fields.is_empty() {
            continue;
        }
        for field in fields {
            let mut sort = Map::new();
            sort.insert(field.clone(), json!(-1));
            let cmd = MongoCommand::Find(MongoFindCommand {
                collection: collection.clone(),
                filter: Map::new(),
                sort: Some(sort),
                limit: Some(50),
                include_paths: vec![],
            });
            let _ = {
                let mut rt = state.rt.lock().await;
                rt.execute_mongo_command(cmd)
            };
            warmed = warmed.saturating_add(1);
            info!("dnadb_http prewarm: touched {collection}.{field}");
        }
    }
    info!("dnadb_http prewarm: completed {warmed} index probes");
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn make_test_state(auth_required: bool) -> (AppState, String, String) {
        let dir = tempdir().expect("tempdir").keep();
        let rt = EngineRuntime::open(dir.as_path(), Some(1024 * 1024));
        let mut auth_store = IdentityStore::new();
        let mut overlays = OverlayRegistry::new();
        overlays.define_overlay(OverlayDefinition::full("full"));
        overlays.define_overlay(OverlayDefinition {
            name: "redacted".to_string(),
            access: OverlayAccess::Partial,
            collections: vec!["posts".to_string()],
            include_fields: {
                let mut m = std::collections::HashMap::new();
                m.insert(
                    "posts".to_string(),
                    vec!["id".to_string(), "title".to_string(), "status".to_string()],
                );
                m
            },
            exclude_fields: {
                let mut m = std::collections::HashMap::new();
                m.insert(
                    "posts".to_string(),
                    vec!["body".to_string(), "slug".to_string(), "author_email".to_string()],
                );
                m
            },
            mutations: vec![OverlayMutation::Read],
            extends: None,
            additionally_include: std::collections::HashMap::new(),
        });
        auth_store
            .create_identity(CreateIdentityRequest {
                name: "admin".to_string(),
                identity_type: IdentityType::Admin,
                overlay: "full".to_string(),
                allowed_collections: vec!["*".to_string()],
                token_expiry_seconds: 86_400,
                mfa_required: false,
                password: None,
                api_key: Some("admin-key".to_string()),
            })
            .expect("admin id");
        auth_store
            .create_identity(CreateIdentityRequest {
                name: "redacted-user".to_string(),
                identity_type: IdentityType::ReadOnly,
                overlay: "redacted".to_string(),
                allowed_collections: vec!["posts".to_string()],
                token_expiry_seconds: 86_400,
                mfa_required: false,
                password: None,
                api_key: Some("redacted-key".to_string()),
            })
            .expect("redacted id");
        let admin_token = auth_store
            .authenticate_with_api_key("admin", "admin-key")
            .expect("admin token")
            .token;
        let redacted_token = auth_store
            .authenticate_with_api_key("redacted-user", "redacted-key")
            .expect("redacted token")
            .token;
        (
            AppState {
                rt: std::sync::Arc::new(Mutex::new(rt)),
                auth_required,
                auth: std::sync::Arc::new(Mutex::new(auth_store)),
                overlays: std::sync::Arc::new(overlays),
            },
            admin_token,
            redacted_token,
        )
    }

    #[tokio::test]
    async fn unauthenticated_request_rejected() {
        let (state, _, _) = make_test_state(true);
        let headers = HeaderMap::new();
        let result = query_find(
            State(state),
            headers,
            AxumPath("posts".to_string()),
            Json(QueryBody {
                filter: Map::new(),
                limit: Some(1),
                sort: None,
            }),
        )
        .await;
        let err = result.expect_err("must reject");
        assert_eq!(err.status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn overlay_strips_excluded_fields_through_http() {
        let (state, admin_token, redacted_token) = make_test_state(true);
        {
            let mut rt = state.rt.lock().await;
            let _ = rt
                .execute_mongo_insert_many(
                    "posts",
                    vec![json!({
                        "id": 1,
                        "title": "Test",
                        "status": "published",
                        "body": "full body text",
                        "slug": "post-1",
                        "author_email": "private@example.com"
                    })],
                )
                .expect("insert");
        }

        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            format!("Bearer {redacted_token}").parse().expect("header"),
        );
        let redacted = query_find(
            State(state.clone()),
            headers,
            AxumPath("posts".to_string()),
            Json(QueryBody {
                filter: Map::new(),
                limit: Some(1),
                sort: None,
            }),
        )
        .await
        .expect("redacted query");
        let rows = redacted
            .0
            .get("rows")
            .and_then(|v| v.as_array())
            .expect("rows");
        let row = rows.first().and_then(|v| v.as_object()).expect("row");
        assert_eq!(row.get("id"), Some(&json!(1)));
        assert_eq!(row.get("title"), Some(&json!("Test")));
        assert!(!row.contains_key("body"));
        assert!(!row.contains_key("author_email"));

        let mut admin_headers = HeaderMap::new();
        admin_headers.insert(
            "authorization",
            format!("Bearer {admin_token}").parse().expect("header"),
        );
        let full = query_find(
            State(state),
            admin_headers,
            AxumPath("posts".to_string()),
            Json(QueryBody {
                filter: Map::new(),
                limit: Some(1),
                sort: None,
            }),
        )
        .await
        .expect("admin query");
        let full_rows = full
            .0
            .get("rows")
            .and_then(|v| v.as_array())
            .expect("rows");
        let full_row = full_rows.first().and_then(|v| v.as_object()).expect("row");
        assert!(full_row.get("body").and_then(|v| v.as_str()).is_some());
        assert!(full_row.get("author_email").and_then(|v| v.as_str()).is_some());
    }
}
