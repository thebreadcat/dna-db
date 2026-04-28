//! Minimal HTTP surface over `EngineRuntime` for local apps (CMS lab, scripts).
//!
//! Build: `cargo build --release --features http_server --bin dnadb_http`
//!
//! Backpressure: set `DNADB_RAW_MAX_PENDING_WAL` to cap raw-journal lag; `DNADB_RAW_CRITICAL_PENDING` for a
//! stricter halt (503). Adaptive materialize: `batch_records == 0` in the engine, or omit
//! `DNADB_RAW_MATERIALIZE_BATCH` / `materialize_batch_size`. See `DNADB_RAW_TARGET_PENDING_SEQUENCES`,
//! `DNADB_RAW_MATERIALIZE_IDLE_MS`, and related env in the runtime.

use std::path::PathBuf;
use std::time::{Duration, Instant};

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

    /// If set, periodically run `materialize_raw_journal_to_durable` for these collections (comma-separated names).
    /// Use with `--raw-materialize-interval-ms`.
    #[arg(long, value_delimiter = ',')]
    raw_materialize_collections: Vec<String>,

    /// Wall-clock interval (ms) for background materialization when `raw_materialize_collections` is non-empty.
    #[arg(long)]
    raw_materialize_interval_ms: Option<u64>,

    /// Batch size for each background materialize tick; omit for adaptive (lag-based), or set `DNADB_RAW_MATERIALIZE_BATCH`.
    #[arg(long)]
    raw_materialize_batch: Option<usize>,
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
            "/collections/:collection/storage/sync",
            post(sync_collection_storage),
        )
        .route(
            "/collections/:collection/raw-materialize",
            post(raw_materialize),
        )
        .route(
            "/collections/:collection/raw-materialize/status",
            get(raw_materialize_status),
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

    if args.raw_materialize_interval_ms.is_some() && args.raw_materialize_collections.is_empty() {
        eprintln!(
            "warning: --raw-materialize-interval-ms set without --raw-materialize-collections; skipping background materializer"
        );
    } else if !args.raw_materialize_collections.is_empty() {
        let state_bg = state.clone();
        let interval_ms = args
            .raw_materialize_interval_ms
            .or_else(|| {
                std::env::var("DNADB_RAW_MATERIALIZE_INTERVAL_MS")
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
            })
            .unwrap_or(10)
            .max(1);
            let batch = args
                .raw_materialize_batch
                .or_else(|| {
                    std::env::var("DNADB_RAW_MATERIALIZE_BATCH")
                        .ok()
                        .and_then(|s| s.parse::<usize>().ok())
                })
                .unwrap_or(512)
                .max(1);
            let max_batches_per_tick = std::env::var("DNADB_RAW_MATERIALIZE_MAX_BATCHES_PER_TICK")
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(4)
                .max(1);
            let idle_ms = std::env::var("DNADB_RAW_MATERIALIZE_IDLE_MS")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(2_000)
                .max(1);
            let collections: Vec<String> = args
                .raw_materialize_collections
                .iter()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            eprintln!(
                "background materialize: collections={collections:?} active={interval_ms}ms idle={idle_ms}ms batch={batch} max_batches_per_tick={max_batches_per_tick}"
            );
        tokio::spawn(async move {
            loop {
                    let mut max_lag = 0u64;
                    for c in &collections {
                        let mut applied_total = 0usize;
                        let mut last_applied = None::<u64>;
                        for _ in 0..max_batches_per_tick {
                            let apply = {
                                let mut rt = state_bg.rt.lock().await;
                                rt.materialize_raw_journal_apply_only_bounded(c, batch, 1)
                            };
                            match apply {
                                Ok(r) => {
                                    if r.records_applied == 0 {
                                        break;
                                    }
                                    applied_total = applied_total.saturating_add(r.records_applied);
                                    last_applied = Some(r.last_applied_wal_sequence);
                            }
                                Err(e) => {
                                    eprintln!("raw_materialize apply error: collection={c} {e}");
                                    break;
                                }
                            }
                        }
                        if applied_total > 0 {
                            if let Ok(mut rt) = state_bg.rt.try_lock() {
                                if let Err(e) = rt.sync_collection_storage(c) {
                                    eprintln!("raw_materialize sync error: collection={c} {e}");
                                }
                            }
                            eprintln!(
                                "raw_materialize: collection={c} applied={applied_total} through_seq={}",
                                last_applied.unwrap_or(0)
                            );
                        }
                        let lag = {
                            let mut rt = state_bg.rt.lock().await;
                            rt.raw_journal_pending_sequences(c).unwrap_or(0)
                        };
                        max_lag = max_lag.max(lag);
                    }
                    let target = std::env::var("DNADB_RAW_TARGET_PENDING_SEQUENCES")
                        .ok()
                        .and_then(|s| s.parse::<u64>().ok())
                        .filter(|&n| n > 0);
                    let mut sleep_ms = if max_lag == 0 {
                        idle_ms
                    } else {
                        interval_ms
                    };
                    if let Some(t) = target {
                        if max_lag > t {
                            sleep_ms = (interval_ms / 4).max(10);
                        }
                    }
                tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
            }
        });
    }

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

/// Which write engine handles the batch. `durable` = MVCC + strands; `raw_segment` = append-only journal (no query yet).
#[derive(Default, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum BulkWritePath {
    #[default]
    Durable,
    RawSegment,
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
    #[serde(default)]
    write_path: BulkWritePath,
    /// After `write_path: raw_segment`, run WAL→durable materialization before responding (dev convenience).
    #[serde(default)]
    auto_materialize: bool,
    #[serde(default)]
    materialize_batch_size: Option<usize>,
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
    AxumPath(collection): AxumPath<String>,
    Json(body): Json<BulkBody>,
) -> Result<Json<Value>, ApiError> {
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

    if matches!(body.write_path, BulkWritePath::RawSegment) {
        let mat_batch = body
            .materialize_batch_size
            .or_else(|| {
                std::env::var("DNADB_RAW_MATERIALIZE_BATCH")
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
            })
            .unwrap_or(0);
        let t0 = Instant::now();
        let mut rt = state.rt.lock().await;
        let affected = rt
            .execute_raw_segment_bulk_insert(&collection, body.documents)
            .map_err(ApiError::runtime)?;
        let materialize = if body.auto_materialize {
            Some(
                rt.materialize_raw_journal_to_durable(&collection, mat_batch)
                    .map_err(ApiError::runtime)?,
            )
        } else {
            None
        };
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        let mvcc_ok = materialize.as_ref().map_or(false, |m| {
            m.raw_wal_high_water_sequence <= m.last_applied_wal_sequence
        });
        return Ok(Json(json!({
            "ok": true,
            "affected": affected,
            "ms": ms,
            "batch_size": batch_size,
            "write_path": "raw_segment",
            "mvcc_queryable": mvcc_ok,
            "auto_materialize": body.auto_materialize,
            "materialize": materialize,
            "note": if materialize.is_some() {
                "Raw ingest + materialize; rows are in the durable store — overlays apply on query."
            } else {
                "Stored under data_dir/raw_journal/<collection>/ only; POST /api/.../raw-materialize or auto_materialize:true for MVCC visibility."
            },
        })));
    }

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

#[derive(serde::Deserialize)]
struct RawMaterializeBody {
    #[serde(default)]
    batch_size: Option<usize>,
}

async fn raw_materialize(
    State(state): State<AppState>,
    AxumPath(collection): AxumPath<String>,
    body: Option<Json<RawMaterializeBody>>,
) -> Result<Json<Value>, ApiError> {
    let batch_size = body
        .and_then(|Json(b)| b.batch_size)
        .or_else(|| {
            std::env::var("DNADB_RAW_MATERIALIZE_BATCH")
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
        })
        .unwrap_or(0);
    let t0 = Instant::now();
    let mut rt = state.rt.lock().await;
    let r = rt
        .materialize_raw_journal_to_durable(&collection, batch_size)
        .map_err(ApiError::runtime)?;
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    Ok(Json(json!({
        "ok": true,
        "collection": collection,
        "ms": ms,
        "records_applied": r.records_applied,
        "last_applied_wal_sequence": r.last_applied_wal_sequence,
        "batches": r.batches,
        "raw_wal_high_water_sequence": r.raw_wal_high_water_sequence,
        "adaptive_batch": r.adaptive_batch,
        "materialization_records_per_sec": r.materialization_records_per_sec,
        "note": "WAL-ordered upserts into the durable store; query is now consistent for materialized keys (overlays still apply on read)."
    })))
}

async fn raw_materialize_status(
    State(state): State<AppState>,
    AxumPath(collection): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    let mut rt = state.rt.lock().await;
    let (st, hw) = rt
        .raw_journal_materialization_status(&collection)
        .map_err(ApiError::runtime)?;
    let pending = hw.saturating_sub(st.last_applied_wal_sequence);
    let suggested_batch = rt
        .adaptive_materialize_batch_preview(&collection)
        .map_err(ApiError::runtime)?;
    let mat_rps_from_state = match (st.last_materialize_records, st.last_materialize_duration_ms) {
        (Some(n), Some(dms)) if dms > 0 => Some((n as f64) / (dms as f64 / 1000.0)),
        _ => None,
    };
    let lag_sec_est = mat_rps_from_state
        .filter(|&r| r > 0.0)
        .map(|r| pending as f64 / r);
    let ingest_total = rt.raw_ingest_total_documents(&collection);
    let ingest_rps = rt.raw_ingest_documents_per_sec_estimate(&collection);
    let target_pending = std::env::var("DNADB_RAW_TARGET_PENDING_SEQUENCES")
        .ok()
        .and_then(|s| s.parse::<u64>().ok());
    let target_lag_ms = std::env::var("DNADB_RAW_TARGET_LAG_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok());
    Ok(Json(
        json!({
            "ok": true,
            "collection": collection,
            "last_applied_wal_sequence": st.last_applied_wal_sequence,
            "raw_wal_high_water_sequence": hw,
            "pending_wal_sequences": pending,
            "suggested_materialize_batch_records": suggested_batch,
            "materialization_records_per_sec_last_run": mat_rps_from_state,
            "lag_seconds_estimate": lag_sec_est,
            "raw_ingest_total_documents": ingest_total,
            "ingest_documents_per_sec_estimate": ingest_rps,
            "env": {
                "DNADB_RAW_TARGET_PENDING_SEQUENCES": target_pending,
                "DNADB_RAW_TARGET_LAG_MS": target_lag_ms,
            }
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
        let status = match &e {
            dnadb_engine::runtime::RuntimeError::RawSegment(_) => StatusCode::BAD_REQUEST,
            dnadb_engine::runtime::RuntimeError::RawIngestCriticalLag { .. } => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            dnadb_engine::runtime::RuntimeError::RawIngestBackpressure { .. } => {
                StatusCode::TOO_MANY_REQUESTS
            }
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self {
            status,
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
