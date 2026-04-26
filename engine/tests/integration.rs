use std::collections::HashMap;

use dnadb_engine::auth::{CreateIdentityRequest, IdentityStore, IdentityType};
use dnadb_engine::codec::{BincodeStrandCodec, StrandCodec, STRAND_FORMAT_MAGIC};
use dnadb_engine::encoding::encode_bytes_to_codons;
use dnadb_engine::overlay::{OverlayAccess, OverlayDefinition, OverlayMutation, OverlayRegistry};
use dnadb_engine::privacy::{mask_record_for_overlay, Record};
use dnadb_engine::processor::{process_wal_entry_with_mode, PersistMode};
use dnadb_engine::query::fetch::fetch_one;
use dnadb_engine::query::{Clause, GuidePattern, ScanConfig};
use dnadb_engine::recovery::replay_pending_wal_after_open;
use dnadb_engine::storage::CollectionStorage;
use dnadb_engine::transaction::TransactionManager;
use dnadb_engine::transaction_durable::DurableTransactionStore;
use dnadb_engine::wal::Wal;
use serde_json::json;
use tempfile::tempdir;

fn decode_all_strands(storage: &CollectionStorage) -> Vec<dnadb_engine::model::Strand> {
    let bytes = std::fs::read(&storage.paths.strands).expect("read strands");
    let codec = BincodeStrandCodec;
    let mut out = Vec::new();
    let mut pos = 0usize;
    while pos + 6 <= bytes.len() && bytes[pos..pos + 4] == STRAND_FORMAT_MAGIC {
        let s = codec.decode_strand(&bytes[pos..]).expect("decode strand");
        pos += codec.encode_strand(&s).expect("encode size").len();
        out.push(s);
    }
    out
}

#[test]
fn full_stack_insert_and_query() {
    let dir = tempdir().expect("tempdir");
    let root = dir.path();
    let mut wal = Wal::open_or_create(root, "users").expect("open wal");
    let mut storage =
        CollectionStorage::open_or_create(root, "users", Some(8 * 1024 * 1024)).expect("open storage");
    let codec = BincodeStrandCodec;

    let payload = br#"{"id":1,"name":"Alice","email":"alice@x.com","age":28}"#;
    let seq = wal.append(payload).expect("append");
    process_wal_entry_with_mode(
        &mut storage,
        &codec,
        1,
        seq,
        payload,
        PersistMode::Buffered,
    )
    .expect("process");
    storage.flush().expect("flush");
    wal.sync().expect("sync wal");

    let reopened = CollectionStorage::open_or_create(root, "users", Some(8 * 1024 * 1024))
        .expect("reopen storage");
    let strands = decode_all_strands(&reopened);
    let pattern = GuidePattern {
        collection: "users".into(),
        clauses: vec![Clause::ExactMatch {
            field_intron: "_payload".into(),
            operand_wire: payload.to_vec(),
            operand_codons: encode_bytes_to_codons(payload),
        }],
        includes: vec![],
        overlay: None,
        order_by: None,
        limit: Some(1),
    };

    let found = fetch_one(
        &pattern,
        &strands,
        &ScanConfig {
            collection_id: Some(1),
            ..ScanConfig::default()
        },
    );
    assert!(found.is_some(), "expected inserted payload to be queryable");
}

#[test]
fn overlay_enforced_end_to_end() {
    let mut auth = IdentityStore::new();
    let mut overlays = OverlayRegistry::new();

    overlays.define_overlay(OverlayDefinition::full("admin"));
    let mut include_fields = HashMap::new();
    include_fields.insert("users".to_string(), vec!["id".to_string(), "name".to_string()]);
    overlays.define_overlay(OverlayDefinition {
        name: "restricted".into(),
        access: OverlayAccess::Partial,
        collections: vec!["users".into()],
        include_fields,
        exclude_fields: HashMap::new(),
        mutations: vec![OverlayMutation::Read],
        extends: None,
        additionally_include: HashMap::new(),
    });

    auth.create_identity(CreateIdentityRequest {
        name: "admin-user".into(),
        identity_type: IdentityType::Admin,
        overlay: "admin".into(),
        allowed_collections: vec!["users".into()],
        token_expiry_seconds: 3600,
        mfa_required: false,
        password: Some("admin-pass".into()),
        api_key: None,
    })
    .expect("create admin");
    auth.create_identity(CreateIdentityRequest {
        name: "restricted-user".into(),
        identity_type: IdentityType::ReadOnly,
        overlay: "restricted".into(),
        allowed_collections: vec!["users".into()],
        token_expiry_seconds: 3600,
        mfa_required: false,
        password: Some("restricted-pass".into()),
        api_key: None,
    })
    .expect("create restricted");

    let admin_token = auth
        .authenticate_with_password("admin-user", "admin-pass")
        .expect("admin auth");
    let restricted_token = auth
        .authenticate_with_password("restricted-user", "restricted-pass")
        .expect("restricted auth");
    let (_, admin_overlay) = overlays
        .resolve_for_session(&auth, &admin_token.token)
        .expect("resolve admin overlay");
    let (_, restricted_overlay) = overlays
        .resolve_for_session(&auth, &restricted_token.token)
        .expect("resolve restricted overlay");

    let mut record = Record::new();
    record.insert("id".into(), json!(1));
    record.insert("name".into(), json!("Bob"));
    record.insert("email".into(), json!("bob@x.com"));
    record.insert("ssn".into(), json!("123-45-6789"));

    let admin_view = mask_record_for_overlay("users", &record, &admin_overlay).expect("admin mask");
    let restricted_view =
        mask_record_for_overlay("users", &record, &restricted_overlay).expect("restricted mask");
    assert!(admin_view.get("ssn").is_some());
    assert!(restricted_view.get("name").is_some());
    assert!(restricted_view.get("id").is_some());
    assert!(restricted_view.get("email").is_none());
    assert!(restricted_view.get("ssn").is_none());
}

#[test]
fn crash_recovery_end_to_end() {
    let dir = tempdir().expect("tempdir");
    let root = dir.path();
    let mut wal = Wal::open_or_create(root, "users").expect("open wal");
    let mut storage =
        CollectionStorage::open_or_create(root, "users", Some(8 * 1024 * 1024)).expect("open storage");
    let codec = BincodeStrandCodec;

    // Write 3 entries to WAL but only materialize first 1 (simulated crash before full flush).
    let p1 = br#"{"id":1,"name":"u1"}"#;
    let p2 = br#"{"id":2,"name":"u2"}"#;
    let p3 = br#"{"id":3,"name":"u3"}"#;
    let s1 = wal.append(p1).expect("append 1");
    let _s2 = wal.append(p2).expect("append 2");
    let _s3 = wal.append(p3).expect("append 3");
    process_wal_entry_with_mode(&mut storage, &codec, 1, s1, p1, PersistMode::Buffered)
        .expect("materialize first");
    storage.flush().expect("flush first");
    wal.sync().expect("sync wal");
    drop(storage);
    drop(wal);

    let stats =
        replay_pending_wal_after_open(root, "users", 1, Some(8 * 1024 * 1024)).expect("recovery");
    assert_eq!(stats.pool_high_water_before, 1);
    assert_eq!(stats.replayed, 2);

    let reopened = CollectionStorage::open_or_create(root, "users", Some(8 * 1024 * 1024))
        .expect("reopen storage");
    let strands = decode_all_strands(&reopened);
    assert_eq!(strands.len(), 3, "all WAL entries should be materialized");
}

#[test]
fn mvcc_snapshot_isolation_end_to_end() {
    let mut tm = TransactionManager::new();

    let mut seed = tm.begin();
    let mut initial = HashMap::new();
    initial.insert("id".to_string(), json!(1));
    initial.insert("balance".to_string(), json!(100));
    tm.upsert(&mut seed, 1, initial);
    tm.commit(&mut seed).expect("seed commit");

    // Reader gets stable snapshot.
    let reader = tm.begin();

    let mut writer = tm.begin();
    let mut updated = HashMap::new();
    updated.insert("id".to_string(), json!(1));
    updated.insert("balance".to_string(), json!(50));
    tm.upsert(&mut writer, 1, updated);
    tm.commit(&mut writer).expect("writer commit");

    let old_view = tm.read(&reader, 1).expect("old reader view");
    assert_eq!(old_view.get("balance"), Some(&json!(100)));

    let new_reader = tm.begin();
    let new_view = tm.read(&new_reader, 1).expect("new reader view");
    assert_eq!(new_view.get("balance"), Some(&json!(50)));
}

#[test]
fn durable_transaction_commit_replays_after_reopen() {
    let dir = tempdir().expect("tempdir");
    let root = dir.path();

    {
        let mut store = DurableTransactionStore::open_or_create(root, "users", 1, Some(8 * 1024 * 1024))
            .expect("open durable store");
        let mut tx = store.begin();
        let mut row = HashMap::new();
        row.insert("id".to_string(), json!(11));
        row.insert("status".to_string(), json!("active"));
        store.upsert(&mut tx, 11, row);
        store.commit(&mut tx).expect("commit");
    }

    let mut reopened = DurableTransactionStore::open_or_create(root, "users", 1, Some(8 * 1024 * 1024))
        .expect("reopen durable store");
    let reader = reopened.begin();
    let row = reopened.read(&reader, 11).expect("replayed row");
    assert_eq!(row.get("status"), Some(&json!("active")));
}

