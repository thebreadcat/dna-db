//! Overlay-aware serialization / masking.
//!
//! Applies resolved overlay policy at serialization time so callers only see fields
//! permitted by engine-level privacy constraints.

use std::collections::HashMap;

use serde_json::Value;
use thiserror::Error;

use crate::overlay::ResolvedOverlay;

pub type Record = HashMap<String, Value>;

#[derive(Debug, Error)]
pub enum PrivacyError {
    #[error("collection is not accessible in overlay: {0}")]
    CollectionDenied(String),
}

/// Returns a masked clone of `record` according to overlay field visibility rules.
pub fn mask_record_for_overlay(
    collection: &str,
    record: &Record,
    overlay: &ResolvedOverlay,
) -> Result<Record, PrivacyError> {
    if !overlay.can_access_collection(collection) {
        return Err(PrivacyError::CollectionDenied(collection.to_string()));
    }

    let mut out = Record::new();
    for (k, v) in record {
        if overlay.is_field_visible(collection, k) {
            out.insert(k.clone(), v.clone());
        }
    }
    Ok(out)
}

/// Applies [`mask_record_for_overlay`] to each record in order.
pub fn mask_records_for_overlay(
    collection: &str,
    records: &[Record],
    overlay: &ResolvedOverlay,
) -> Result<Vec<Record>, PrivacyError> {
    records
        .iter()
        .map(|r| mask_record_for_overlay(collection, r, overlay))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use proptest::prelude::*;
    use serde_json::json;

    use crate::overlay::{
        OverlayAccess, OverlayDefinition, OverlayMutation, OverlayRegistry,
    };

    use super::{mask_record_for_overlay, mask_records_for_overlay, PrivacyError, Record};

    fn sample_record() -> Record {
        let mut r = HashMap::new();
        r.insert("id".into(), json!(1));
        r.insert("email".into(), json!("a@example.com"));
        r.insert("ssn".into(), json!("111-11-1111"));
        r
    }

    #[test]
    fn partial_overlay_masks_sensitive_fields() {
        let mut reg = OverlayRegistry::new();
        let mut include_fields = HashMap::new();
        include_fields.insert("users".into(), vec!["id".into(), "email".into()]);
        let mut exclude_fields = HashMap::new();
        exclude_fields.insert("users".into(), vec!["ssn".into()]);

        reg.define_overlay(OverlayDefinition {
            name: "support_agent".into(),
            access: OverlayAccess::Partial,
            collections: vec!["users".into()],
            include_fields,
            exclude_fields,
            mutations: vec![OverlayMutation::Read],
            extends: None,
            additionally_include: HashMap::new(),
        });

        let overlay = reg.resolve_overlay("support_agent").expect("overlay");
        let masked = mask_record_for_overlay("users", &sample_record(), &overlay).expect("mask");
        assert_eq!(masked.get("id").unwrap(), &json!(1));
        assert!(masked.contains_key("email"));
        assert!(!masked.contains_key("ssn"));
    }

    #[test]
    fn denies_collection_when_overlay_cannot_access() {
        let mut reg = OverlayRegistry::new();
        reg.define_overlay(OverlayDefinition {
            name: "public".into(),
            access: OverlayAccess::Partial,
            collections: vec!["products".into()],
            include_fields: HashMap::new(),
            exclude_fields: HashMap::new(),
            mutations: vec![OverlayMutation::Read],
            extends: None,
            additionally_include: HashMap::new(),
        });
        let overlay = reg.resolve_overlay("public").expect("overlay");
        let err = mask_record_for_overlay("users", &sample_record(), &overlay).expect_err("deny");
        assert!(matches!(err, PrivacyError::CollectionDenied(_)));
    }

    #[test]
    fn masks_batch_records() {
        let mut reg = OverlayRegistry::new();
        reg.define_overlay(OverlayDefinition::full("admin"));
        let overlay = reg.resolve_overlay("admin").expect("overlay");

        let records = vec![sample_record(), sample_record()];
        let masked = mask_records_for_overlay("users", &records, &overlay).expect("batch");
        assert_eq!(masked.len(), 2);
        assert!(masked[0].contains_key("ssn"));
    }

    proptest! {
        #[test]
        fn overlay_never_leaks_fields(
            all_fields in prop::collection::vec("[a-z_]{1,12}", 1..16),
            allowed_count in 0usize..16
        ) {
            let mut include_fields = HashMap::new();
            let allowed: Vec<String> = all_fields
                .iter()
                .take(allowed_count.min(all_fields.len()))
                .cloned()
                .collect();
            include_fields.insert("users".to_string(), allowed.clone());

            let mut reg = OverlayRegistry::new();
            reg.define_overlay(OverlayDefinition {
                name: "restricted".into(),
                access: OverlayAccess::Partial,
                collections: vec!["users".into()],
                include_fields,
                exclude_fields: HashMap::new(),
                mutations: vec![OverlayMutation::Read],
                extends: None,
                additionally_include: HashMap::new(),
            });
            let overlay = reg.resolve_overlay("restricted").expect("overlay");

            let mut record = Record::new();
            for f in &all_fields {
                record.insert(f.clone(), json!(f));
            }
            let output = mask_record_for_overlay("users", &record, &overlay).expect("mask");
            for k in output.keys() {
                prop_assert!(allowed.contains(k), "field leaked from overlay: {k}");
            }
        }
    }
}

