//! Stage 3 overlay definitions + resolver pipeline.
//!
//! This module defines overlay policies and resolves request access by binding a validated
//! session token (auth) to an overlay policy (privacy).

use std::collections::{HashMap, HashSet};

use thiserror::Error;

use crate::auth::{AuthError, IdentityStore, SessionToken};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum OverlayAccess {
    Full,
    Partial,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum OverlayMutation {
    Read,
    Write,
    Delete,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OverlayDefinition {
    pub name: String,
    pub access: OverlayAccess,
    pub collections: Vec<String>,
    pub include_fields: HashMap<String, Vec<String>>,
    pub exclude_fields: HashMap<String, Vec<String>>,
    pub mutations: Vec<OverlayMutation>,
    pub extends: Option<String>,
    pub additionally_include: HashMap<String, Vec<String>>,
}

impl OverlayDefinition {
    pub fn full(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            access: OverlayAccess::Full,
            collections: vec!["*".to_string()],
            include_fields: HashMap::new(),
            exclude_fields: HashMap::new(),
            mutations: vec![
                OverlayMutation::Read,
                OverlayMutation::Write,
                OverlayMutation::Delete,
            ],
            extends: None,
            additionally_include: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedOverlay {
    pub name: String,
    pub access: OverlayAccess,
    pub collections: HashSet<String>,
    pub include_fields: HashMap<String, HashSet<String>>,
    pub exclude_fields: HashMap<String, HashSet<String>>,
    pub mutations: HashSet<OverlayMutation>,
}

impl ResolvedOverlay {
    pub fn can_mutate(&self, mutation: OverlayMutation) -> bool {
        self.mutations.contains(&mutation)
    }

    pub fn can_access_collection(&self, collection: &str) -> bool {
        self.collections.contains("*") || self.collections.contains(collection)
    }

    /// Returns whether this field should be visible after overlay masking.
    pub fn is_field_visible(&self, collection: &str, field: &str) -> bool {
        if !self.can_access_collection(collection) {
            return false;
        }

        if self.access == OverlayAccess::Full {
            return true;
        }

        if let Some(denied) = self.exclude_fields.get(collection) {
            if denied.contains(field) {
                return false;
            }
        }

        if let Some(allowed) = self.include_fields.get(collection) {
            return allowed.contains(field);
        }

        false
    }
}

#[derive(Debug, Error)]
pub enum OverlayError {
    #[error("overlay not found: {0}")]
    OverlayNotFound(String),
    #[error("overlay `{0}` has unresolved parent: {1}")]
    UnresolvedParent(String, String),
    #[error("overlay inheritance cycle detected at: {0}")]
    InheritanceCycle(String),
    #[error("identity/session error: {0}")]
    Auth(#[from] AuthError),
}

#[derive(Debug, Default)]
pub struct OverlayRegistry {
    defs: HashMap<String, OverlayDefinition>,
}

impl OverlayRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn define_overlay(&mut self, overlay: OverlayDefinition) {
        self.defs.insert(overlay.name.clone(), overlay);
    }

    pub fn get_overlay(&self, name: &str) -> Option<&OverlayDefinition> {
        self.defs.get(name)
    }

    pub fn resolve_overlay(&self, name: &str) -> Result<ResolvedOverlay, OverlayError> {
        let mut visiting = HashSet::new();
        self.resolve_overlay_inner(name, &mut visiting)
    }

    pub fn resolve_for_session(
        &self,
        auth: &IdentityStore,
        token: &str,
    ) -> Result<(SessionToken, ResolvedOverlay), OverlayError> {
        let session = auth.validate_session(token)?.clone();
        let resolved = self.resolve_overlay(&session.overlay)?;
        Ok((session, resolved))
    }

    fn resolve_overlay_inner(
        &self,
        name: &str,
        visiting: &mut HashSet<String>,
    ) -> Result<ResolvedOverlay, OverlayError> {
        let def = self
            .defs
            .get(name)
            .ok_or_else(|| OverlayError::OverlayNotFound(name.to_string()))?;

        if !visiting.insert(name.to_string()) {
            return Err(OverlayError::InheritanceCycle(name.to_string()));
        }

        let mut base = if let Some(parent_name) = &def.extends {
            if !self.defs.contains_key(parent_name) {
                return Err(OverlayError::UnresolvedParent(
                    name.to_string(),
                    parent_name.clone(),
                ));
            }
            self.resolve_overlay_inner(parent_name, visiting)?
        } else {
            ResolvedOverlay {
                name: name.to_string(),
                access: def.access,
                collections: HashSet::new(),
                include_fields: HashMap::new(),
                exclude_fields: HashMap::new(),
                mutations: HashSet::new(),
            }
        };

        base.name = def.name.clone();
        base.access = def.access;
        base.collections.extend(def.collections.iter().cloned());
        base.mutations.extend(def.mutations.iter().copied());

        merge_field_map(&mut base.include_fields, &def.include_fields);
        merge_field_map(&mut base.exclude_fields, &def.exclude_fields);
        merge_field_map(&mut base.include_fields, &def.additionally_include);

        visiting.remove(name);
        Ok(base)
    }
}

fn merge_field_map(
    target: &mut HashMap<String, HashSet<String>>,
    source: &HashMap<String, Vec<String>>,
) {
    for (collection, fields) in source {
        target
            .entry(collection.clone())
            .or_default()
            .extend(fields.iter().cloned());
    }
}

#[cfg(test)]
mod tests {
    use super::{
        OverlayAccess, OverlayDefinition, OverlayMutation, OverlayRegistry, ResolvedOverlay,
    };
    use crate::auth::{CreateIdentityRequest, IdentityStore, IdentityType};
    use std::collections::HashMap;

    fn support_overlay() -> OverlayDefinition {
        let mut include_fields = HashMap::new();
        include_fields.insert(
            "users".to_string(),
            vec![
                "id".to_string(),
                "name".to_string(),
                "email".to_string(),
                "created_at".to_string(),
            ],
        );
        let mut exclude_fields = HashMap::new();
        exclude_fields.insert(
            "users".to_string(),
            vec!["ssn".to_string(), "password_hash".to_string()],
        );
        OverlayDefinition {
            name: "support_agent".to_string(),
            access: OverlayAccess::Partial,
            collections: vec!["users".to_string(), "orders".to_string()],
            include_fields,
            exclude_fields,
            mutations: vec![OverlayMutation::Read],
            extends: None,
            additionally_include: HashMap::new(),
        }
    }

    #[test]
    fn resolves_basic_overlay_and_field_visibility() {
        let mut registry = OverlayRegistry::new();
        registry.define_overlay(support_overlay());

        let resolved = registry.resolve_overlay("support_agent").expect("resolved");
        assert!(resolved.can_access_collection("users"));
        assert!(!resolved.can_access_collection("products"));
        assert!(resolved.can_mutate(OverlayMutation::Read));
        assert!(!resolved.can_mutate(OverlayMutation::Write));
        assert!(resolved.is_field_visible("users", "email"));
        assert!(!resolved.is_field_visible("users", "ssn"));
        assert!(!resolved.is_field_visible("users", "account_tier"));
    }

    #[test]
    fn resolves_compound_overlay_with_extends() {
        let mut registry = OverlayRegistry::new();
        registry.define_overlay(support_overlay());

        let mut additionally_include = HashMap::new();
        additionally_include.insert("users".to_string(), vec!["payment_last_four".to_string()]);
        registry.define_overlay(OverlayDefinition {
            name: "senior_support".to_string(),
            access: OverlayAccess::Partial,
            collections: vec![],
            include_fields: HashMap::new(),
            exclude_fields: HashMap::new(),
            mutations: vec![OverlayMutation::Read],
            extends: Some("support_agent".to_string()),
            additionally_include,
        });

        let resolved = registry.resolve_overlay("senior_support").expect("resolved");
        assert!(resolved.is_field_visible("users", "payment_last_four"));
        assert!(resolved.is_field_visible("users", "email"));
    }

    #[test]
    fn resolve_for_session_binds_auth_to_overlay() {
        let mut auth = IdentityStore::new();
        auth.create_identity(CreateIdentityRequest {
            name: "alice".into(),
            identity_type: IdentityType::Human,
            overlay: "support_agent".into(),
            allowed_collections: vec!["users".into()],
            token_expiry_seconds: 60,
            mfa_required: false,
            password: Some("secret".into()),
            api_key: None,
        })
        .expect("identity");
        let session = auth
            .authenticate_with_password("alice", "secret")
            .expect("session");

        let mut registry = OverlayRegistry::new();
        registry.define_overlay(support_overlay());

        let (validated_session, overlay) = registry
            .resolve_for_session(&auth, &session.token)
            .expect("resolve");
        assert_eq!(validated_session.overlay, "support_agent");
        assert_eq!(overlay.name, "support_agent");
        assert!(overlay.can_mutate(OverlayMutation::Read));
    }

    #[test]
    fn full_access_overlay_allows_everything() {
        let mut registry = OverlayRegistry::new();
        registry.define_overlay(OverlayDefinition::full("admin"));
        let resolved: ResolvedOverlay = registry.resolve_overlay("admin").expect("resolved");
        assert!(resolved.can_access_collection("users"));
        assert!(resolved.can_access_collection("any_collection"));
        assert!(resolved.is_field_visible("users", "ssn"));
        assert!(resolved.can_mutate(OverlayMutation::Delete));
    }
}

