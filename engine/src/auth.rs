//! Stage 3 auth foundation: identity store + token/session model.
//!
//! This module provides an in-memory identity registry and short-lived session tokens
//! bound to overlay assignments, matching Layer 6 authentication flow at a core level.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum IdentityType {
    Human,
    Service,
    Admin,
    ReadOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CredentialKind {
    Password,
    ApiKey,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Identity {
    pub identity_id: Uuid,
    pub name: String,
    pub identity_type: IdentityType,
    pub overlay: String,
    pub allowed_collections: Vec<String>,
    pub token_expiry_seconds: u64,
    pub mfa_required: bool,
}

#[derive(Debug, Clone)]
struct IdentityRecord {
    identity: Identity,
    password_hash: Option<String>,
    api_key_hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SessionToken {
    pub token: String,
    pub identity_id: Uuid,
    pub overlay: String,
    pub issued_at_ns: u64,
    pub expires_at_ns: u64,
    pub revoked: bool,
}

impl SessionToken {
    pub fn is_expired_at(&self, now_ns: u64) -> bool {
        now_ns >= self.expires_at_ns
    }
}

#[derive(Debug, Clone)]
pub struct CreateIdentityRequest {
    pub name: String,
    pub identity_type: IdentityType,
    pub overlay: String,
    pub allowed_collections: Vec<String>,
    pub token_expiry_seconds: u64,
    pub mfa_required: bool,
    pub password: Option<String>,
    pub api_key: Option<String>,
}

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("identity name already exists: {0}")]
    DuplicateIdentity(String),
    #[error("identity not found: {0}")]
    IdentityNotFound(String),
    #[error("invalid credentials")]
    InvalidCredentials,
    #[error("credential kind is not configured for identity: {0}")]
    UnsupportedCredentialKind(String),
    #[error("session token not found")]
    SessionNotFound,
    #[error("session token expired")]
    SessionExpired,
    #[error("session token revoked")]
    SessionRevoked,
    #[error("token expiry must be > 0 seconds")]
    InvalidTokenExpiry,
}

#[derive(Debug, Default)]
pub struct IdentityStore {
    identities_by_id: HashMap<Uuid, IdentityRecord>,
    identity_id_by_name: HashMap<String, Uuid>,
    sessions_by_token: HashMap<String, SessionToken>,
}

impl IdentityStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create_identity(&mut self, req: CreateIdentityRequest) -> Result<Identity, AuthError> {
        if req.token_expiry_seconds == 0 {
            return Err(AuthError::InvalidTokenExpiry);
        }
        if self.identity_id_by_name.contains_key(&req.name) {
            return Err(AuthError::DuplicateIdentity(req.name));
        }

        let identity = Identity {
            identity_id: Uuid::new_v4(),
            name: req.name.clone(),
            identity_type: req.identity_type,
            overlay: req.overlay,
            allowed_collections: req.allowed_collections,
            token_expiry_seconds: req.token_expiry_seconds,
            mfa_required: req.mfa_required,
        };

        let record = IdentityRecord {
            identity: identity.clone(),
            password_hash: req.password.as_deref().map(hash_secret),
            api_key_hash: req.api_key.as_deref().map(hash_secret),
        };

        self.identity_id_by_name
            .insert(identity.name.clone(), identity.identity_id);
        self.identities_by_id.insert(identity.identity_id, record);
        Ok(identity)
    }

    pub fn get_identity_by_name(&self, name: &str) -> Option<&Identity> {
        let id = self.identity_id_by_name.get(name)?;
        self.identities_by_id.get(id).map(|r| &r.identity)
    }

    pub fn authenticate_with_password(
        &mut self,
        identity_name: &str,
        password: &str,
    ) -> Result<SessionToken, AuthError> {
        let identity = self.get_identity_record(identity_name)?.clone();
        let expected = identity
            .password_hash
            .as_ref()
            .ok_or_else(|| AuthError::UnsupportedCredentialKind(identity_name.to_string()))?;
        if *expected != hash_secret(password) {
            return Err(AuthError::InvalidCredentials);
        }
        Ok(self.issue_session_token(&identity))
    }

    pub fn authenticate_with_api_key(
        &mut self,
        identity_name: &str,
        api_key: &str,
    ) -> Result<SessionToken, AuthError> {
        let identity = self.get_identity_record(identity_name)?.clone();
        let expected = identity
            .api_key_hash
            .as_ref()
            .ok_or_else(|| AuthError::UnsupportedCredentialKind(identity_name.to_string()))?;
        if *expected != hash_secret(api_key) {
            return Err(AuthError::InvalidCredentials);
        }
        Ok(self.issue_session_token(&identity))
    }

    pub fn validate_session(&self, token: &str) -> Result<&SessionToken, AuthError> {
        let s = self
            .sessions_by_token
            .get(token)
            .ok_or(AuthError::SessionNotFound)?;
        if s.revoked {
            return Err(AuthError::SessionRevoked);
        }
        if s.is_expired_at(now_ns()) {
            return Err(AuthError::SessionExpired);
        }
        Ok(s)
    }

    pub fn revoke_session(&mut self, token: &str) -> Result<(), AuthError> {
        let s = self
            .sessions_by_token
            .get_mut(token)
            .ok_or(AuthError::SessionNotFound)?;
        s.revoked = true;
        Ok(())
    }

    /// Rotates the session token and invalidates the old one.
    pub fn refresh_session(&mut self, token: &str) -> Result<SessionToken, AuthError> {
        let current = self.validate_session(token)?.clone();
        let record = self
            .identities_by_id
            .get(&current.identity_id)
            .ok_or_else(|| AuthError::IdentityNotFound(current.identity_id.to_string()))?
            .clone();

        self.revoke_session(token)?;
        Ok(self.issue_session_token(&record))
    }

    fn get_identity_record(&self, name: &str) -> Result<&IdentityRecord, AuthError> {
        let id = self
            .identity_id_by_name
            .get(name)
            .ok_or_else(|| AuthError::IdentityNotFound(name.to_string()))?;
        self.identities_by_id
            .get(id)
            .ok_or_else(|| AuthError::IdentityNotFound(name.to_string()))
    }

    fn issue_session_token(&mut self, record: &IdentityRecord) -> SessionToken {
        let issued = now_ns();
        let expires = issued + Duration::from_secs(record.identity.token_expiry_seconds).as_nanos() as u64;
        let token = format!("st_{}", Uuid::new_v4().simple());
        let session = SessionToken {
            token: token.clone(),
            identity_id: record.identity.identity_id,
            overlay: record.identity.overlay.clone(),
            issued_at_ns: issued,
            expires_at_ns: expires,
            revoked: false,
        };
        self.sessions_by_token.insert(token, session.clone());
        session
    }
}

#[inline]
fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn hash_secret(secret: &str) -> String {
    blake3::hash(secret.as_bytes()).to_hex().to_string()
}

#[cfg(test)]
mod tests {
    use super::{AuthError, CreateIdentityRequest, IdentityStore, IdentityType};

    #[test]
    fn create_identity_and_authenticate_password() {
        let mut store = IdentityStore::new();
        store
            .create_identity(CreateIdentityRequest {
                name: "alice".into(),
                identity_type: IdentityType::Human,
                overlay: "support_agent".into(),
                allowed_collections: vec!["users".into(), "orders".into()],
                token_expiry_seconds: 60,
                mfa_required: false,
                password: Some("secret".into()),
                api_key: None,
            })
            .expect("identity");

        let s = store
            .authenticate_with_password("alice", "secret")
            .expect("session");
        let validated = store.validate_session(&s.token).expect("validate");
        assert_eq!(validated.overlay, "support_agent");
        assert!(!validated.revoked);
    }

    #[test]
    fn api_key_auth_and_refresh_rotates_token() {
        let mut store = IdentityStore::new();
        store
            .create_identity(CreateIdentityRequest {
                name: "svc-billing".into(),
                identity_type: IdentityType::Service,
                overlay: "admin".into(),
                allowed_collections: vec!["*".into()],
                token_expiry_seconds: 60,
                mfa_required: false,
                password: None,
                api_key: Some("apikey-1".into()),
            })
            .expect("identity");

        let s1 = store
            .authenticate_with_api_key("svc-billing", "apikey-1")
            .expect("session");
        let s2 = store.refresh_session(&s1.token).expect("refresh");
        assert_ne!(s1.token, s2.token);
        assert!(matches!(
            store.validate_session(&s1.token),
            Err(AuthError::SessionRevoked)
        ));
        assert!(store.validate_session(&s2.token).is_ok());
    }

    #[test]
    fn wrong_credentials_fail() {
        let mut store = IdentityStore::new();
        store
            .create_identity(CreateIdentityRequest {
                name: "bob".into(),
                identity_type: IdentityType::Human,
                overlay: "read_only".into(),
                allowed_collections: vec!["users".into()],
                token_expiry_seconds: 60,
                mfa_required: false,
                password: Some("correct".into()),
                api_key: None,
            })
            .expect("identity");

        assert!(matches!(
            store.authenticate_with_password("bob", "wrong"),
            Err(AuthError::InvalidCredentials)
        ));
    }

    #[test]
    fn zero_ttl_is_rejected() {
        let mut store = IdentityStore::new();
        let out = store.create_identity(CreateIdentityRequest {
            name: "bad".into(),
            identity_type: IdentityType::Human,
            overlay: "x".into(),
            allowed_collections: vec![],
            token_expiry_seconds: 0,
            mfa_required: false,
            password: Some("x".into()),
            api_key: None,
        });
        assert!(matches!(out, Err(AuthError::InvalidTokenExpiry)));
    }
}

