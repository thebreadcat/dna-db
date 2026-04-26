//! Immutable audit logging primitives.
//!
//! Audit entries are append-only and linked by hash chain for tamper-evidence.

use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AuditEntry {
    pub audit_id: String,
    pub timestamp_ns: u64,
    pub identity_id: String,
    pub overlay: String,
    pub operation: String,
    pub collection: String,
    pub strand_count: u64,
    pub fields_returned: Vec<String>,
    pub query_pattern: String,
    pub client_ip: String,
    pub duration_ms: u64,
    pub prev_hash: Option<String>,
    pub entry_hash: String,
}

#[derive(Debug, Clone)]
pub struct AuditWrite {
    pub timestamp_ns: u64,
    pub identity_id: String,
    pub overlay: String,
    pub operation: String,
    pub collection: String,
    pub strand_count: u64,
    pub fields_returned: Vec<String>,
    pub query_pattern: String,
    pub client_ip: String,
    pub duration_ms: u64,
}

#[derive(Debug, Error)]
pub enum AuditError {
    #[error("audit chain integrity check failed at index {0}")]
    IntegrityFailure(usize),
    #[error("serialization error: {0}")]
    Serialize(#[from] bincode::Error),
}

#[derive(Debug, Default)]
pub struct AuditLog {
    entries: Vec<AuditEntry>,
}

impl AuditLog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append-only write; no API for mutation/deletion.
    pub fn append(&mut self, write: AuditWrite) -> Result<&AuditEntry, AuditError> {
        let prev_hash = self.entries.last().map(|e| e.entry_hash.clone());
        let audit_id = format!("audit:{}", Uuid::new_v4().simple());
        let entry_hash = compute_entry_hash(
            prev_hash.as_deref(),
            &audit_id,
            write.timestamp_ns,
            &write.identity_id,
            &write.overlay,
            &write.operation,
            &write.collection,
            write.strand_count,
            &write.fields_returned,
            &write.query_pattern,
            &write.client_ip,
            write.duration_ms,
        )?;

        let entry = AuditEntry {
            audit_id,
            timestamp_ns: write.timestamp_ns,
            identity_id: write.identity_id,
            overlay: write.overlay,
            operation: write.operation,
            collection: write.collection,
            strand_count: write.strand_count,
            fields_returned: write.fields_returned,
            query_pattern: write.query_pattern,
            client_ip: write.client_ip,
            duration_ms: write.duration_ms,
            prev_hash,
            entry_hash,
        };
        self.entries.push(entry);
        Ok(self.entries.last().expect("entry just pushed"))
    }

    pub fn entries(&self) -> &[AuditEntry] {
        &self.entries
    }

    /// Verifies hash-chain integrity of the entire log.
    pub fn verify_integrity(&self) -> Result<(), AuditError> {
        for idx in 0..self.entries.len() {
            let e = &self.entries[idx];
            let expected_prev = if idx == 0 {
                None
            } else {
                Some(self.entries[idx - 1].entry_hash.as_str())
            };
            if e.prev_hash.as_deref() != expected_prev {
                return Err(AuditError::IntegrityFailure(idx));
            }

            let recomputed = compute_entry_hash(
                e.prev_hash.as_deref(),
                &e.audit_id,
                e.timestamp_ns,
                &e.identity_id,
                &e.overlay,
                &e.operation,
                &e.collection,
                e.strand_count,
                &e.fields_returned,
                &e.query_pattern,
                &e.client_ip,
                e.duration_ms,
            )?;
            if recomputed != e.entry_hash {
                return Err(AuditError::IntegrityFailure(idx));
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn tamper_query_pattern_for_test(&mut self, index: usize, value: &str) {
        if let Some(e) = self.entries.get_mut(index) {
            e.query_pattern = value.to_string();
        }
    }
}

fn compute_entry_hash(
    prev_hash: Option<&str>,
    audit_id: &str,
    timestamp_ns: u64,
    identity_id: &str,
    overlay: &str,
    operation: &str,
    collection: &str,
    strand_count: u64,
    fields_returned: &[String],
    query_pattern: &str,
    client_ip: &str,
    duration_ms: u64,
) -> Result<String, AuditError> {
    let payload = (
        prev_hash,
        audit_id,
        timestamp_ns,
        identity_id,
        overlay,
        operation,
        collection,
        strand_count,
        fields_returned,
        query_pattern,
        client_ip,
        duration_ms,
    );
    let encoded = bincode::serialize(&payload)?;
    Ok(blake3::hash(&encoded).to_hex().to_string())
}

#[cfg(test)]
mod tests {
    use super::{AuditError, AuditLog, AuditWrite};

    fn write(ts: u64, pattern: &str) -> AuditWrite {
        AuditWrite {
            timestamp_ns: ts,
            identity_id: "user:alice".into(),
            overlay: "support_agent".into(),
            operation: "read".into(),
            collection: "users".into(),
            strand_count: 1,
            fields_returned: vec!["id".into(), "email".into()],
            query_pattern: pattern.into(),
            client_ip: "10.0.0.1".into(),
            duration_ms: 3,
        }
    }

    #[test]
    fn append_creates_hash_chain() {
        let mut log = AuditLog::new();
        let e1 = log.append(write(1, "email = ?")).expect("e1").clone();
        let e2 = log.append(write(2, "id = ?")).expect("e2").clone();
        assert!(e1.prev_hash.is_none());
        assert_eq!(e2.prev_hash.as_deref(), Some(e1.entry_hash.as_str()));
        assert!(log.verify_integrity().is_ok());
    }

    #[test]
    fn integrity_verification_detects_tampering() {
        let mut log = AuditLog::new();
        log.append(write(1, "email = ?")).expect("e1");
        log.append(write(2, "id = ?")).expect("e2");

        // Simulate unauthorized mutation in-memory.
        log.tamper_query_pattern_for_test(1, "tampered");

        let err = log.verify_integrity().expect_err("should fail");
        assert!(matches!(err, AuditError::IntegrityFailure(1)));
    }
}

