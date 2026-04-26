//! TLS baseline + secure defaults (Stage 3).
//!
//! This module centralizes transport-security policy checks so production deployments
//! fail fast when configured insecurely.

use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Environment {
    LocalDev,
    Staging,
    Production,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TlsMinVersion {
    V12,
    V13,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TlsConfig {
    pub enabled: bool,
    pub allow_plaintext: bool,
    pub min_version: TlsMinVersion,
    pub cert_file: Option<PathBuf>,
    pub key_file: Option<PathBuf>,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            allow_plaintext: false,
            min_version: TlsMinVersion::V13,
            cert_file: None,
            key_file: None,
        }
    }
}

#[derive(Debug, Error)]
pub enum TlsPolicyError {
    #[error("TLS must be enabled for non-local environments")]
    TlsRequired,
    #[error("plaintext connections are not allowed outside local development")]
    PlaintextForbidden,
    #[error("minimum TLS version must be 1.3 in non-local environments")]
    MinVersionTooLow,
    #[error("TLS enabled but certificate path is missing")]
    MissingCertFile,
    #[error("TLS enabled but key path is missing")]
    MissingKeyFile,
}

/// Validate TLS policy against environment profile.
pub fn validate_tls_policy(cfg: &TlsConfig, env: Environment) -> Result<(), TlsPolicyError> {
    match env {
        Environment::LocalDev => {
            // Local development can use plaintext and disabled TLS for ergonomics.
            if cfg.enabled {
                if cfg.cert_file.is_none() {
                    return Err(TlsPolicyError::MissingCertFile);
                }
                if cfg.key_file.is_none() {
                    return Err(TlsPolicyError::MissingKeyFile);
                }
            }
            Ok(())
        }
        Environment::Staging | Environment::Production => {
            if !cfg.enabled {
                return Err(TlsPolicyError::TlsRequired);
            }
            if cfg.allow_plaintext {
                return Err(TlsPolicyError::PlaintextForbidden);
            }
            if cfg.min_version != TlsMinVersion::V13 {
                return Err(TlsPolicyError::MinVersionTooLow);
            }
            if cfg.cert_file.is_none() {
                return Err(TlsPolicyError::MissingCertFile);
            }
            if cfg.key_file.is_none() {
                return Err(TlsPolicyError::MissingKeyFile);
            }
            Ok(())
        }
    }
}

/// Strict recommended default profile for production-safe deployments.
pub fn production_secure_defaults() -> TlsConfig {
    TlsConfig {
        enabled: true,
        allow_plaintext: false,
        min_version: TlsMinVersion::V13,
        cert_file: Some(PathBuf::from("/certs/server.crt")),
        key_file: Some(PathBuf::from("/certs/server.key")),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        production_secure_defaults, validate_tls_policy, Environment, TlsConfig, TlsMinVersion,
        TlsPolicyError,
    };
    use std::path::PathBuf;

    #[test]
    fn production_secure_defaults_validate() {
        let cfg = production_secure_defaults();
        assert!(validate_tls_policy(&cfg, Environment::Production).is_ok());
    }

    #[test]
    fn production_rejects_plaintext_or_disabled_tls() {
        let mut cfg = production_secure_defaults();
        cfg.allow_plaintext = true;
        assert!(matches!(
            validate_tls_policy(&cfg, Environment::Production),
            Err(TlsPolicyError::PlaintextForbidden)
        ));

        cfg.allow_plaintext = false;
        cfg.enabled = false;
        assert!(matches!(
            validate_tls_policy(&cfg, Environment::Production),
            Err(TlsPolicyError::TlsRequired)
        ));
    }

    #[test]
    fn production_requires_tls13_and_cert_key() {
        let mut cfg = production_secure_defaults();
        cfg.min_version = TlsMinVersion::V12;
        assert!(matches!(
            validate_tls_policy(&cfg, Environment::Production),
            Err(TlsPolicyError::MinVersionTooLow)
        ));

        cfg.min_version = TlsMinVersion::V13;
        cfg.cert_file = None;
        assert!(matches!(
            validate_tls_policy(&cfg, Environment::Production),
            Err(TlsPolicyError::MissingCertFile)
        ));

        cfg.cert_file = Some(PathBuf::from("/certs/server.crt"));
        cfg.key_file = None;
        assert!(matches!(
            validate_tls_policy(&cfg, Environment::Production),
            Err(TlsPolicyError::MissingKeyFile)
        ));
    }

    #[test]
    fn local_dev_allows_plaintext_or_disabled_tls() {
        let cfg = TlsConfig {
            enabled: false,
            allow_plaintext: true,
            min_version: TlsMinVersion::V12,
            cert_file: None,
            key_file: None,
        };
        assert!(validate_tls_policy(&cfg, Environment::LocalDev).is_ok());
    }
}

