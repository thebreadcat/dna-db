//! Persistent lifecycle manager for global indexes (build/save/load).

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::model::Strand;

use super::index::{GlobalHashIndex, GlobalHashIndexError, GlobalRangeIndex};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct IndexManifest {
    hash_fields: Vec<String>,
    range_fields: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct GlobalIndexCatalog {
    pub hash_indexes: HashMap<String, GlobalHashIndex>,
    pub range_indexes: HashMap<String, GlobalRangeIndex>,
}

#[derive(Debug, Error)]
pub enum GlobalIndexCatalogError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialize: {0}")]
    Serialize(#[from] bincode::Error),
    #[error("hash index: {0}")]
    Hash(#[from] GlobalHashIndexError),
}

fn index_dir(root: &Path, collection: &str) -> PathBuf {
    root.join(format!("{collection}.indexes"))
}

fn field_key(field: &str) -> String {
    blake3::hash(field.as_bytes()).to_hex().to_string()
}

impl GlobalIndexCatalog {
    pub fn build(strands: &[Strand], hash_fields: &[&str], range_fields: &[&str]) -> Self {
        let mut out = Self::default();
        for field in hash_fields {
            out.hash_indexes
                .insert((*field).to_string(), GlobalHashIndex::build(*field, strands));
        }
        for field in range_fields {
            out.range_indexes
                .insert((*field).to_string(), GlobalRangeIndex::build(*field, strands));
        }
        out
    }

    pub fn save_to_root(&self, root: &Path, collection: &str) -> Result<(), GlobalIndexCatalogError> {
        let dir = index_dir(root, collection);
        std::fs::create_dir_all(&dir)?;
        let manifest = IndexManifest {
            hash_fields: self.hash_indexes.keys().cloned().collect(),
            range_fields: self.range_indexes.keys().cloned().collect(),
        };
        let manifest_path = dir.join("manifest.bin");
        std::fs::write(manifest_path, bincode::serialize(&manifest)?)?;

        for (field, idx) in &self.hash_indexes {
            let path = dir.join(format!("hash-{}.bin", field_key(field)));
            idx.save_to_path(&path)?;
        }
        for (field, idx) in &self.range_indexes {
            let path = dir.join(format!("range-{}.bin", field_key(field)));
            idx.save_to_path(&path)?;
        }
        Ok(())
    }

    pub fn load_from_root(root: &Path, collection: &str) -> Result<Self, GlobalIndexCatalogError> {
        let dir = index_dir(root, collection);
        let manifest_path = dir.join("manifest.bin");
        let bytes = std::fs::read(manifest_path)?;
        let manifest: IndexManifest = bincode::deserialize(&bytes)?;
        let mut out = Self::default();

        for field in manifest.hash_fields {
            let path = dir.join(format!("hash-{}.bin", field_key(&field)));
            let idx = GlobalHashIndex::load_from_path(&path)?;
            out.hash_indexes.insert(field, idx);
        }
        for field in manifest.range_fields {
            let path = dir.join(format!("range-{}.bin", field_key(&field)));
            let idx = GlobalRangeIndex::load_from_path(&path)?;
            out.range_indexes.insert(field, idx);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::processor::strand_from_wal_payload;

    use super::GlobalIndexCatalog;

    #[test]
    fn catalog_roundtrip_save_and_load() {
        let dir = tempdir().expect("tempdir");
        let strands = vec![
            strand_from_wal_payload(1, 1, b"10"),
            strand_from_wal_payload(1, 2, b"20"),
            strand_from_wal_payload(1, 3, b"alice@example.com"),
        ];
        let built = GlobalIndexCatalog::build(&strands, &["_payload"], &["_payload"]);
        built
            .save_to_root(dir.path(), "users")
            .expect("save catalog");
        let loaded = GlobalIndexCatalog::load_from_root(dir.path(), "users").expect("load catalog");
        assert!(loaded.hash_indexes.contains_key("_payload"));
        assert!(loaded.range_indexes.contains_key("_payload"));
        let hash_hits = loaded.hash_indexes["_payload"].lookup(b"alice@example.com");
        assert_eq!(hash_hits.len(), 1);
        let range_hits = loaded.range_indexes["_payload"].lookup(crate::query::RangeOp::GreaterThan, 15);
        assert_eq!(range_hits.len(), 1);
    }
}

