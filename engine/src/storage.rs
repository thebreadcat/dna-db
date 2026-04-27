use memmap2::MmapMut;
use std::fs::{create_dir_all, File, OpenOptions};
use std::path::{Path, PathBuf};
use thiserror::Error;

use crate::codec::{BincodeStrandCodec, STRAND_FORMAT_MAGIC};
use crate::model::Codon;

const DEFAULT_FILE_SIZE: usize = 1024 * 1024; // 1 MiB bootstrap

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("buffer full, resize not implemented yet")]
    BufferFull,
    #[error("strand pool scan: {0}")]
    StrandScan(#[from] crate::codec::CodecError),
}

#[derive(Debug, Clone)]
pub struct CollectionPaths {
    pub strands: PathBuf,
    pub complement: PathBuf,
    pub meta: PathBuf,
}

impl CollectionPaths {
    pub fn from_root(root: &Path, collection: &str) -> Self {
        Self {
            strands: root.join(format!("{collection}.strands")),
            complement: root.join(format!("{collection}.complement")),
            meta: root.join(format!("{collection}.meta")),
        }
    }
}

pub struct CollectionStorage {
    pub paths: CollectionPaths,
    strands_file: File,
    complement_file: File,
    meta_file: File,
    strands_map: MmapMut,
    complement_map: MmapMut,
    meta_map: MmapMut,
    strands_len: usize,
    complement_len: usize,
    meta_len: usize,
    strands_grow_events: u64,
    complement_grow_events: u64,
    meta_grow_events: u64,
    /// Highest `Strand.version` observed in the strand pool (matches WAL sequence in Stage 1 pipeline).
    pub materialized_high_water_sequence: u64,
}

impl CollectionStorage {
    pub fn open_or_create(
        root: &Path,
        collection: &str,
        initial_size: Option<usize>,
    ) -> Result<Self, StorageError> {
        create_dir_all(root)?;
        let size = initial_size.unwrap_or(DEFAULT_FILE_SIZE);
        let paths = CollectionPaths::from_root(root, collection);

        let strands_file = open_and_size(&paths.strands, size)?;
        let complement_file = open_and_size(&paths.complement, size)?;
        let meta_file = open_and_size(&paths.meta, size)?;

        let strands_map = map_mut(&strands_file)?;
        let complement_map = map_mut(&complement_file)?;
        let meta_map = map_mut(&meta_file)?;

        let strands_slice: &[u8] = &strands_map[..];
        let complement_slice: &[u8] = &complement_map[..];
        let meta_slice: &[u8] = &meta_map[..];

        let (strands_len, materialized_high_water_sequence) = scan_strands_tail(strands_slice)?;
        let complement_len = scan_complement_tail(complement_slice);
        let meta_len = scan_meta_tail(meta_slice);

        Ok(Self {
            paths,
            strands_file,
            complement_file,
            meta_file,
            strands_map,
            complement_map,
            meta_map,
            strands_len,
            complement_len,
            meta_len,
            strands_grow_events: 0,
            complement_grow_events: 0,
            meta_grow_events: 0,
            materialized_high_water_sequence,
        })
    }

    pub fn append_strands(&mut self, bytes: &[u8]) -> Result<usize, StorageError> {
        let (offset, grew) = append_with_auto_grow(
            &mut self.strands_file,
            &mut self.strands_map,
            &mut self.strands_len,
            bytes,
        )?;
        if grew {
            self.strands_grow_events += 1;
        }
        Ok(offset)
    }

    pub fn append_complement(&mut self, bytes: &[u8]) -> Result<usize, StorageError> {
        let (offset, grew) = append_with_auto_grow(
            &mut self.complement_file,
            &mut self.complement_map,
            &mut self.complement_len,
            bytes,
        )?;
        if grew {
            self.complement_grow_events += 1;
        }
        Ok(offset)
    }

    pub fn append_meta(&mut self, bytes: &[u8]) -> Result<usize, StorageError> {
        let (offset, grew) = append_with_auto_grow(
            &mut self.meta_file,
            &mut self.meta_map,
            &mut self.meta_len,
            bytes,
        )?;
        if grew {
            self.meta_grow_events += 1;
        }
        Ok(offset)
    }

    pub fn flush(&mut self) -> Result<(), StorageError> {
        self.flush_maps()?;
        self.sync_files()?;
        Ok(())
    }

    /// Flush dirty mmap pages without forcing file `sync_data`.
    pub fn flush_maps(&mut self) -> Result<(), StorageError> {
        self.strands_map.flush()?;
        self.complement_map.flush()?;
        self.meta_map.flush()?;
        Ok(())
    }

    /// Force file durability with `sync_data`.
    pub fn sync_files(&mut self) -> Result<(), StorageError> {
        self.strands_file.sync_data()?;
        self.complement_file.sync_data()?;
        self.meta_file.sync_data()?;
        Ok(())
    }

    pub fn record_materialized_sequence(&mut self, sequence: u64) {
        self.materialized_high_water_sequence =
            self.materialized_high_water_sequence.max(sequence);
    }

    /// Byte length of the valid strand-frame prefix in the strand pool (excludes trailing mmap zeros).
    pub fn strand_bytes_written(&self) -> usize {
        self.strands_len
    }

    /// Number of mmap growth/remap events per backing file.
    pub fn grow_events(&self) -> (u64, u64, u64) {
        (
            self.strands_grow_events,
            self.complement_grow_events,
            self.meta_grow_events,
        )
    }

    /// Current mmap capacities in bytes per backing file.
    pub fn map_capacities(&self) -> (usize, usize, usize) {
        (
            self.strands_map.len(),
            self.complement_map.len(),
            self.meta_map.len(),
        )
    }
}

/// Scan concatenated `DNAS` strand frames; returns byte offset after last valid frame and max `Strand.version`.
fn scan_strands_tail(bytes: &[u8]) -> Result<(usize, u64), crate::codec::CodecError> {
    let codec = BincodeStrandCodec;
    let mut pos = 0usize;
    let mut max_ver = 0u64;

    while pos + 6 <= bytes.len() {
        if bytes[pos..pos + 4] != STRAND_FORMAT_MAGIC {
            break;
        }
        let (strand, len) = codec.decode_strand_with_len(&bytes[pos..])?;
        max_ver = max_ver.max(strand.version);
        pos += len;
    }

    Ok((pos, max_ver))
}

/// Scan length-prefixed bincode `Vec<Codon>` chunks written by the WAL processor.
fn scan_complement_tail(bytes: &[u8]) -> usize {
    let mut pos = 0usize;
    while pos + 4 <= bytes.len() {
        let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        if pos + 4 + len > bytes.len() {
            break;
        }
        if bincode::deserialize::<Vec<Codon>>(&bytes[pos + 4..pos + 4 + len]).is_err() {
            break;
        }
        pos += 4 + len;
    }
    pos
}

/// Byte length of non-zero prefix in meta pool (arbitrary bytes; trailing mmap zeros ignored).
fn scan_meta_tail(bytes: &[u8]) -> usize {
    bytes
        .iter()
        .rposition(|b| *b != 0)
        .map(|i| i + 1)
        .unwrap_or(0)
}

fn open_and_size(path: &Path, size: usize) -> Result<File, StorageError> {
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path)?;

    if file.metadata()?.len() < size as u64 {
        file.set_len(size as u64)?;
    }
    Ok(file)
}

fn map_mut(file: &File) -> Result<MmapMut, StorageError> {
    // SAFETY: The file is opened read/write and lives for the map lifetime inside CollectionStorage.
    Ok(unsafe { MmapMut::map_mut(file)? })
}

fn append_to_map(map: &mut MmapMut, current_len: &mut usize, bytes: &[u8]) -> Result<usize, StorageError> {
    let start = *current_len;
    let end = start + bytes.len();
    if end > map.len() {
        return Err(StorageError::BufferFull);
    }
    map[start..end].copy_from_slice(bytes);
    *current_len = end;
    Ok(start)
}

fn append_with_auto_grow(
    file: &mut File,
    map: &mut MmapMut,
    current_len: &mut usize,
    bytes: &[u8],
) -> Result<(usize, bool), StorageError> {
    let grew = ensure_capacity(file, map, *current_len, bytes.len())?;
    let offset = append_to_map(map, current_len, bytes)?;
    Ok((offset, grew))
}

fn ensure_capacity(
    file: &mut File,
    map: &mut MmapMut,
    current_len: usize,
    append_len: usize,
) -> Result<bool, StorageError> {
    let required = current_len.saturating_add(append_len);
    if required <= map.len() {
        return Ok(false);
    }

    let mut new_size = map.len().max(DEFAULT_FILE_SIZE);
    while new_size < required {
        new_size = new_size.saturating_mul(2);
    }
    file.set_len(new_size as u64)?;
    *map = map_mut(file)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::CollectionStorage;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn creates_collection_files_and_appends() {
        let dir = tempdir().expect("tempdir");
        let mut storage =
            CollectionStorage::open_or_create(dir.path(), "users", Some(4096)).expect("open");

        let s_off = storage.append_strands(b"strand-bytes").expect("append strands");
        let c_off = storage
            .append_complement(b"complement-bytes")
            .expect("append complement");
        let m_off = storage.append_meta(b"meta-bytes").expect("append meta");
        storage.flush().expect("flush");

        assert_eq!(s_off, 0);
        assert_eq!(c_off, 0);
        assert_eq!(m_off, 0);
        assert!(storage.paths.strands.exists());
        assert!(storage.paths.complement.exists());
        assert!(storage.paths.meta.exists());
    }

    #[test]
    fn auto_grows_maps_when_append_exceeds_initial_size() {
        let dir = tempdir().expect("tempdir");
        let mut storage =
            CollectionStorage::open_or_create(dir.path(), "users", Some(64)).expect("open");

        let big = vec![b'x'; 512];
        storage.append_strands(&big).expect("append strands");
        storage.append_complement(&big).expect("append complement");
        storage.append_meta(&big).expect("append meta");
        storage.flush().expect("flush");

        assert!(fs::metadata(&storage.paths.strands).expect("strands meta").len() >= 512);
        assert!(fs::metadata(&storage.paths.complement).expect("comp meta").len() >= 512);
        assert!(fs::metadata(&storage.paths.meta).expect("meta meta").len() >= 512);
    }
}
