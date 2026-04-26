use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use thiserror::Error;

const WAL_MAGIC: [u8; 4] = *b"DWAL";
const WAL_VERSION: u16 = 1;
const WAL_HEADER_SIZE: u64 = 6;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalEntry {
    pub sequence: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum WalError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid wal header magic")]
    InvalidMagic,
    #[error("unsupported wal version: {0}")]
    UnsupportedVersion(u16),
    #[error("truncated wal entry")]
    TruncatedEntry,
    #[error("entry payload too large: {0}")]
    PayloadTooLarge(usize),
}

pub struct Wal {
    path: PathBuf,
    file: File,
    next_sequence: u64,
}

impl Wal {
    pub fn open_or_create(root: &Path, collection: &str) -> Result<Self, WalError> {
        create_dir_all(root)?;
        let path = root.join(format!("{collection}.wal"));

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;

        let next_sequence = initialize_and_scan(&mut file)?;
        file.seek(SeekFrom::End(0))?;

        Ok(Self {
            path,
            file,
            next_sequence,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    /// Append one entry without forcing fsync.
    ///
    /// Call [`Wal::sync`] explicitly for group-commit behavior.
    pub fn append(&mut self, payload: &[u8]) -> Result<u64, WalError> {
        if payload.len() > u32::MAX as usize {
            return Err(WalError::PayloadTooLarge(payload.len()));
        }

        let sequence = self.next_sequence;
        self.file.write_all(&sequence.to_le_bytes())?;
        self.file.write_all(&(payload.len() as u32).to_le_bytes())?;
        self.file.write_all(payload)?;
        self.next_sequence += 1;
        Ok(sequence)
    }

    /// Force durability for previously appended entries.
    pub fn sync(&mut self) -> Result<(), WalError> {
        self.file.sync_data()?;
        Ok(())
    }

    pub fn append_and_fsync(&mut self, payload: &[u8]) -> Result<u64, WalError> {
        let sequence = self.append(payload)?;
        self.sync()?;
        Ok(sequence)
    }

    pub fn read_all_entries(&mut self) -> Result<Vec<WalEntry>, WalError> {
        self.file.flush()?;
        self.file.seek(SeekFrom::Start(WAL_HEADER_SIZE))?;
        let mut entries = Vec::new();

        loop {
            let mut seq_buf = [0_u8; 8];
            match self.file.read_exact(&mut seq_buf) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(err) => return Err(WalError::Io(err)),
            }

            let mut len_buf = [0_u8; 4];
            if let Err(err) = self.file.read_exact(&mut len_buf) {
                if err.kind() == std::io::ErrorKind::UnexpectedEof {
                    return Err(WalError::TruncatedEntry);
                }
                return Err(WalError::Io(err));
            }
            let len = u32::from_le_bytes(len_buf) as usize;

            let mut payload = vec![0_u8; len];
            if let Err(err) = self.file.read_exact(&mut payload) {
                if err.kind() == std::io::ErrorKind::UnexpectedEof {
                    return Err(WalError::TruncatedEntry);
                }
                return Err(WalError::Io(err));
            }

            entries.push(WalEntry {
                sequence: u64::from_le_bytes(seq_buf),
                payload,
            });
        }

        self.file.seek(SeekFrom::End(0))?;
        Ok(entries)
    }

    /// Read entries with `sequence >= from_sequence`, optionally limited to `limit`.
    pub fn read_entries_from(
        &mut self,
        from_sequence: u64,
        limit: Option<usize>,
    ) -> Result<Vec<WalEntry>, WalError> {
        let mut entries = self.read_all_entries()?;
        entries.retain(|e| e.sequence >= from_sequence);
        if let Some(max) = limit {
            entries.truncate(max);
        }
        Ok(entries)
    }
}

fn initialize_and_scan(file: &mut File) -> Result<u64, WalError> {
    let len = file.metadata()?.len();
    if len == 0 {
        file.write_all(&WAL_MAGIC)?;
        file.write_all(&WAL_VERSION.to_le_bytes())?;
        file.sync_data()?;
        return Ok(1);
    }

    file.seek(SeekFrom::Start(0))?;
    let mut magic = [0_u8; 4];
    file.read_exact(&mut magic)?;
    if magic != WAL_MAGIC {
        return Err(WalError::InvalidMagic);
    }

    let mut version = [0_u8; 2];
    file.read_exact(&mut version)?;
    let version = u16::from_le_bytes(version);
    if version != WAL_VERSION {
        return Err(WalError::UnsupportedVersion(version));
    }

    let mut last_sequence = 0_u64;
    loop {
        let mut seq_buf = [0_u8; 8];
        match file.read_exact(&mut seq_buf) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(WalError::Io(err)),
        }

        let mut len_buf = [0_u8; 4];
        if let Err(err) = file.read_exact(&mut len_buf) {
            if err.kind() == std::io::ErrorKind::UnexpectedEof {
                return Err(WalError::TruncatedEntry);
            }
            return Err(WalError::Io(err));
        }
        let payload_len = u32::from_le_bytes(len_buf) as usize;

        let mut discard = vec![0_u8; payload_len];
        if let Err(err) = file.read_exact(&mut discard) {
            if err.kind() == std::io::ErrorKind::UnexpectedEof {
                return Err(WalError::TruncatedEntry);
            }
            return Err(WalError::Io(err));
        }
        last_sequence = u64::from_le_bytes(seq_buf);
    }

    Ok(last_sequence + 1)
}

#[cfg(test)]
mod tests {
    use super::Wal;
    use tempfile::tempdir;

    #[test]
    fn wal_appends_with_monotonic_sequence_and_replays() {
        let dir = tempdir().expect("tempdir");
        let mut wal = Wal::open_or_create(dir.path(), "users").expect("open wal");

        let s1 = wal.append_and_fsync(b"first").expect("append first");
        let s2 = wal.append_and_fsync(b"second").expect("append second");
        assert_eq!(s1, 1);
        assert_eq!(s2, 2);

        let entries = wal.read_all_entries().expect("read entries");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].sequence, 1);
        assert_eq!(entries[0].payload, b"first".to_vec());
        assert_eq!(entries[1].sequence, 2);
        assert_eq!(entries[1].payload, b"second".to_vec());
    }

    #[test]
    fn wal_recovers_next_sequence_on_reopen() {
        let dir = tempdir().expect("tempdir");
        let mut wal = Wal::open_or_create(dir.path(), "users").expect("open wal");
        wal.append_and_fsync(b"one").expect("append");
        wal.append_and_fsync(b"two").expect("append");
        let path = wal.path().to_path_buf();
        drop(wal);

        let mut reopened = Wal::open_or_create(dir.path(), "users").expect("reopen wal");
        assert_eq!(reopened.next_sequence(), 3);
        assert_eq!(reopened.path(), path.as_path());

        let s3 = reopened.append_and_fsync(b"three").expect("append three");
        assert_eq!(s3, 3);
    }

    #[test]
    fn wal_reads_entries_from_sequence_with_limit() {
        let dir = tempdir().expect("tempdir");
        let mut wal = Wal::open_or_create(dir.path(), "users").expect("open wal");
        wal.append_and_fsync(b"one").expect("append one");
        wal.append_and_fsync(b"two").expect("append two");
        wal.append_and_fsync(b"three").expect("append three");

        let from_two = wal
            .read_entries_from(2, Some(1))
            .expect("read from sequence");
        assert_eq!(from_two.len(), 1);
        assert_eq!(from_two[0].sequence, 2);
        assert_eq!(from_two[0].payload, b"two");
    }
}
