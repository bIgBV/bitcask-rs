use std::{
    fs::{File, OpenOptions},
    io,
    os::unix::fs::FileExt,
    sync::RwLock,
};

use tracing::{info, instrument};

use super::{repr::Entry, CacheEntry};

/// An offset of an entry in a data file
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Offset(pub usize);

/// Provides a convenient way to interface with the file system
#[derive(Debug)]
pub(crate) struct Fs {
    inner: RwLock<FsInner>,
}

#[derive(Debug)]
struct FsInner {
    active: File,
    cursor: u64,
}

impl Fs {
    pub fn new(path: &str) -> Result<Self, FsError> {
        let path = format!("{path}/active.db");
        let active_file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&path)?;

        Ok(Fs {
            inner: RwLock::new(FsInner {
                active: active_file,
                cursor: 0,
            }),
        })
    }

    #[instrument(skip(self, entry), fields(entry.header))]
    pub fn write_entry<'entry>(&self, entry: Entry<'entry>) -> Result<CacheEntry, FsError> {
        info!(
            entry_size = entry.len(),
            "Inserting entry into current active file"
        );
        let buf = entry.serialize();

        let mut size = 0;

        // Get write lock on inner struct to linearize writes to the WAL in the active db file.
        let mut inner = self.inner.write().expect("Unable to lock active file");

        while size < buf.len() {
            size += inner.active.write_at(&buf, inner.cursor)?;
        }

        let current = Offset(inner.cursor as usize);
        // Update our cursor into the active file
        inner.cursor += size as u64;
        Ok(CacheEntry {
            value_size: entry.header.value_size,
            offset: current,
            timestamp: entry.header.timestamp,
        })
    }

    #[instrument(skip(self, buf), fields(read_size=buf.len()))]
    /// Reads a chunk the size of the given buffer into the active file at the provided offset
    pub fn get_chunk(&self, offset: Offset, buf: &mut [u8]) -> Result<(), FsError> {
        info!("Reading chunk from active file");
        let inner = self
            .inner
            .read()
            .expect("Unable to obtain read lock on active file");

        inner.active.read_exact_at(buf, offset.0 as u64)?;

        Ok(())
    }

    pub fn active_size(&self) -> Result<u64, FsError> {
        let inner = self.inner.read().expect("Unable to lock active file");
        let metadata = inner.active.metadata()?;
        Ok(metadata.len())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FsError {
    #[error("IoError: {0}")]
    Io(#[from] io::Error),
}
