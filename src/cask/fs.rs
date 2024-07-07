use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
    os::unix::fs::FileExt,
};

use tracing::{info, instrument};

use super::{repr::Entry, CacheEntry};

/// An offset of an entry in a data file
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Offset(pub usize);

/// Provides a convenient way to interface with the file system
#[derive(Debug)]
pub(crate) struct Fs {
    active: File,
    cursor: usize,
}

impl Fs {
    pub fn new(path: &str) -> Result<Self, FsError> {
        let path = format!("{path}/active.db");
        let active_file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        Ok(Fs {
            active: active_file,
            cursor: 0,
        })
    }

    #[instrument(skip(self), fields(entry.header))]
    pub fn write_entry<'entry>(&mut self, entry: Entry<'entry>) -> Result<CacheEntry, FsError> {
        info!(
            size = entry.len(),
            "Inserting entry into current active file"
        );
        let buf = entry.serialize();
        let current = Offset(self.cursor);
        // Active file is opened in write mode. Therefore all writes are always appended to the
        // file.
        let offset = self.active.write(&buf)?;

        // Update our cursor into the active file
        self.cursor += offset;
        Ok(CacheEntry {
            value_size: entry.header.value_size,
            offset: current,
            timestamp: entry.header.timestamp,
        })
    }

    #[instrument(skip(self, buf))]
    // Reads a chunk the size of the given buffer into the active file at the provided offset
    pub fn get_chunk(&self, offset: Offset, buf: &mut [u8]) -> Result<(), FsError> {
        info!("Reading chunk from active file");
        self.active.read_exact_at(buf, offset.0 as u64)?;

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FsError {
    #[error("IoError: {0}")]
    Io(#[from] io::Error),
}
