use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
    os::unix::fs::FileExt,
};

use tracing::{info, instrument};

use super::repr::Entry;

/// Provides a convenient way to interface with the file system
#[derive(Debug)]
pub(crate) struct Fs {
    active: File,
}

/// An offset of an entry in a data file
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Offset(pub usize);

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
        })
    }

    #[instrument(skip(self), fields(entry.header))]
    pub fn write_entry<'entry>(&mut self, entry: Entry<'entry>) -> Result<Offset, FsError> {
        info!(
            size = entry.len(),
            "Inserting entry into current active file"
        );
        let buf = entry.serialize();
        // TODO(bhargav): Actually manage offsets into the file
        self.active.write(&buf)?;
        Ok(Offset(entry.len()))
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