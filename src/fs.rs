use std::{
    fs::{File, OpenOptions},
    io,
};

use crate::{Entry, StoredData};

/// Provides a convenient way to interface with the file system
pub(crate) struct Fs {
    active: File,
}

/// An offset of an entry in a data file
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Offset(pub usize);

impl Fs {
    pub fn new(path: &str) -> Result<Self, FsError> {
        let active_file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;
        Ok(Fs {
            active: active_file,
        })
    }

    pub fn write_entry<'entry>(&mut self, entry: Entry<'entry>) -> Result<Offset, FsError> {
        todo!()
    }

    pub fn get_chunk(&self, offset: Offset, buf: &mut [u8]) -> Result<(), FsError> {
        todo!()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FsError {
    #[error("IoError: {0}")]
    Io(#[from] io::Error),
}
