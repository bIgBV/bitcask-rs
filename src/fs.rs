use crate::{Entry, StoredData};

/// Provides a convenient way to interface with the file system
pub(crate) struct Fs {}

/// An offset of an entry in a data file
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Offset(pub usize);

impl Fs {
    pub fn new() -> Self {
        Fs {}
    }

    pub fn write_entry<K, V>(&mut self, entry: Entry<K, V>) -> Result<Offset, FsError>
    where
        K: StoredData,
        V: StoredData,
    {
        todo!()
    }

    pub fn get_chunk(&self, offset: Offset, buf: &mut [u8]) -> Result<(), FsError> {
        todo!()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FsError {}
