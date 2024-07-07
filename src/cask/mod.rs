mod fs;
mod repr;

use bytemuck::PodCastError;
use fs::{Fs, FsError, Offset};
use repr::StoredData;
use repr::{Entry, EntryError, Header};
use tracing::instrument;

pub struct Cask {
    fs: Fs,
}

impl Cask {
    pub fn new(path: &str) -> Result<Self, ()> {
        Ok(Cask {
            fs: Fs::new(path).unwrap(),
        })
    }

    /// Inserts a new entry into the data store
    pub fn insert<K, V>(&mut self, key: K, value: V) -> Result<Offset, CaskError>
    where
        K: StoredData,
        V: StoredData,
    {
        let entry = Entry::new_encoded(&key, &value)?;

        Ok(self.fs.write_entry(entry)?)
    }

    /// Gets an entry from the data store if it's present
    pub fn get<K>(&mut self, key: K) -> Result<Vec<u8>, CaskError> {
        let mut buf = [0u8; Header::LEN as usize];
        self.fs.get_chunk(Offset(0), &mut buf)?;
        let header: &Header = bytemuck::try_from_bytes(&buf).map_err(CaskError::Cast)?;

        let data_len = header.data_size();
        let mut buf = vec![0u8; data_len as usize];
        self.fs.get_chunk(Offset(Header::LEN as usize), &mut buf)?;

        let value = &buf[header.key_size as usize..];

        Ok(value.into())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum CaskError {
    #[error("Error interacting with the filesystem: {0}")]
    Fs(#[from] FsError),

    #[error("Error casting value: {0}")]
    Cast(PodCastError),

    #[error("Encoding error: {0}")]
    Entry(#[from] EntryError),
}
