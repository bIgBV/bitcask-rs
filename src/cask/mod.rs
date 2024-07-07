mod fs;
mod repr;

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::RwLock;

use bytemuck::PodCastError;
use fs::{Fs, FsError, Offset};
use repr::StoredData;
use repr::{Entry, EntryError, Header};

pub struct Cask<K> {
    fs: Fs,
    keydir: HashMap<K, RwLock<CacheEntry>>,
}

// todo add file ids
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct CacheEntry {
    value_size: u32,
    offset: Offset,
    timestamp: u64,
}

impl CacheEntry {
    pub fn data_offset(&self) -> Offset {
        Offset(self.offset.0 + Header::LEN as usize)
    }
}

impl<K> Cask<K> {
    pub fn new(path: &str) -> Result<Self, ()> {
        Ok(Cask {
            fs: Fs::new(path).unwrap(),
            keydir: HashMap::new(),
        })
    }

    /// Inserts a new entry into the data store
    pub fn insert<V>(&mut self, key: K, value: V) -> Result<(), CaskError>
    where
        K: StoredData + Hash + Eq,
        V: StoredData,
    {
        let entry = Entry::new_encoded(&key, &value)?;
        let entry = self.fs.write_entry(entry)?;

        self.keydir.insert(key, RwLock::new(entry));

        Ok(())
    }

    /// Gets an entry from the data store if it's present
    pub fn get(&mut self, key: K) -> Result<Vec<u8>, CaskError>
    where
        K: Hash + Eq,
    {
        let Some(cache_entry) = self.keydir.get(&key) else {
            return Err(CaskError::NotFound);
        };

        let cache_entry = cache_entry
            .read()
            .expect("Unable to obtain read lock for entry");

        let mut buf = [0u8; Header::LEN as usize];
        self.fs.get_chunk(cache_entry.offset, &mut buf)?;
        let header: &Header = bytemuck::try_from_bytes(&buf).map_err(CaskError::Cast)?;

        let data_len = header.data_size();
        let mut buf = vec![0u8; data_len as usize];
        self.fs.get_chunk(cache_entry.data_offset(), &mut buf)?;

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

    #[error("Entry not found")]
    NotFound,
}
