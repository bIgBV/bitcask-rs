mod fs;
mod repr;

use std::{collections::HashMap, hash::Hash, sync::RwLock};

use bytemuck::PodCastError;
use fs::{Fs, FsError, Offset};
use repr::{Entry, EntryError, Header, StoredData};
use tracing::{debug, info, instrument};

pub struct Cask {
    fs: Fs,
    keydir: HashMap<Vec<u8>, RwLock<CacheEntry>>,
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

impl Cask {
    #[instrument]
    pub fn new(path: &str) -> Result<Self, CaskError> {
        let fs = Fs::new(path)?;

        let size = fs.active_size()?;
        // We already have an active db. Build KeyDir
        let keydir = if size > 0 {
            info!(file_size = size, "Active db exists");
            let iterator = EntryIter {
                fs: &fs,
                current: Offset(0),
            };

            let mut map = HashMap::new();

            for entry in iterator {
                let (key, cache_entry) = entry?;
                map.insert(key, RwLock::new(cache_entry));
            }

            map
        } else {
            HashMap::new()
        };

        Ok(Cask { fs, keydir })
    }

    /// Inserts a new entry into the data store
    pub fn insert<K, V>(&mut self, key: K, value: V) -> Result<(), CaskError>
    where
        K: StoredData + Hash + Eq,
        V: StoredData,
    {
        let entry = Entry::new_encoded(&key, &value)?;
        let entry = self.fs.write_entry(entry)?;

        // TODO: Can we get away from allocating a whole vec for every key?
        // IMO no? We need to own the data for the type in this container.
        let key = key.as_bytes().into();

        self.keydir
            .entry(key)
            .and_modify(|cache_entry| *cache_entry.write().unwrap() = entry.clone())
            .or_insert_with(|| RwLock::new(entry));

        Ok(())
    }

    /// Gets an entry from the data store if it's present
    pub fn get<K>(&self, key: &K) -> Result<Vec<u8>, CaskError>
    where
        K: StoredData + Hash + Eq,
    {
        let Some(cache_entry) = self.keydir.get(key.as_bytes()) else {
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

    pub fn remove<K>(&mut self, key: &K) -> Result<(), CaskError>
    where
        K: StoredData + Hash + Eq,
    {
        let tombstone = Entry::new_empty(key);
        let entry = self.fs.write_entry(tombstone)?;

        // TODO: Can we get away from allocating a whole vec for every key?
        // IMO no? We need to own the data for the type in this container.
        let key = key.as_bytes().into();

        self.keydir.remove(key);
        Ok(())
    }
}

pub(in crate::cask) struct EntryIter<'cask> {
    fs: &'cask Fs,
    current: Offset,
}

impl<'cask> Iterator for EntryIter<'cask> {
    type Item = Result<(Vec<u8>, CacheEntry), CaskError>;

    #[instrument(skip(self))]
    fn next(&mut self) -> Option<Self::Item> {
        let file_size = match self.fs.active_size() {
            Ok(size) => size,
            Err(err) => return Some(Err(err.into())),
        };

        if self.current.0 < file_size as usize {
            debug!(offset = self.current.0, "reading another entry");

            let mut buf = [0u8; Header::LEN as usize];
            match self.fs.get_chunk(self.current, &mut buf) {
                Ok(()) => (),
                Err(err) => return Some(Err(err.into())),
            };
            let header: &Header = match bytemuck::try_from_bytes(&buf) {
                Ok(header) => header,
                Err(err) => return Some(Err(CaskError::Cast(err))),
            };

            let mut buf = vec![0u8; header.key_size as usize];
            match self
                .fs
                .get_chunk(Offset(self.current.0 + Header::LEN as usize), &mut buf)
            {
                Ok(()) => (),
                Err(err) => return Some(Err(err.into())),
            };

            let cache_entry = CacheEntry {
                value_size: header.value_size,
                offset: self.current,
                timestamp: header.timestamp,
            };

            self.current = Offset(self.current.0 + header.entry_size());

            return Some(Ok((buf, cache_entry)));
        }

        None
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
