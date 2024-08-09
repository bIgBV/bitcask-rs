#![feature(error_generic_member_access)]

//! A embedded hash backed log structured key value store.
//!
//! [`Cask`] implements a variation of the data store described in the classic Bitcask paper. It is
//! threadsafe, and supports pluggable storage _and_ system interfaces. This allows us to implement
//! deterministic tests.

mod compactor;
mod fs;
mod pool;
mod repr;
pub mod test;

use compactor::Compactor;
pub use fs::{ConcreteSystem, FileSystem};
use pool::Pool;

use std::{
    collections::HashMap,
    hash::Hash,
    sync::{Arc, RwLock},
};

use bytemuck::PodCastError;
use fs::{Fd, Fs, FsError, Offset};
use repr::{Entry, EntryError, Header};
use tracing::{debug, info, instrument};

/// Knobs for tuning the behavior of the data store.
#[derive(Debug, Clone)]
pub struct Config {
    /// Upper bound of the size of the active file in bytes.
    ///
    /// Note: the actual size on the file will be one entry larger than this threshold.
    pub active_threshold: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            active_threshold: 4096,
        }
    }
}

#[derive(Clone)]
pub struct Cask<T> {
    inner: Arc<Inner<T>>,
    config: Config,
}

struct Inner<T> {
    fs: Fs<T>,
    // This can be a RwLock
    keydir: RwLock<HashMap<Vec<u8>, CacheEntry>>,
    pool: Pool,
}

impl<T> Cask<T>
where
    T: System,
{
    #[instrument]
    pub fn new_with_config(path: &str, config: Config) -> Result<Self, CaskError> {
        let fs_impl = T::init(path)?;

        Ok(Cask::new_with_fs_impl(path, config, fs_impl)?)
    }

    #[instrument(skip(fs_impl))]
    pub fn new_with_fs_impl(path: &str, config: Config, fs_impl: T) -> Result<Self, CaskError> {
        let fs = Fs::new(fs_impl)?;

        let size = fs.active_size()?;
        // We already have an active db. Build KeyDir
        let keydir = if size > 0 {
            info!(file_size = size, "Active db exists");
            let iterator = HeaderIter {
                active_fd: fs.active_fd(),
                fs: &fs,
                current: Offset(0),
            };

            let mut map = HashMap::new();

            for entry in iterator {
                let (key, cache_entry) = entry?;
                map.insert(key, cache_entry);
            }

            // Update FS cursor to the end of the file
            fs.update_cursor(fs.active_size()?);
            map
        } else {
            HashMap::new()
        };

        Ok(Cask {
            inner: Arc::new(Inner {
                fs,
                keydir: RwLock::new(keydir),
                pool: Pool::new(4),
            }),
            config,
        })
    }

    #[instrument]
    pub fn new(path: &str) -> Result<Self, CaskError> {
        Cask::new_with_config(path, Config::default())
    }

    pub fn init(self) -> Self {
        // todo: parameterize
        for _ in 0..2 {
            // Create a new Cask instance which is a copy of the innser struct to ensure that the
            // whole clone is moved into each background thread closure.
            let new_inner = self.inner.clone();
            let new_cask = Cask {
                inner: new_inner,
                config: Config::default(),
            };

            self.inner.pool.execute(move || {
                new_cask.compaction_loop();
            });
        }

        self
    }

    /// Inserts a new entry into the data store
    ///
    /// The keys and values should be serializable, which is done via the `StoredData` trait.
    ///
    /// ```rust
    /// # use std::error::Error;
    /// # use bitcask::{Cask, test::TestFileSystem};
    /// # fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    ///     let cask: Cask<TestFileSystem> = Cask::new("")?;
    ///     cask.insert("hello", "world")?;
    ///     # Ok(())
    /// # }
    /// ```
    pub fn insert<K, V>(&self, key: K, value: V) -> Result<(), CaskError>
    where
        K: AsRef<[u8]> + Hash + Eq,
        V: AsRef<[u8]>,
    {
        let entry = Entry::new_encoded(&key, &value)?;
        let entry = self.inner.fs.write_entry(entry)?;

        // A branch requring a mutex on every insert could get expensive
        if self.inner.fs.active_size()? as usize >= self.config.active_threshold {
            self.inner.fs.swap_active()?;
        }

        // TODO: Can we get away from allocating a whole new vec for every key?
        // IMO no? We need to own the data for the type in this container.
        let key = key.as_ref().into();

        self.inner
            .keydir
            .write()
            .expect("Unable to lock hashmap mutex")
            .entry(key)
            .and_modify(|cache_entry| *cache_entry = entry.clone())
            .or_insert_with(|| entry);

        Ok(())
    }

    /// Gets an entry from the data store if it's present
    ///
    /// ```rust
    /// # use std::error::Error;
    /// # use bitcask::{Cask, test::TestFileSystem};
    /// # fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    ///     let cask: Cask<TestFileSystem> = Cask::new("")?;
    ///     cask.insert("hello", "world")?;
    ///     assert_eq!(cask.get(&"hello")?, "world".as_bytes());
    ///     # Ok(())
    /// # }
    /// ```
    pub fn get<K>(&self, key: &K) -> Result<Vec<u8>, CaskError>
    where
        K: AsRef<[u8]> + Hash + Eq,
    {
        let entry = self.inner.keydir.read().unwrap();
        let Some(cache_entry) = entry.get(key.as_ref()) else {
            return Err(CaskError::NotFound);
        };

        let mut buf = [0u8; Header::LEN as usize];
        self.inner.fs.get_chunk(cache_entry.offset, &mut buf)?;
        let header: &Header = bytemuck::try_from_bytes(&buf).map_err(CaskError::Cast)?;

        let data_len = header.data_size();
        let mut buf = vec![0u8; data_len as usize];
        self.inner
            .fs
            .get_chunk(cache_entry.data_offset(), &mut buf)?;

        let value = &buf[header.key_size as usize..];

        Ok(value.into())
    }

    /// Delete an entry from the data store
    pub fn remove<K>(&self, key: &K) -> Result<(), CaskError>
    where
        K: AsRef<[u8]> + Hash + Eq,
    {
        // TODO: Can we get away from allocating a whole vec for every key?
        // IMO no? We need to own the data for the type in this container.
        let tombstone = Entry::new_empty(key);
        let key = key.as_ref().into();

        if let Some(_) = self.inner.keydir.write().unwrap().remove(key) {
            let _entry = self.inner.fs.write_entry(tombstone)?;
        }
        Ok(())
    }
}

// Compaction impl
impl<T> Cask<T>
where
    T: System,
{
    #[instrument(skip(self))]
    pub(crate) fn compaction_loop(self: Self) {
        let mut compactor = Compactor::new();

        let operation = compactor
            .poll_transmit()
            .expect("First poll is always present");
    }
}

pub(crate) struct HeaderIter<'cask, T> {
    fs: &'cask Fs<T>,
    active_fd: Fd,
    current: Offset,
}

impl<'cask, T> Iterator for HeaderIter<'cask, T>
where
    T: System,
{
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
                fd: self.active_fd,
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

pub trait System: FileSystem + ClockSource + Send + Sync + 'static {}

pub trait ClockSource {}

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

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct CacheEntry {
    fd: Fd,
    value_size: u32,
    offset: Offset,
    timestamp: u64,
}

impl CacheEntry {
    pub fn data_offset(&self) -> Offset {
        Offset(self.offset.0 + Header::LEN as usize)
    }
}
