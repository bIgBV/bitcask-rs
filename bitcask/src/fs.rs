use core::fmt;
use std::{
    backtrace::Backtrace,
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    os::unix::fs::FileExt,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock,
    },
};

use tracing::{debug, info, instrument, trace};

use crate::{ClockSource, System};

use super::{repr::Entry, CacheEntry};

/// An offset of an entry in a data file
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Offset(pub usize);

/// Provides a convenient way to interface with the file system
#[derive(Debug)]
pub(crate) struct Fs<T> {
    inner: RwLock<FsInner<T>>,
    active_fd: Fd,
}

#[derive(Debug)]
struct FsInner<T> {
    fs_impl: T,
    cursor: u64,
}

impl<T> Fs<T>
where
    T: FileSystem,
{
    pub fn new(fs: T) -> Result<Self, FsError> {
        let active = fs.active();
        Ok(Fs {
            inner: RwLock::new(FsInner {
                fs_impl: fs,
                cursor: 0,
            }),
            active_fd: active,
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

        debug!(pos = inner.cursor);

        while size < buf.len() {
            size += inner.fs_impl.write_at(self.active_fd, &buf, inner.cursor)?;
        }

        // Flush to ensure write is persisted
        inner.fs_impl.flush(self.active_fd)?;

        let current = Offset(inner.cursor as usize);
        // Update our cursor into the active file
        inner.cursor += size as u64;
        Ok(CacheEntry {
            fd: inner.fs_impl.active(),
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

        inner
            .fs_impl
            .read_exact_at(self.active_fd, buf, offset.0 as u64)?;

        Ok(())
    }

    /// Get a chunk of buf.len() from file associated with given Fd
    #[instrument(skip(self, buf))]
    pub fn get_chunk_fd(&self, offset: Offset, buf: &mut [u8], fd: Fd) -> Result<(), FsError> {
        info!(fd = ?fd, "reading chunk from immutable file");

        let inner = self
            .inner
            .read()
            .expect("Unable to obtain read lock on active file");

        inner.fs_impl.read_exact_at(fd, buf, offset.0 as u64)?;

        Ok(())
    }

    pub fn active_size(&self) -> Result<u64, FsError> {
        let inner = self.inner.read().expect("Unable to lock active file");
        Ok(inner.fs_impl.file_size(self.active_fd)?)
    }

    pub fn active_fd(&self) -> Fd {
        let inner = self.inner.read().expect("Unable to lock active file");
        inner.fs_impl.active()
    }

    pub fn swap_active(&self) -> Result<(), FsError> {
        self.inner.write().unwrap().fs_impl.new_active()
    }
}

impl<T> Fs<T> {
    pub fn update_cursor(&self, loc: u64) {
        let mut inner = self.inner.write().expect("Unable to obtain write lock");
        inner.cursor = loc;
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FsError {
    #[error("IoError: {source}")]
    Io {
        #[from]
        source: io::Error,
        backtrace: Backtrace,
    },
}

/// Represents a file descriptor
///
/// This allows FileSystems to have multiple working files, without prescribing how the files or
/// their references are stored.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Fd(usize);

impl Fd {
    pub fn new_empty() -> Self {
        Fd(0)
    }

    pub fn increment(&mut self) {
        self.0 = self.0 + 1;
    }
}

impl fmt::Display for Fd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Fd({})", self.0)
    }
}

/// Basic file system operations used by the FS layer.
///
/// Trait implementations do not need to be threadsafe.
pub trait FileSystem {
    fn write_at(&self, file: Fd, buf: &[u8], offset: u64) -> io::Result<usize>;
    fn read_exact_at(&self, file: Fd, buf: &mut [u8], offset: u64) -> io::Result<()>;
    fn file_size(&self, file: Fd) -> io::Result<u64>;
    fn flush(&mut self, file: Fd) -> io::Result<()>;
    fn active(&self) -> Fd;

    /// Creates a new instace of this FileSystemImpl
    fn init(path: impl Into<PathBuf>) -> Result<Self, FsError>
    where
        Self: Sized;
    fn new_active(&mut self) -> Result<(), FsError>;
}

/// Implements the FileSystem interface for an actual system.
///
/// This structure does not need to be threadsafe as it is used within the `Fs` struct and wrapped
/// with a lock there.
pub struct ConcreteSystem {
    fd_num: AtomicUsize,
    active: Fd,
    map: HashMap<Fd, File>,
    cask_path: PathBuf,
}

impl ConcreteSystem {
    fn new(cask_path: impl Into<PathBuf>) -> Self {
        ConcreteSystem {
            fd_num: AtomicUsize::new(1),
            active: Fd(1),
            map: HashMap::new(),
            cask_path: cask_path.into(),
        }
    }

    fn next_fd(&self) -> Fd {
        Fd(self.fd_num.fetch_add(1, Ordering::Relaxed))
    }

    fn create_or_swap_active(&mut self) -> Result<(), FsError> {
        let has_active = fs::read_dir(self.cask_path.clone())?
            .any(|entry| entry.map_or(false, |entry| entry.file_name() == "active.db"));

        if has_active {
            // we have an existing active file, move it into immutable
            let current_active = self.cask_path.join("active.db");
            let active_fd = self.active().0;
            let new_immutable = self.cask_path.join(format!("immutable-{active_fd}.db"));

            fs::rename(current_active, &new_immutable)?;
            let fd = self.next_fd();
            let new_immutable_file = File::open(new_immutable)?;
            self.map.insert(fd, new_immutable_file);
        }

        // Create new active file
        let active_path = self.cask_path.join("active.db");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(dbg!(active_path))?;

        let fd = self.next_fd();
        self.map.insert(fd, file);

        self.active = fd;

        Ok(())
    }
}

impl FileSystem for ConcreteSystem {
    fn init(path: impl Into<PathBuf>) -> Result<Self, FsError> {
        let mut system = ConcreteSystem::new(path);
        system.new_active()?;

        Ok(system)
    }

    fn new_active(&mut self) -> Result<(), FsError> {
        self.create_or_swap_active()?;
        Ok(())
    }

    #[instrument(skip(self, buf))]
    fn write_at(&self, file: Fd, buf: &[u8], offset: u64) -> io::Result<usize> {
        if let Some(file) = self.map.get(&file) {
            trace!(file = ?file, write_size = buf.len(), "Writing buf into file");
            return file.write_at(buf, offset);
        }
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("Unable to fine file with fd: {}", file),
        ))
    }

    #[instrument(skip(self, buf))]
    fn read_exact_at(&self, file: Fd, buf: &mut [u8], offset: u64) -> io::Result<()> {
        if let Some(file) = self.map.get(&file) {
            trace!(file = ?file, read_size = buf.len(), "Reading into buf from file");
            return file.read_exact_at(buf, offset);
        }
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("Unable to fine file with fd: {}", file),
        ))
    }

    #[instrument(skip(self))]
    fn file_size(&self, file: Fd) -> io::Result<u64> {
        if let Some(file) = self.map.get(&file) {
            trace!("Reading metadata for active file");
            return Ok(file.metadata()?.len());
        }
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("Unable to fine file with fd: {}", file),
        ))
    }

    #[instrument(skip(self))]
    fn flush(&mut self, file: Fd) -> io::Result<()> {
        if let Some(file) = self.map.get_mut(&file) {
            trace!("Flushing to disk");
            return Ok(file.flush()?);
        }
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("Unable to fine file with fd: {}", file),
        ))
    }

    fn active(&self) -> Fd {
        self.active
    }
}

impl ClockSource for ConcreteSystem {}

impl System for ConcreteSystem {}

unsafe impl Send for ConcreteSystem {}
unsafe impl Sync for ConcreteSystem {}
