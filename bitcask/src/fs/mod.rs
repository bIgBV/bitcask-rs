mod concrete;

pub use concrete::ConcreteSystem;
use std::{backtrace::Backtrace, fmt, io, path::PathBuf, sync::RwLock};

use tracing::{debug, info, instrument, trace};

use super::{repr::Entry, CacheEntry};

/// An offset of an entry in a data file
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Offset(pub usize);

/// Provides a convenient way to interface with the file system
#[derive(Debug)]
pub(crate) struct Fs<T> {
    inner: RwLock<FsInner<T>>,
}

#[derive(Debug)]
struct FsInner<T> {
    fs_impl: T,
    cursor: u64,
    active_fd: Fd,
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
                active_fd: active,
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
        let current_active = inner.active_fd;

        debug!(pos = inner.cursor);

        while size < buf.len() {
            size += inner.fs_impl.write_at(current_active, &buf, inner.cursor)?;
        }

        // Flush to ensure write is persisted
        inner.fs_impl.flush(current_active)?;

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
            .read_exact_at(inner.active_fd, buf, offset.0 as u64)?;

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
        Ok(inner.cursor)
    }

    pub fn active_fd(&self) -> Fd {
        let inner = self.inner.read().expect("Unable to lock active file");
        inner.active_fd
    }

    #[instrument(skip(self))]
    pub fn swap_active(&self) -> Result<(), FsError> {
        let mut inner = self.inner.write().unwrap();
        let new_active = inner.fs_impl.new_active()?;
        trace!(new_active = ?new_active, "Swapping active file");

        // Update the active Fd and make sure to reset the cursor into the new file
        inner.active_fd = new_active;
        inner.cursor = 0;
        Ok(())
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
    fn new_active(&mut self) -> Result<Fd, FsError>;
}
