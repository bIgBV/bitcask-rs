use std::{
    fs::{File, Metadata, OpenOptions},
    io::{self, Write},
    os::unix::fs::FileExt,
    path::Path,
    sync::RwLock,
};

use tracing::{debug, info, instrument, trace};

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
    active: T,
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
                active: fs,
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
            size += inner.active.write_at(self.active_fd, &buf, inner.cursor)?;
        }

        // Flush to ensure write is persisted
        inner.active.flush(self.active_fd)?;

        let current = Offset(inner.cursor as usize);
        // Update our cursor into the active file
        inner.cursor += size as u64;
        Ok(CacheEntry {
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
            .active
            .read_exact_at(self.active_fd, buf, offset.0 as u64)?;

        Ok(())
    }

    pub fn active_size(&self) -> Result<u64, FsError> {
        let inner = self.inner.read().expect("Unable to lock active file");
        let metadata = inner.active.metadata(self.active_fd)?;
        Ok(metadata.len())
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
    #[error("IoError: {0}")]
    Io(#[from] io::Error),
}

/// Represents a file descriptor
///
/// This allows FileSystems to have multiple working files, without prescribing how the files or
/// their references are stored.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Fd(usize);

/// Basic file system operations used by the FS layer.
///
/// Trait implementations do not need to be threadsafe.
pub trait FileSystem {
    fn write_at(&self, file: Fd, buf: &[u8], offset: u64) -> io::Result<usize>;
    fn read_exact_at(&self, file: Fd, buf: &mut [u8], offset: u64) -> io::Result<()>;
    fn metadata(&self, file: Fd) -> io::Result<Metadata>;
    fn flush(&mut self, file: Fd) -> io::Result<()>;
    fn active(&self) -> Fd;

    /// Creates a new instace of this FileSystemImpl
    fn create_write(path: impl AsRef<Path>) -> Result<Self, FsError>
    where
        Self: Sized;
}

pub struct SysFileSystem {
    active: Fd,
    active_file: File,
}

impl SysFileSystem {
    fn new(path: impl AsRef<Path>) -> Result<Self, FsError> {
        let path = path.as_ref().join("active.db");
        let file = OpenOptions::new().read(true).write(true).open(dbg!(path))?;
        Ok(SysFileSystem {
            active: Fd(0),
            active_file: file,
        })
    }
}

impl FileSystem for SysFileSystem {
    fn create_write(path: impl AsRef<Path>) -> Result<Self, FsError> {
        SysFileSystem::new(path)
    }

    #[instrument(skip(self, buf))]
    fn write_at(&self, _file: Fd, buf: &[u8], offset: u64) -> io::Result<usize> {
        trace!(active_file = ?self.active_file, write_size = buf.len(), "Writing buf into active file");
        self.active_file.write_at(buf, offset)
    }

    #[instrument(skip(self, buf))]
    fn read_exact_at(&self, _file: Fd, buf: &mut [u8], offset: u64) -> io::Result<()> {
        trace!(active_file = ?self.active_file, read_size = buf.len(), "Reading into buf from active file");
        self.active_file.read_exact_at(buf, offset)
    }

    #[instrument(skip(self))]
    fn metadata(&self, _file: Fd) -> io::Result<Metadata> {
        trace!("Reading metadata for active file");
        self.active_file.metadata()
    }

    #[instrument(skip(self))]
    fn flush(&mut self, _file: Fd) -> io::Result<()> {
        trace!("Flushing to disk");
        self.active_file.flush()
    }

    fn active(&self) -> Fd {
        self.active
    }
}
