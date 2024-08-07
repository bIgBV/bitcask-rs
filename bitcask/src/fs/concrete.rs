use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io,
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};

use tracing::{instrument, trace};

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

    fn create_or_swap_active(&mut self) -> Result<Fd, FsError> {
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

        Ok(fd)
    }
}

impl FileSystem for ConcreteSystem {
    fn init(path: impl Into<PathBuf>) -> Result<Self, FsError> {
        let mut system = ConcreteSystem::new(path);
        system.new_active()?;

        Ok(system)
    }

    fn new_active(&mut self) -> Result<Fd, FsError> {
        Ok(self.create_or_swap_active()?)
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
