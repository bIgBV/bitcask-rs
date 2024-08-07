use std::{
    cell::RefCell,
    collections::HashMap,
    io::{self, Write},
    sync::Arc,
};

use tracing::{info, instrument, trace};

use crate::{
    fs::{Fd, FileSystem},
    ClockSource, System,
};

/// A test file system
///
/// Implementors of the `FileSystem` trait do not need to be threadsafe (this might change in the
/// future). Therefore, we can safely implement interior mutablity without using a locking scheme.
///
/// Interior mutability is required since we need to be able to modify the buffers backing the
/// in-memory files in the file system.
pub struct TestFileSystem {
    inner: Arc<RefCell<TestFsInner>>,
}

#[derive(Debug)]
struct TestFsInner {
    buffers: HashMap<Fd, TestFile>,
    active: Fd,
}

impl Clone for TestFileSystem {
    fn clone(&self) -> Self {
        TestFileSystem {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl TestFileSystem {
    fn new(fd: Fd, map: HashMap<Fd, TestFile>) -> Self {
        Self {
            inner: Arc::new(RefCell::new(TestFsInner {
                buffers: map,
                active: fd,
            })),
        }
    }

    pub fn num_files(&self) -> usize {
        self.inner.as_ref().borrow().buffers.len()
    }
}

impl FileSystem for TestFileSystem {
    #[instrument(skip(self, buf))]
    fn write_at(&self, file: Fd, buf: &[u8], offset: u64) -> std::io::Result<usize> {
        let offset = offset as usize;
        let buf = buf.to_owned();
        let len = buf.len();
        self.inner
            .as_ref()
            .borrow_mut()
            .buffers
            .get_mut(&file)
            .map(|file_buf| {
                info!(fd = ?file, len = buf.len(), "Writing to file");
                file_buf.write_at(offset, &buf);
            })
            .ok_or(io::Error::new(
                io::ErrorKind::NotFound,
                "Unable to find file buf",
            ))?;
        Ok(len)
    }

    fn read_exact_at(
        &self,
        file: crate::fs::Fd,
        mut buf: &mut [u8],
        offset: u64,
    ) -> std::io::Result<()> {
        let buf_handle = &self.inner.as_ref().borrow().buffers;

        let Some(file_buf) = buf_handle.get(&file) else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Unable to find file handle: {file}"),
            ));
        };

        let offset = offset as usize;
        if dbg!(file_buf.len() < offset) || dbg!((offset + buf.len()) > file_buf.len()) {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "reading past the end of the buffer",
            ));
        }

        info!(fd = ?file, len = buf.len(), "Reading from file");
        file_buf.read_at(offset, &mut buf)?;
        Ok(())
    }

    fn file_size(&self, file: crate::fs::Fd) -> std::io::Result<u64> {
        self.inner
            .as_ref()
            .borrow()
            .buffers
            .get(&file)
            .map(|file_buf| {
                trace!(file = ?file, len = file_buf.len(), "File size");
                file_buf.len() as u64
            })
            .ok_or(io::Error::new(
                io::ErrorKind::NotFound,
                "Unable to find file buf",
            ))
    }

    fn flush(&mut self, _file: crate::fs::Fd) -> std::io::Result<()> {
        Ok(())
    }

    fn active(&self) -> crate::fs::Fd {
        self.inner.as_ref().borrow().active
    }

    fn init(_path: impl Into<std::path::PathBuf>) -> Result<Self, crate::fs::FsError>
    where
        Self: Sized,
    {
        let fd = Fd::new_empty();
        let mut map = HashMap::new();
        map.insert(fd.clone(), TestFile::new());

        Ok(TestFileSystem::new(fd, map))
    }

    #[instrument(skip(self))]
    fn new_active(&mut self) -> Result<Fd, crate::fs::FsError> {
        self.inner.as_ref().borrow_mut().active.increment();
        let new_active = self.inner.as_ref().borrow().active.clone();

        self.inner.as_ref().borrow_mut().active = new_active;

        trace!("Swapping current active file");
        self.inner
            .as_ref()
            .borrow_mut()
            .buffers
            .insert(new_active, TestFile::new());

        Ok(new_active)
    }
}

impl System for TestFileSystem {}
impl ClockSource for TestFileSystem {}

unsafe impl Send for TestFileSystem {}
unsafe impl Sync for TestFileSystem {}

#[derive(Debug)]
struct TestFile {
    buf: Vec<u8>,
    pos: usize,
}

impl TestFile {
    fn new() -> Self {
        Self {
            buf: vec![0; 64],
            pos: 0,
        }
    }

    fn write_at(&mut self, offset: usize, buf: &[u8]) {
        let buf_end = offset + buf.len();

        if buf_end > self.buf.len() {
            self.buf.resize(buf_end, 0);
        }

        for i in 0..buf.len() {
            self.buf[offset + i] = buf[i]
        }
        self.pos += buf.len();
    }

    #[instrument(skip(self, buf))]
    fn read_at(&self, offset: usize, mut buf: &mut [u8]) -> io::Result<usize> {
        // TODO: handle reads past the cursor
        info!(
            offset = offset,
            file_len = self.buf.len(),
            "reading from test file"
        );
        buf.write(&self.buf[offset..offset + buf.len()])
    }

    fn len(&self) -> usize {
        self.pos
    }
}
