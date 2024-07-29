use std::{
    collections::HashMap,
    io::{self, Write},
    sync::RwLock,
};

use crate::{
    fs::{Fd, FileSystem},
    ClockSource, System,
};

/// A test file system
pub struct TestFileSystem {
    buffers: RwLock<HashMap<Fd, Vec<u8>>>,
    active: Fd,
}

impl TestFileSystem {
    pub fn new(fd: Fd, map: HashMap<Fd, Vec<u8>>) -> Self {
        Self {
            buffers: RwLock::new(map),
            active: fd,
        }
    }
}

impl FileSystem for TestFileSystem {
    fn write_at(&self, file: Fd, buf: &[u8], offset: u64) -> std::io::Result<usize> {
        let offset = offset as usize;
        let buf = buf.to_owned();
        let len = buf.len();
        self.buffers
            .write()
            .unwrap()
            .get_mut(&file)
            .map(|file_buf| file_buf.splice(offset..offset + buf.len(), buf))
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
        let buf_handle = self.buffers.read().unwrap();

        let Some(file_buf) = buf_handle.get(&file) else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Unable to find file handle: {file}"),
            ));
        };

        let offset = offset as usize;

        if file_buf.len() < offset || (offset + buf.len()) < file_buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "reading past the end of the buffer",
            ));
        }

        buf.write(&file_buf[offset as usize..buf.len()])?;
        Ok(())
    }

    fn file_size(&self, file: crate::fs::Fd) -> std::io::Result<u64> {
        self.buffers
            .write()
            .unwrap()
            .get_mut(&file)
            .map(|file_buf| file_buf.len() as u64)
            .ok_or(io::Error::new(
                io::ErrorKind::NotFound,
                "Unable to find file buf",
            ))
    }

    fn flush(&mut self, _file: crate::fs::Fd) -> std::io::Result<()> {
        Ok(())
    }

    fn active(&self) -> crate::fs::Fd {
        self.active
    }

    fn init(_path: impl Into<std::path::PathBuf>) -> Result<Self, crate::fs::FsError>
    where
        Self: Sized,
    {
        let fd = Fd::new_empty();
        let mut map = HashMap::new();
        map.insert(fd.clone(), vec![]);

        Ok(TestFileSystem::new(fd, map))
    }

    fn new_active(&mut self) -> Result<(), crate::fs::FsError> {
        self.active.increment();

        self.buffers
            .write()
            .unwrap()
            .insert(self.active.clone(), vec![]);

        Ok(())
    }
}

impl ClockSource for TestFileSystem {}

impl System for TestFileSystem {}
unsafe impl Send for TestFileSystem {}
unsafe impl Sync for TestFileSystem {}
