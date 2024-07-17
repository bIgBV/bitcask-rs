use std::{
    io::{self, Write},
    sync::RwLock,
};

use crate::fs::{Fd, FileSystem};

/// A test file system
pub struct TestFileSystem {
    buf: RwLock<Vec<u8>>,
}

impl TestFileSystem {
    pub fn new() -> Self {
        Self {
            buf: RwLock::new(Vec::with_capacity(4096)),
        }
    }
}

impl FileSystem for TestFileSystem {
    fn write_at(&self, _file: Fd, buf: &[u8], offset: u64) -> std::io::Result<usize> {
        let offset = offset as usize;
        let buf = buf.to_owned();
        let len = buf.len();
        self.buf
            .write()
            .unwrap()
            .splice(offset..offset + buf.len(), buf);
        Ok(len)
    }

    fn read_exact_at(
        &self,
        _file: crate::fs::Fd,
        mut buf: &mut [u8],
        offset: u64,
    ) -> std::io::Result<()> {
        if offset < self.buf.read().unwrap().len() as u64 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "reading past the end of the buffer",
            ));
        }

        buf.write(&self.buf.read().unwrap()[offset as usize..buf.len()])?;
        Ok(())
    }

    fn file_size(&self, _file: crate::fs::Fd) -> std::io::Result<u64> {
        Ok(self.buf.read().unwrap().len() as u64)
    }

    fn flush(&mut self, _file: crate::fs::Fd) -> std::io::Result<()> {
        Ok(())
    }

    fn active(&self) -> crate::fs::Fd {
        Fd::new_empty()
    }

    fn create_write(_path: impl AsRef<std::path::Path>) -> Result<Self, crate::fs::FsError>
    where
        Self: Sized,
    {
        Ok(TestFileSystem::new())
    }
}
