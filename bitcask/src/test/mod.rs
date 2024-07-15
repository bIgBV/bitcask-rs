use crate::fs::{Fd, FileSystem};

pub struct TestFileSystem {
    buf: Vec<u8>,
}

impl TestFileSystem {
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }
}

impl FileSystem for TestFileSystem {
    fn write_at(&self, file: crate::fs::Fd, buf: &[u8], offset: u64) -> std::io::Result<usize> {
        todo!()
    }

    fn read_exact_at(
        &self,
        _file: crate::fs::Fd,
        buf: &mut [u8],
        offset: u64,
    ) -> std::io::Result<()> {
        todo!()
    }

    fn file_size(&self, _file: crate::fs::Fd) -> std::io::Result<u64> {
        Ok(self.buf.len() as u64)
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
