use std::{
    env,
    fs::{self, File},
    io::{Seek, SeekFrom, Write},
    time::SystemTime,
};

fn main() {
    let mut cask = Cask::new().unwrap();
    cask.insert("hello", "world");
}

struct Cask {
    file_handle: File,
    position: u64,
}

impl Cask {
    fn new() -> Result<Self, ()> {
        let path = format!("./active.db");
        let file = File::create_new(path).unwrap();

        Ok(Cask {
            file_handle: file,
            position: 0,
        })
    }

    pub fn insert<K, V>(&mut self, key: K, value: V)
    where
        K: AsBytes,
        V: AsBytes,
    {
        let header = encode(&key, &value);
        let key = key.as_bytes();
        let value = value.as_bytes();

        self.file_handle
            .seek(SeekFrom::Start(self.position))
            .unwrap();

        self.file_handle
            .write_all(&header.timestamp.to_le_bytes())
            .unwrap();
        self.file_handle
            .write_all(&header.key_size.to_le_bytes())
            .unwrap();
        self.file_handle
            .write_all(&header.value_size.to_le_bytes())
            .unwrap();
        self.file_handle.write_all(key).unwrap();
        self.file_handle.write_all(value).unwrap();
    }
}

#[derive(Debug)]
#[repr(C)]
/// Database entry header
struct Header {
    timestamp: u64,
    key_size: u16,
    value_size: u32,
}

pub trait AsBytes {
    fn as_bytes(&self) -> &[u8];
}

impl AsBytes for String {
    fn as_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl AsBytes for &str {
    fn as_bytes(&self) -> &[u8] {
        str::as_bytes(&self)
    }
}

fn encode<K, V>(key: &K, value: &V) -> Header
where
    K: AsBytes,
    V: AsBytes,
{
    // TODO: calling as_bytes everytime might be costly
    let key_len = key.as_bytes().len();
    let val_len = value.as_bytes().len();

    debug_assert!((key_len as u16) < u16::MAX);
    debug_assert!((val_len as u32) < u32::MAX);

    // TODO: This needs to be made deterministic for tests
    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    Header {
        key_size: key_len as u16,
        value_size: val_len as u32,
        timestamp,
    }
}

#[cfg(test)]
mod test {
    use crate::encode;

    #[test]
    fn simple_encode() {
        let key = "hello";
        let val = "world";
        let header = encode(key, val);

        assert_eq!(header.key_size as usize, key.as_bytes().len())
    }
}
