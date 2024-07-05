use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    mem,
    os::unix::fs::FileExt,
    time::SystemTime,
};

fn main() {
    let mut cask = Cask::new().unwrap();
    cask.insert("hello", "world 🧏‍♀️");
    let result = cask.get("hello").unwrap();

    let val = String::from_utf8(result).unwrap();
    println!("{val}");
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

    // todo: we can issue a single read to the file by pre-calculating the size of the buffer
    pub fn get<K>(&mut self, key: K) -> Option<Vec<u8>> {
        let mut buf = [0u8; 8];
        self.file_handle
            .read_at(&mut buf, Header::TIMESTAMP_OFFSET)
            .unwrap();
        let timestamp = u64::from_le_bytes(buf);
        println!("{timestamp}");

        let mut buf = [0u8; 2];
        self.file_handle
            .read_at(&mut buf, Header::KEY_SIZE_OFFSET)
            .unwrap();
        let key_size = u16::from_le_bytes(buf);

        let mut buf = [0u8; 4];
        self.file_handle
            .read_at(&mut buf, Header::VALUE_SIZE_OFFSET)
            .unwrap();
        let val_size = u32::from_le_bytes(buf);

        let mut buf = vec![0; key_size.into()];
        self.file_handle
            .seek(SeekFrom::Start(Header::KEY_OFFSET))
            .unwrap();

        self.file_handle.read_exact(&mut buf).unwrap();

        let key = String::from_utf8(buf).unwrap();
        dbg!(key);

        let val_offset = Header::KEY_OFFSET + key_size as u64;
        self.file_handle
            .seek(SeekFrom::Start(val_offset as u64))
            .unwrap();
        let mut buf = vec![0; val_size.try_into().unwrap()];

        self.file_handle.read_exact(&mut buf).unwrap();

        Some(buf)
    }
}

#[derive(Debug)]
#[repr(C)]
/// Database entry header
///
///
struct Header {
    timestamp: u64,
    key_size: u16,
    value_size: u32,
}

impl Header {
    const TIMESTAMP_OFFSET: u64 = 0;
    const KEY_SIZE_OFFSET: u64 = Header::TIMESTAMP_OFFSET + mem::size_of::<u64>() as u64;
    const VALUE_SIZE_OFFSET: u64 = Header::KEY_SIZE_OFFSET + mem::size_of::<u16>() as u64;
    const KEY_OFFSET: u64 = Header::VALUE_SIZE_OFFSET + mem::size_of::<u32>() as u64;
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
