mod fs;

use std::{
    mem,
    time::{SystemTime, SystemTimeError},
};

use bytemuck::{Pod, PodCastError, Zeroable};
use fs::{Fs, FsError, Offset};

fn main() {
    let mut cask = Cask::new().unwrap();
    cask.insert("hello", "world 🧏‍♀️").unwrap();
    let result = cask.get("hello").unwrap();

    let val = String::from_utf8(result).unwrap();
    println!("{val}");
}

struct Cask {
    fs: Fs,
    position: u64,
}

impl Cask {
    fn new() -> Result<Self, ()> {
        Ok(Cask {
            fs: Fs::new(),
            position: 0,
        })
    }

    pub fn insert<K, V>(&mut self, key: K, value: V) -> Result<Offset, CaskError>
    where
        K: StoredData,
        V: StoredData,
    {
        let header = encode(&key, &value)?;

        let entry = Entry::new(header, key, value);

        Ok(self.fs.write_entry(entry)?)
    }

    pub fn get<K>(&mut self, key: K) -> Result<Vec<u8>, CaskError> {
        let mut buf = [0u8; Header::LEN as usize];
        self.fs.get_chunk(Offset(0), &mut buf)?;
        let header: &Header = bytemuck::try_from_bytes(&buf).map_err(CaskError::Cast)?;

        let data_len = header.value_size.saturating_add(header.key_size as u32);
        let mut buf = vec![0u8; data_len as usize];
        self.fs.get_chunk(Offset(Header::LEN as usize), &mut buf)?;

        Ok(buf)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum CaskError {
    #[error("Error interacting with the filesystem: {0}")]
    Fs(#[from] FsError),

    #[error("Error casting value: {0}")]
    Cast(PodCastError),

    #[error("Encoding error: {0}")]
    Encode(#[from] SystemTimeError),
}

/// Database entry header
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C, packed)]
struct Header {
    timestamp: u64,
    value_size: u32,
    key_size: u16,
}

impl Header {
    const TIMESTAMP_OFFSET: u64 = 0;
    const KEY_SIZE_OFFSET: u64 = Header::TIMESTAMP_OFFSET + mem::size_of::<u64>() as u64;
    const VALUE_SIZE_OFFSET: u64 = Header::KEY_SIZE_OFFSET + mem::size_of::<u16>() as u64;
    const KEY_OFFSET: u64 = Header::VALUE_SIZE_OFFSET + mem::size_of::<u32>() as u64;
    const LEN: u64 = mem::size_of::<Header>() as u64;
}

pub trait StoredData {
    fn as_bytes(&self) -> &[u8];
}

impl StoredData for String {
    fn as_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl StoredData for &str {
    fn as_bytes(&self) -> &[u8] {
        str::as_bytes(&self)
    }
}

fn encode<K, V>(key: &K, value: &V) -> Result<Header, CaskError>
where
    K: StoredData,
    V: StoredData,
{
    // TODO: calling as_bytes everytime might be costly
    let key_len = key.as_bytes().len();
    let val_len = value.as_bytes().len();

    debug_assert!((key_len as u16) < u16::MAX);
    debug_assert!((val_len as u32) < u32::MAX);

    // TODO: This needs to be made deterministic for tests
    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_secs();

    Ok(Header {
        key_size: key_len as u16,
        value_size: val_len as u32,
        timestamp,
    })
}

pub struct Entry<K, V> {
    header: Header,
    key: K,
    value: V,
}

impl<K, V> Entry<K, V> {
    fn new(header: Header, key: K, value: V) -> Self {
        Entry { header, key, value }
    }
}
