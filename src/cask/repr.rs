use std::{
    mem,
    time::{SystemTime, SystemTimeError},
};

use bytemuck::{bytes_of, Pod, Zeroable};

/// Database entry header
///
/// We want to ensure the struct is packed for cleaner de/serialization. This struct should never be
/// stored in a cache senstivie manner.
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C, packed)]
pub(in crate::cask) struct Header {
    timestamp: u64,
    pub value_size: u32,
    pub key_size: u16,
}

impl Header {
    pub const LEN: u64 = mem::size_of::<Header>() as u64;

    /// The size of the data field in this entry
    ///
    /// This will be encoded as |key|value|
    pub fn data_size(&self) -> usize {
        (self.value_size.saturating_add(self.key_size as u32)) as usize
    }

    pub fn serialize(&self) -> &[u8] {
        bytes_of(self)
    }
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

/// Represents an entry in a data file.
#[derive(Debug)]
pub struct Entry<'entry> {
    pub(in crate::cask) header: Header,
    key: &'entry [u8],
    value: &'entry [u8],
}

impl<'entry> Entry<'entry> {
    pub fn new_encoded<K, V>(key: &'entry K, value: &'entry V) -> Result<Entry<'entry>, EntryError>
    where
        K: StoredData,
        V: StoredData,
    {
        // TODO: calling as_bytes everytime might be costly
        let key = key.as_bytes();
        let val = value.as_bytes();

        let key_len = key.len();
        let val_len = val.len();

        debug_assert!((key_len as u16) < u16::MAX);
        debug_assert!((val_len as u32) < u32::MAX);

        // TODO: This needs to be made deterministic for tests
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_secs();

        let header = Header {
            key_size: key_len as u16,
            value_size: val_len as u32,
            timestamp,
        };

        Ok(Entry {
            header,
            key,
            value: val,
        })
    }

    // TODO: Allocating a whole vector for the entry is wasteful. We should be able to write the
    // whole structure to the file somehow.
    pub fn serialize(&self) -> Vec<u8> {
        [self.header.serialize(), self.key, self.value].concat()
    }

    pub fn len(&self) -> usize {
        (Header::LEN + self.header.key_size as u64 + self.header.value_size as u64) as usize
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EntryError {
    #[error("Error converting timestamp: {0}")]
    Time(#[from] SystemTimeError),
}
