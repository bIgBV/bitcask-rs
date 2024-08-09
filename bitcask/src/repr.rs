use std::{
    backtrace::Backtrace,
    mem,
    time::{SystemTime, SystemTimeError},
};

use bytemuck::{bytes_of, Pod, Zeroable};

/// Database entry header
///
/// We want to ensure the struct is packed for cleaner de/serialization
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C, packed)]
pub(crate) struct Header {
    // todo: we're using unix timestamps, so we should be able to pack tombstone information into
    // the higher order bits of a u64
    pub tombstone: u8,
    pub timestamp: u64,
    pub key_size: u16,
    pub value_size: u32,
}

impl Header {
    pub const IS_DELETED: u8 = 1;
    pub const NOT_DELETED: u8 = 0;
    pub const LEN: u64 = mem::size_of::<Header>() as u64;

    /// The size of the data field in this entry
    ///
    /// This will be encoded as |key|value|
    pub fn data_size(&self) -> usize {
        (self.value_size.saturating_add(self.key_size as u32)) as usize
    }

    /// Total size of an entry associated with this header
    pub fn entry_size(&self) -> usize {
        Header::LEN as usize + self.data_size()
    }

    pub fn serialize(&self) -> &[u8] {
        bytes_of(self)
    }
}

/// Represents an entry in a data file.
#[derive(Debug)]
pub struct Entry<'input> {
    pub(crate) header: Header,
    key: &'input [u8],
    value: Option<&'input [u8]>,
}

impl<'input> Entry<'input> {
    pub fn new_encoded<K, V>(key: &'input K, value: &'input V) -> Result<Entry<'input>, EntryError>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let val = value.as_ref();

        let key_len = key.len();
        let val_len = val.len();

        debug_assert!((key_len as u16) < u16::MAX);
        debug_assert!((val_len as u32) < u32::MAX);

        // TODO: This needs to be made deterministic for tests
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_secs();

        let header = Header {
            tombstone: Header::NOT_DELETED,
            key_size: key_len as u16,
            value_size: val_len as u32,
            timestamp,
        };

        Ok(Entry {
            header,
            key,
            value: Some(val),
        })
    }

    /// Creates an empty tombstone entry for deleted values
    pub fn new_empty<K>(key: &'input K) -> Entry<'input>
    where
        K: AsRef<[u8]>,
    {
        let key = key.as_ref();
        debug_assert!(key.len() < u16::MAX.into());
        Entry {
            header: Header {
                tombstone: Header::IS_DELETED,
                timestamp: 0,
                key_size: key.len() as u16,
                value_size: 0,
            },
            key,
            value: None,
        }
    }

    // TODO: Allocating a whole vector for the entry is wasteful. We should be able to write the
    // whole structure to the file somehow.
    pub fn serialize(&self) -> Vec<u8> {
        [
            self.header.serialize(),
            self.key,
            self.value.unwrap_or_else(|| &[]),
        ]
        .concat()
    }

    pub fn len(&self) -> usize {
        (Header::LEN + self.header.key_size as u64 + self.header.value_size as u64) as usize
    }

    pub fn is_tombstone(&self) -> bool {
        self.header.tombstone == Header::IS_DELETED
    }

    pub fn key(&self) -> &[u8] {
        self.key
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EntryError {
    #[error("Error converting timestamp: {source}")]
    Time {
        #[from]
        source: SystemTimeError,
        backtrace: Backtrace,
    },
}
