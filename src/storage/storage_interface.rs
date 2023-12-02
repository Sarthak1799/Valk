// All the interfaces used for storage purposes
use super::{engine::IoResult, storage_types as types};
use crate::constants::PAGE_SIZE;
use std::{io, sync::Arc};

pub trait Storage<K, V> {
    fn store(&mut self, key: Option<K>, value: V) -> IoResult<()>;

    fn retrieve(&self, key: Option<K>) -> IoResult<Option<&V>>;
}

impl Storage<String, types::VLogEntry> for types::LogArray {
    fn store(&mut self, _key: Option<String>, value: types::VLogEntry) -> IoResult<()> {
        self.arr.push(value);
        Ok(())
    }

    fn retrieve(&self, _key: Option<String>) -> IoResult<Option<&types::VLogEntry>> {
        Ok(self.arr.last())
    }
}

impl Storage<String, Arc<types::ValueLog>> for types::LogBufferMap {
    fn store(&mut self, key: Option<String>, value: Arc<types::ValueLog>) -> IoResult<()> {
        let key = key.ok_or(io::Error::new(
            io::ErrorKind::Other,
            "key not found".to_string(),
        ))?;

        self.insert(key, value);
        Ok(())
    }

    fn retrieve(&self, key: Option<String>) -> IoResult<Option<&Arc<types::ValueLog>>> {
        let key = key.ok_or(io::Error::new(
            io::ErrorKind::Other,
            "key not found".to_string(),
        ))?;

        Ok(self.get(key))
    }
}

pub trait Page {
    fn new(id: u64, buffer: [u8; PAGE_SIZE]) -> Self;

    fn get_id(&self) -> u64;

    fn get_page_bytes(&mut self) -> &mut [u8; PAGE_SIZE];
}

impl Page for types::LogPage {
    fn new(id: u64, buffer: [u8; PAGE_SIZE]) -> Self {
        Self {
            id,
            log_bytes: buffer,
        }
    }

    fn get_id(&self) -> u64 {
        self.id
    }

    fn get_page_bytes(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.log_bytes
    }
}

impl Page for types::SortedStorePage {
    fn new(id: u64, buffer: [u8; PAGE_SIZE]) -> Self {
        Self { id, bytes: buffer }
    }

    fn get_id(&self) -> u64 {
        self.id
    }

    fn get_page_bytes(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.bytes
    }
}

impl Page for types::IndexPage {
    fn new(id: u64, buffer: [u8; PAGE_SIZE]) -> Self {
        Self { id, bytes: buffer }
    }

    fn get_id(&self) -> u64 {
        self.id
    }

    fn get_page_bytes(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.bytes
    }
}
//pub trait Header {}
