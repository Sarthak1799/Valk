// All the interfaces used for storage purposes
use super::{engine::IoResult, log_manager::VLogManager, storage_types as types};
use crate::constants::PAGE_SIZE;
use siphasher::sip128::SipHasher24;
use std::{io, sync::Arc};

pub trait Storage<K, V> {
    fn store(&mut self, key: Option<K>, value: V) -> IoResult<()>;

    fn retrieve(&self, key: Option<K>) -> IoResult<Option<&V>>;
}

impl Storage<String, Arc<types::VLogEntry>> for types::LogArray {
    fn store(&mut self, _key: Option<String>, value: Arc<types::VLogEntry>) -> IoResult<()> {
        self.arr.push(value);
        Ok(())
    }

    fn retrieve(&self, _key: Option<String>) -> IoResult<Option<&Arc<types::VLogEntry>>> {
        Ok(self.arr.last())
    }
}

impl Storage<String, Arc<types::VLogEntry>> for types::LogBufferMap {
    fn store(&mut self, key: Option<String>, value: Arc<types::VLogEntry>) -> IoResult<()> {
        let key = key.ok_or(io::Error::new(
            io::ErrorKind::Other,
            "key not found".to_string(),
        ))?;

        self.insert(key, value);
        Ok(())
    }

    fn retrieve(&self, key: Option<String>) -> IoResult<Option<&Arc<types::VLogEntry>>> {
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

impl From<&types::VLogEntry> for types::KeyValOffset {
    fn from(value: &types::VLogEntry) -> Self {
        Self {
            key: value.log.key.clone(),
            header: value.header.clone(),
            rec_type: value.log.record_type.clone(),
        }
    }
}

// impl Iterator for types::CompactionBuffer {
//     type Item = types::TypeBuffer;

//     fn next(&mut self) -> Option<Self::Item> {
//         let buff = self.byte_buffer.as_slice();
//         let item = bincode::deserialize::<Vec<types::KeyValOffset>>(&buff[0..PAGE_SIZE])
//             .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))
//             .ok();

//         item
//     }
// }

pub trait CompactionPreprocessing {
    type CompactionChildren;
    fn form_input_buffers(&mut self, hasher: SipHasher24) -> IoResult<types::InputBuffer>;

    fn perform_preprocessing(&mut self) -> IoResult<()>;
}

impl<'b> CompactionPreprocessing for VLogManager<'b> {
    type CompactionChildren = types::ChildrenBuckets;

    fn form_input_buffers(&mut self, hasher: SipHasher24) -> IoResult<types::InputBuffer> {
        let reader = self
            .map
            .read()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let mut input_buffer = types::InputBuffer::new();

        let _ = reader.map.iter().map(|(key, log)| {
            let val = types::KeyValOffset::from(log.as_ref());
            let key_hash = hasher.hash(key.as_bytes()).as_u128();
            input_buffer.append_to_child(val, key_hash, 0);
        });

        Ok(input_buffer)
    }

    fn perform_preprocessing(&mut self) -> IoResult<()> {
        self.flush_log()
    }
}

pub trait KWayMerge {
    fn perform_buffered_merge(&mut self) -> IoResult<()>;
}

pub trait Compaction: CompactionPreprocessing {
    type CompactionInput;
    type HashObject;

    fn get_compaction_inputs(
        &mut self,
        hasher: Self::HashObject,
    ) -> IoResult<Self::CompactionInput>;
}

impl<'b> Compaction for VLogManager<'b> {
    type CompactionInput = types::InputBuffer;
    type HashObject = SipHasher24;

    fn get_compaction_inputs(
        &mut self,
        hasher: Self::HashObject,
    ) -> IoResult<Self::CompactionInput> {
        let input = self.form_input_buffers(hasher)?;
        self.perform_preprocessing()?;
        Ok(input)
    }
}
