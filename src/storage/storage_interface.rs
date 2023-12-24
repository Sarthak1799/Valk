// All the interfaces used for storage purposes
use super::{
    engine::IoResult,
    log_manager::VLogManager,
    sorted_store::{Bucket, SortedStoreTable},
    storage_types as types,
};
use crate::constants::PAGE_SIZE;
use siphasher::sip128::SipHasher24;
use std::{
    io,
    rc::Rc,
    sync::{Arc, RwLock},
};

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
    type PreprocessingOutput;
    type AuxilaryInput;
    type HashObject;

    fn form_input_buffers(
        &self,
        hasher: Self::HashObject,
        auxilary_input: Option<Self::AuxilaryInput>,
    ) -> IoResult<types::InputBuffer>;

    fn perform_preprocessing(&mut self) -> IoResult<Self::PreprocessingOutput>;
}

impl<'b> CompactionPreprocessing for VLogManager<'b> {
    type PreprocessingOutput = ();
    type AuxilaryInput = ();
    type HashObject = SipHasher24;

    fn form_input_buffers(
        &self,
        hasher: Self::HashObject,
        auxilary_input: Option<Self::AuxilaryInput>,
    ) -> IoResult<types::InputBuffer> {
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

    fn perform_preprocessing(&mut self) -> IoResult<Self::PreprocessingOutput> {
        self.flush_log()
    }
}

impl CompactionPreprocessing for Bucket {
    type PreprocessingOutput = types::BucketIndexEntry;
    type AuxilaryInput = types::BucketIndexEntry;
    type HashObject = SipHasher24;

    fn perform_preprocessing(&mut self) -> IoResult<Self::PreprocessingOutput> {
        let reader = self
            .index
            .read()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let entry = reader.index[0].clone();
        Ok(entry)
    }

    fn form_input_buffers(
        &self,
        hasher: <Self as CompactionPreprocessing>::HashObject,
        sst_entry: Option<Self::AuxilaryInput>,
    ) -> IoResult<types::InputBuffer> {
        let entry = sst_entry.ok_or(io::Error::new(
            io::ErrorKind::Other,
            "index entry not found".to_string(),
        ))?;
        let sst = SortedStoreTable::new_load_from_disk(&entry.sorted_table_file)?;
        let mut input_buffer = types::InputBuffer::new();

        while let Some(buff) = sst.get_compaction_buffer(0)? {
            let mut merge_vec = Vec::new();

            for buf in buff.chunks(PAGE_SIZE) {
                let item = bincode::deserialize::<Vec<types::KeyValOffset>>(&buf)
                    .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

                merge_vec.extend(item);
            }

            let _ = merge_vec.into_iter().map(|kval| {
                let key_hash = hasher.hash(kval.key.as_bytes()).as_u128();
                input_buffer.append_to_child(kval, key_hash, 0);
            });
        }

        Ok(input_buffer)
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
        hasher: <Self as CompactionPreprocessing>::HashObject,
    ) -> IoResult<Self::CompactionInput>;
}

impl<'b, 'a> Compaction for VLogManager<'b> {
    type CompactionInput = types::InputBuffer;
    type HashObject = SipHasher24;

    fn get_compaction_inputs(
        &mut self,
        hasher: <Self as Compaction>::HashObject,
    ) -> IoResult<Self::CompactionInput> {
        let input = self.form_input_buffers(hasher, None)?;
        self.perform_preprocessing()?;
        Ok(input)
    }
}

impl Compaction for Bucket {
    type CompactionInput = types::InputBuffer;
    type HashObject = SipHasher24;

    fn get_compaction_inputs(
        &mut self,
        hasher: <Self as Compaction>::HashObject,
    ) -> IoResult<Self::CompactionInput> {
        let entry = self.perform_preprocessing()?;
        self.form_input_buffers(hasher, Some(entry))
    }
}
