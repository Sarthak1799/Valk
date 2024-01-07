use super::{
    engine::IoResult,
    pager::Pager,
    storage_interface::{Compaction, Storage},
    storage_types as types,
};
use siphasher::sip128::SipHasher24;
use std::{
    io,
    sync::{Arc, RwLock},
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug)]
pub struct VLogController {
    pub path: &'static str,
    pub pager: Arc<RwLock<Pager<types::LogPage>>>,
    pub metadata: Arc<RwLock<types::VLogMetadata>>,
    pub route_flag: Arc<RwLock<types::GenericFlag>>,
    pub current: VLogManager,
    pub next: VLogManager,
}

impl VLogController {
    pub fn init(path: &'static str) -> IoResult<()> {
        let pager = Pager::<types::LogPage>::new(path)?;
        let metadata = types::VLogMetadata::new();

        let enc = bincode::serialize(&metadata)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        pager.flush_arbitrary(0, enc.as_slice())?;

        Ok(())
    }
    pub fn new_load_from_disk(path: &'static str) -> IoResult<Self> {
        let mut buffer = [0 as u8; 16];

        let mut pager = Pager::<types::LogPage>::new(path)?;
        pager.read_arbitrary_from_offset(0, &mut buffer)?;

        let metadata = bincode::deserialize::<types::VLogMetadata>(&buffer)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        pager.set_page(metadata.head_offset)?;
        let current = VLogManager::new();
        let next = VLogManager::new();
        Ok(Self {
            path,
            pager: Arc::new(RwLock::new(pager)),
            metadata: Arc::new(RwLock::new(metadata)),
            current,
            next,
            route_flag: Arc::new(RwLock::new(types::GenericFlag::new())),
        })
    }

    fn read_flag(&self) -> IoResult<bool> {
        let reader = self
            .route_flag
            .read()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        Ok(reader.check())
    }

    pub fn get_current_size(&self) -> IoResult<u64> {
        let curr_val = self.read_flag()?;

        if curr_val {
            self.next.get_size()
        } else {
            self.current.get_size()
        }
    }

    pub fn append_log(&self, log: types::ValueLog) -> IoResult<()> {
        let pager = self.pager.clone();
        let metadata = self.metadata.clone();
        self.current.append_log(pager, metadata, log)
    }

    pub fn get_val_from_log<T>(&self, kval: types::KeyValOffset) -> IoResult<Vec<T>>
    where
        for<'a> T: serde::de::Deserialize<'a>,
    {
        let pager = self.pager.clone();
        self.current.get_val_from_log(kval, pager)
    }

    pub fn get(&self, key: String) -> IoResult<types::ValueLog> {
        self.current.get(key)
    }

    pub fn flip_flag(&self) -> IoResult<()> {
        let reader = self.route_flag.try_read();
        let mut curr_val = false;

        if let Err(_e) = reader {
            return Ok(());
        } else if let Ok(read) = reader {
            curr_val = read.check();
        }

        let mut writer = self
            .route_flag
            .write()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        if writer.check() != curr_val {
            return Ok(());
        } else {
            writer.flip();
        }

        Ok(())
    }

    pub fn get_compaction_inputs(&self, hasher: SipHasher24) -> IoResult<types::InputBuffer> {
        let curr_val = self.read_flag()?;
        let pager = self.pager.clone();
        let metadata = self.metadata.clone();

        let inputs = if curr_val {
            self.current
                .get_compaction_inputs(hasher, Some((pager, metadata)))?
        } else {
            self.next
                .get_compaction_inputs(hasher, Some((pager, metadata)))?
        };

        Ok(inputs)
    }
}

#[derive(Debug)]
pub struct VLogManager {
    pub entries: Arc<RwLock<types::LogArray>>,
    pub map: Arc<RwLock<types::LogBufferMap>>,
    pub current_size: Arc<RwLock<types::VLogSize>>,
}

impl VLogManager {
    fn new() -> Self {
        let entries = Arc::new(RwLock::new(types::LogArray::new()));
        let map = Arc::new(RwLock::new(types::LogBufferMap::new()));

        Self {
            entries,
            map,
            current_size: Arc::new(RwLock::new(types::VLogSize(0))),
        }
    }

    pub fn clear(&self, pager: Arc<RwLock<Pager<types::LogPage>>>) -> IoResult<()> {
        let mut page_writer = pager
            .write()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
        let mut vec_writer = self
            .entries
            .write()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
        let mut map_writer = self
            .map
            .write()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
        let mut size_writer = self
            .current_size
            .write()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        page_writer.clear();
        vec_writer.clear();
        map_writer.clear();
        size_writer.0 = 0;

        Ok(())
    }

    fn append_log(
        &self,
        pager: Arc<RwLock<Pager<types::LogPage>>>,
        metadata: Arc<RwLock<types::VLogMetadata>>,
        log: types::ValueLog,
    ) -> IoResult<()> {
        let mut page_writer = pager
            .write()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let (page_id, val_offset, val_size) = page_writer.append_to_page(&log)?;
        drop(page_writer);

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?
            .as_nanos();

        let header = types::LogHeader::new(page_id, val_offset, val_size, timestamp);

        let key = log.key.clone();
        let vlog_entry = Arc::new(types::VLogEntry::new(log, header));

        let mut writer = self
            .entries
            .write()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        writer.store(None, vlog_entry)?;
        drop(writer);

        let reader = self
            .entries
            .read()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let entry = reader.retrieve(Some(key.clone()))?.ok_or(io::Error::new(
            io::ErrorKind::Other,
            "Unable to retrive".to_string(),
        ))?;

        let log_ref = entry.clone();

        let mut map_writer = self
            .map
            .write()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        map_writer.store(Some(key), log_ref)?;

        let mut metadata_writer = metadata
            .write()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        metadata_writer.update_head_offset(val_offset + val_size);
        let mut curr_size_writer = self
            .current_size
            .write()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        curr_size_writer.0 += val_size as u64;

        Ok(())
    }

    fn get(&self, key: String) -> IoResult<types::ValueLog> {
        let reader = self
            .map
            .read()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let res = reader
            .retrieve(Some(key))?
            .ok_or(io::Error::new(
                io::ErrorKind::Other,
                "Not found".to_string(),
            ))?
            .clone();

        Ok(res.log.clone())
    }

    pub fn flush_log(
        &self,
        pager: Arc<RwLock<Pager<types::LogPage>>>,
        metadata: Arc<RwLock<types::VLogMetadata>>,
    ) -> IoResult<()> {
        let metadata_reader = metadata
            .read()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let tail = metadata_reader.tail_offset;
        let head = metadata_reader.head_offset;
        let meta = metadata_reader.get_self_ref();
        let metadata_bytes = bincode::serialize(meta)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        drop(metadata_reader);

        let mut page_writer = pager
            .write()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        page_writer.flush_arbitrary(0, metadata_bytes.as_slice())?;
        page_writer.flush_pages(tail)?;

        drop(page_writer);

        // assume operations to be successful for now due to absence of a recovery module
        let mut metadata_writer = metadata
            .write()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        metadata_writer.update_tail_offset(head);

        Ok(())
    }

    pub fn get_val_from_log<T>(
        &self,
        kval: types::KeyValOffset,
        pager: Arc<RwLock<Pager<types::LogPage>>>,
    ) -> IoResult<Vec<T>>
    where
        for<'a> T: serde::de::Deserialize<'a>,
    {
        let (page_id, offset, size) = (kval.header.page_id, kval.header.offset, kval.header.size);
        let mut page_writer = pager
            .write()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        page_writer.read_pages::<T>(page_id, offset, size)
    }

    pub fn get_size(&self) -> IoResult<u64> {
        let reader = self
            .current_size
            .read()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        Ok(reader.0)
    }
}

#[cfg(test)]
mod test {

    use crate::storage::log_manager::VLogController;

    use super::{types::RecordType, types::ValueLog, types::ValueType};
    use std::{fs, iter::zip};

    #[test]
    fn log_manager_test() {
        let path = "/home/sarthak17/Desktop/disk_test";

        VLogController::init(path).expect("failed to init");
        let controller = VLogController::new_load_from_disk(path).expect("failed");

        let key = "test_key".to_string();
        let bytes = [1, 2, 3];
        let mut value = Vec::new();
        value.extend_from_slice(&bytes);

        let value = ValueType::NumberArray(value);

        let log = ValueLog::new(key.clone(), value.clone(), RecordType::AppendOrUpdate);

        controller.append_log(log.clone()).expect("failed");

        let val = controller.get(key).expect("Failed");

        assert_eq!(log, val);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn log_manager_flush_test<'de>() {
        let path = "/home/sarthak17/Desktop/disk_test";

        VLogController::init(path).expect("failed to init");
        let controller = VLogController::new_load_from_disk(path).expect("failed");

        let key = "test_key".to_string();
        let bytes = [1, 2, 3];
        let key1 = "test_key_1".to_string();
        let bytes1 = [4, 5, 6];
        let mut value = Vec::new();
        value.extend_from_slice(&bytes);
        let mut value1 = Vec::new();
        value1.extend_from_slice(&bytes1);

        let value = ValueType::NumberArray(value);
        let value1 = ValueType::NumberArray(value1);

        let log = ValueLog::new(key.clone(), value.clone(), RecordType::AppendOrUpdate);
        let log1 = ValueLog::new(key1.clone(), value1.clone(), RecordType::AppendOrUpdate);

        controller.append_log(log.clone()).expect("failed ok");
        controller.append_log(log1.clone()).expect("failed ok");

        controller
            .current
            .flush_log(controller.pager.clone(), controller.metadata.clone())
            .expect("Failed to flush");

        let mut initial_vec = Vec::new();
        initial_vec.push(log);
        initial_vec.push(log1);

        let mut page_mut = controller.pager.write().unwrap();

        let log_vec = page_mut.read_pages::<ValueLog>(0, 16, 112).unwrap();

        for (l1, l2) in zip(initial_vec, log_vec) {
            assert_eq!(l1, l2);
        }

        fs::remove_file(path).unwrap();
    }
}
