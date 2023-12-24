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
pub struct VLogController<'b> {
    pub path: &'b str,
    pub current: VLogManager<'b>,
}

impl<'b> VLogController<'b> {
    pub fn new(path: &'b str) -> IoResult<Self> {
        let current = VLogManager::new(path)?;
        Ok(Self { path, current })
    }

    pub fn get_current_size(&self) -> u64 {
        self.current.get_size()
    }

    pub fn append_log(&mut self, log: types::ValueLog) -> IoResult<()> {
        self.current.append_log(log)
    }

    pub fn get_val_from_log<T>(&mut self, kval: types::KeyValOffset) -> IoResult<Vec<T>>
    where
        for<'a> T: serde::de::Deserialize<'a>,
    {
        self.current.get_val_from_log(kval)
    }

    pub fn get(&self, key: String) -> IoResult<types::ValueLog> {
        self.current.get(key)
    }

    pub fn get_compaction_inputs(&mut self, hasher: SipHasher24) -> IoResult<types::InputBuffer> {
        let current_manager = &mut self.current;
        let inputs = current_manager.get_compaction_inputs(hasher)?;

        let new_manager = VLogManager::new(self.path)?;
        self.current = new_manager;

        Ok(inputs)
    }
}

#[derive(Debug)]
pub struct VLogManager<'b> {
    pub path: &'b str,
    pub pager: Pager<types::LogPage>,
    pub metadata: types::VLogMetadata,
    pub entries: Arc<RwLock<types::LogArray>>,
    pub map: Arc<RwLock<types::LogBufferMap>>,
    pub current_size: u64,
}

impl<'b> VLogManager<'b> {
    fn new(path: &'b str) -> IoResult<Self> {
        let mut buffer = [0 as u8; 16];

        let mut pager = Pager::new(path)?;
        pager.read_arbitrary_from_offset(0, &mut buffer)?;

        let metadata = bincode::deserialize::<types::VLogMetadata>(&buffer)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        pager.set_page(metadata.head_offset)?;

        let entries = Arc::new(RwLock::new(types::LogArray::new()));
        let map = Arc::new(RwLock::new(types::LogBufferMap::new()));

        Ok(Self {
            path,
            pager,
            metadata,
            entries,
            map,
            current_size: 0,
        })
    }

    fn append_log(&mut self, log: types::ValueLog) -> IoResult<()> {
        let (page_id, val_offset, val_size) = self.pager.append_to_page(&log)?;
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

        self.metadata.head_offset = val_offset + val_size;
        self.current_size += val_size as u64;

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

    pub fn flush_log(&mut self) -> IoResult<()> {
        let offset = self.metadata.tail_offset;

        let metadata = &self.metadata;
        let metadata_bytes = bincode::serialize(metadata)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        self.pager.flush_arbitrary(0, metadata_bytes.as_slice())?;
        self.pager.flush_pages(offset)?;

        // assume operations to be successful for now due to absence of a recovery module
        self.metadata.tail_offset = self.metadata.head_offset;

        Ok(())
    }

    pub fn get_val_from_log<T>(&mut self, kval: types::KeyValOffset) -> IoResult<Vec<T>>
    where
        for<'a> T: serde::de::Deserialize<'a>,
    {
        let (page_id, offset, size) = (kval.header.page_id, kval.header.offset, kval.header.size);
        self.pager.read_pages::<T>(page_id, offset, size)
    }

    pub fn get_size(&self) -> u64 {
        self.current_size
    }
}

#[cfg(test)]
mod test {

    use super::{
        types::ChildrenBuckets, types::LogPage, types::RecordType, types::VLogMetadata,
        types::ValueLog, types::ValueType, Pager, VLogManager,
    };
    use std::{fs, iter::zip};

    #[test]
    fn log_manager_test() {
        let path = "/home/sarthak17/Desktop/disk_test";

        let file_creation_result = fs::File::create(path);
        if let Err(err) = file_creation_result {
            panic!("Failed to create the file: {}", err);
        }

        let initial_metadata = VLogMetadata {
            head_offset: 16,
            tail_offset: 16,
        };

        let bytes = bincode::serialize(&initial_metadata).unwrap();
        let disk = Pager::<LogPage>::new(path).expect("failed");
        disk.flush_arbitrary(0, bytes.as_slice()).expect("failed");

        let mut log_manager = VLogManager::new(path).expect("Failed to create VLogManager");

        let key = "test_key".to_string();
        let bytes = [1, 2, 3];
        let mut value = Vec::new();
        value.extend_from_slice(&bytes);

        let value = ValueType::NumberArray(value);

        let log = ValueLog::new(key.clone(), value.clone(), RecordType::AppendOrUpdate);

        log_manager.append_log(log.clone()).expect("failed ok");

        let val = log_manager.get(key).expect("Failed");

        assert_eq!(log, val);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn log_manager_flush_test<'de>() {
        let path = "/home/sarthak17/Desktop/disk_test";

        let file_creation_result = fs::File::create(path);
        if let Err(err) = file_creation_result {
            panic!("Failed to create the file: {}", err);
        }

        let initial_metadata = VLogMetadata {
            head_offset: 16,
            tail_offset: 16,
        };

        let bytes = bincode::serialize(&initial_metadata).unwrap();
        let disk = Pager::<LogPage>::new(path).expect("failed");
        disk.flush_arbitrary(0, bytes.as_slice()).expect("failed");

        let mut log_manager = VLogManager::new(path).expect("Failed to create VLogManager");

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

        log_manager.append_log(log.clone()).expect("failed ok");
        log_manager.append_log(log1.clone()).expect("failed ok");

        log_manager.flush_log().expect("Failed to flush");

        let mut initial_vec = Vec::new();
        initial_vec.push(log);
        initial_vec.push(log1);

        let log_vec = log_manager
            .pager
            .read_pages::<ValueLog>(0, 16, 112)
            .unwrap();

        for (l1, l2) in zip(initial_vec, log_vec) {
            assert_eq!(l1, l2);
        }

        fs::remove_file(path).unwrap();
    }
}
