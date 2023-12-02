use super::{engine::IoResult, pager::Pager, storage_interface::Storage, storage_types as types};
use std::{
    io,
    sync::{Arc, RwLock},
};

#[derive(Debug)]
pub struct VLogManager<'b> {
    pub path: &'b str,
    pub pager: Pager<types::LogPage>,
    pub metadata: types::VLogMetadata,
    pub entries: Arc<RwLock<types::LogArray>>,
    pub map: Arc<RwLock<types::LogBufferMap>>,
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
        })
    }

    fn append_log(&mut self, log: types::ValueLog) -> IoResult<()> {
        let (page_id, val_offset, val_size) = self.pager.append_to_page(&log)?;
        let header = types::LogHeader::new(page_id, val_offset, val_size);

        let key = log.key.clone();
        let vlog_entry = types::VLogEntry::new(log, header);

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

        let log_ref = entry.log.clone();

        let mut map_writer = self
            .map
            .write()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        map_writer.store(Some(key), log_ref)?;

        self.metadata.head_offset = val_offset + val_size;

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

        Ok(res.as_ref().clone())
    }

    fn flush_log(&mut self) -> IoResult<()> {
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
}

#[cfg(test)]
mod test {

    use super::{
        types::LogPage, types::RecordType, types::VLogMetadata, types::ValueLog, types::ValueType,
        Pager, VLogManager,
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
