use super::engine::IoResult;
use crate::constants::{BUCKET_CHILDREN, PAGE_SIZE};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, io, sync::Arc};

// ----------------------Log Types--------------------------

#[derive(Debug, Clone)]
pub struct LogPage {
    pub id: u64,
    pub log_bytes: [u8; PAGE_SIZE],
}

impl LogPage {}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum RecordType {
    AppendOrUpdate,
    Delete,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum ValueType {
    Number(i64),
    StringType(String),
    NumberArray(Vec<i64>),
    StringArray(Vec<String>),
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct ValueLog {
    pub key: String,
    pub value: ValueType,
    pub record_type: RecordType,
}

impl ValueLog {
    pub fn new(key: String, value: ValueType, record_type: RecordType) -> Self {
        Self {
            key,
            value,
            record_type,
        }
    }
}

//#[derive(Debug, Serialize, Deserialize, Clone)]
#[derive(Debug, Clone)]
pub struct VLogEntry {
    // #[serde(deserialize_with = "", serialize_with = "")]
    pub header: LogHeader,
    pub log: Arc<ValueLog>,
}

impl VLogEntry {
    pub fn new(log: ValueLog, header: LogHeader) -> Self {
        let log = Arc::new(log);
        Self { header, log }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct VLogMetadata {
    pub head_offset: usize,
    pub tail_offset: usize,
}

#[derive(Debug, Clone)]
pub struct LogHeader {
    pub page_id: u64,
    pub offset: usize,
    pub size: usize,
}

impl LogHeader {
    pub fn new(page_id: u64, offset: usize, size: usize) -> Self {
        Self {
            page_id,
            offset,
            size,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LogArray {
    pub arr: Vec<VLogEntry>,
}

impl LogArray {
    pub fn new() -> Self {
        Self { arr: Vec::new() }
    }
}

#[derive(Debug, Clone)]
pub struct LogBufferMap {
    map: BTreeMap<String, Arc<ValueLog>>,
}

impl LogBufferMap {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, key: String, value: Arc<ValueLog>) {
        self.map.insert(key, value);
    }

    pub fn get(&self, key: String) -> Option<&Arc<ValueLog>> {
        self.map.get(&key)
    }
}

// ----------------------Sorted Store Types--------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyValOffset {
    pub key: String,
    pub val_offset: usize,
    pub size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableIndexEntry {
    pub key: String,
    pub page_offset: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableIndex {
    pub index: Vec<TableIndexEntry>,
}

impl TableIndex {
    pub fn new() -> Self {
        Self { index: Vec::new() }
    }

    pub fn search(&self, item: &String) -> IoResult<usize> {
        let res = self
            .index
            .binary_search_by_key(item, |entry| entry.key.clone())
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        Ok(res)
    }

    pub fn get_offset(&self, index: usize) -> usize {
        let entry = &self.index[index];
        entry.page_offset
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    pub index_size: u64,
    pub bitmap_size: u64,
    pub filter_keys: [u64; 4],
}

impl TableMetadata {
    pub fn new() -> Self {
        Self {
            index_size: 0,
            bitmap_size: 0,
            filter_keys: [0, 0, 0, 0],
        }
    }
}

#[derive(Debug, Clone)]
pub struct SortedStorePage {
    pub id: u64,
    pub bytes: [u8; PAGE_SIZE],
}

impl SortedStorePage {}

#[derive(Debug, Clone)]
pub struct IndexPage {
    pub id: u64,
    pub bytes: [u8; PAGE_SIZE],
}

impl IndexPage {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketMetadata {
    pub index_size: u64,
    pub children_data_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ChildBucket {
    Child(String),
    Null,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChildrenBuckets {
    pub children: Vec<ChildBucket>,
}

impl ChildrenBuckets {
    fn new() -> Self {
        Self {
            children: Vec::with_capacity(BUCKET_CHILDREN),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketIndexEntry {
    pub key: String,
    pub sorted_table_file: String,
}

#[derive(Debug)]
pub struct BucketIndex {
    pub index: Vec<BucketIndexEntry>,
}
