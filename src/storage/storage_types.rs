use super::{engine::IoResult, storage_interface::KWayMerge};
use crate::constants::{BUCKET_CHILDREN, PAGE_SIZE};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    io,
    sync::Arc,
};

// ----------------------Log Types--------------------------

#[derive(Debug, Clone)]
pub struct LogPage {
    pub id: u64,
    pub log_bytes: [u8; PAGE_SIZE],
}

impl LogPage {}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum RecordType {
    AppendOrUpdate,
    Delete,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
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
    pub log: ValueLog,
}

impl VLogEntry {
    pub fn new(log: ValueLog, header: LogHeader) -> Self {
        let log = log;
        Self { header, log }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct VLogMetadata {
    pub head_offset: usize,
    pub tail_offset: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct LogHeader {
    pub page_id: u64,
    pub offset: usize,
    pub size: usize,
    pub timestamp: u128,
}

impl LogHeader {
    pub fn new(page_id: u64, offset: usize, size: usize, timestamp: u128) -> Self {
        Self {
            page_id,
            offset,
            size,
            timestamp,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LogArray {
    pub arr: Vec<Arc<VLogEntry>>,
}

impl LogArray {
    pub fn new() -> Self {
        Self { arr: Vec::new() }
    }
}

#[derive(Debug, Clone)]
pub struct LogBufferMap {
    pub map: BTreeMap<String, Arc<VLogEntry>>,
}

impl LogBufferMap {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, key: String, value: Arc<VLogEntry>) {
        self.map.insert(key, value);
    }

    pub fn get(&self, key: String) -> Option<&Arc<VLogEntry>> {
        self.map.get(&key)
    }
}

// ----------------------Sorted Store Types--------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyValOffset {
    pub key: String,
    pub header: LogHeader,
    pub rec_type: RecordType,
}

impl PartialEq for KeyValOffset {
    fn eq(&self, other: &Self) -> bool {
        self.key.eq(&other.key)
    }
}

impl Eq for KeyValOffset {}

impl PartialOrd for KeyValOffset {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(&other))
    }
}

impl Ord for KeyValOffset {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableIndexEntry {
    pub key: String,
    pub page_offset: usize,
}

impl TableIndexEntry {
    pub fn new(key: String, page_offset: usize) -> Self {
        Self { key, page_offset }
    }
}

impl Ord for TableIndexEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}

impl PartialOrd for TableIndexEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableIndex {
    pub index: Vec<TableIndexEntry>,
}

impl TableIndex {
    pub fn new() -> Self {
        Self { index: Vec::new() }
    }

    pub fn set(&mut self, item: TableIndexEntry) {
        self.index.push(item);
    }

    pub fn sort(&mut self) {
        self.index.sort();
    }

    pub fn search(&self, item: &String) -> IoResult<Option<usize>> {
        if self.index.is_empty() {
            return Ok(None);
        }

        let res = self
            .index
            .binary_search_by_key(item, |entry| entry.key.clone());
        match res {
            Ok(idx) => Ok(Some(idx)),
            Err(idx) => {
                if idx >= self.index.len() {
                    Ok(Some(idx - 1))
                } else {
                    Ok(Some(idx))
                }
            }
        }
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
    pub data_size: u64,
    pub filter_keys: [u64; 4],
}

impl TableMetadata {
    pub fn new() -> Self {
        Self {
            index_size: 0,
            bitmap_size: 0,
            data_size: 0,
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
    pub lvl: u16,
    pub hash: u128,
    // pub children_data_size: u64,
}

impl BucketMetadata {
    pub fn new(lvl: u16, hash: u128) -> Self {
        Self {
            index_size: 0,
            lvl,
            hash,
            // children_data_size: 0,
        }
    }
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
    pub fn new() -> Self {
        let mut children = Vec::with_capacity(BUCKET_CHILDREN);
        unsafe { children.set_len(BUCKET_CHILDREN) }
        Self { children }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BucketIndexEntry {
    pub key: String,
    pub sorted_table_file: String,
}

impl Ord for BucketIndexEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}

impl PartialOrd for BucketIndexEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug)]
pub struct BucketIndex {
    pub index: Vec<BucketIndexEntry>,
}

impl BucketIndex {
    pub fn new() -> Self {
        Self { index: Vec::new() }
    }

    pub fn search(&self, item: &String) -> IoResult<Option<usize>> {
        if self.index.is_empty() {
            return Ok(None);
        }

        let res = self
            .index
            .binary_search_by_key(item, |entry| entry.key.clone());
        match res {
            Ok(idx) => Ok(Some(idx)),
            Err(idx) => {
                if idx >= self.index.len() {
                    Ok(Some(idx - 1))
                } else {
                    Ok(Some(idx))
                }
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }
}

pub type TypeBuffer = Vec<KeyValOffset>;

#[derive(Debug, PartialEq, Eq)]
pub struct CompactionBuffer {
    pub type_buffer: TypeBuffer,
}

impl Ord for CompactionBuffer {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.type_buffer[0].cmp(&other.type_buffer[0]).reverse()
    }
}

impl PartialOrd for CompactionBuffer {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl CompactionBuffer {
    pub fn new() -> Self {
        Self {
            type_buffer: Vec::new(),
        }
    }

    pub fn insert(&mut self, val: KeyValOffset) {
        self.type_buffer.push(val);
    }
}

pub struct InputBuffer {
    buffers: Vec<CompactionBuffer>,
}

impl InputBuffer {
    pub fn new() -> Self {
        let mut buffers = Vec::with_capacity(BUCKET_CHILDREN);
        unsafe { buffers.set_len(BUCKET_CHILDREN) }
        Self { buffers }
    }

    fn init_new_child(&mut self, idx: usize) -> &mut CompactionBuffer {
        self.buffers[idx] = CompactionBuffer::new();
        &mut self.buffers[idx]
    }

    pub fn append_to_child(&mut self, val: KeyValOffset, key_hash: u128, lvl: usize) {
        let idx = (key_hash >> (lvl * 4)) & 15;

        let child_buff = self.buffers.get_mut(idx as usize);
        if let Some(buff) = child_buff {
            buff.insert(val);
        } else {
            let child = self.init_new_child(idx as usize);
            child.insert(val);
        }
    }
}

#[derive(Debug)]
pub struct CompactionFlag {
    pub flag: bool,
}

impl CompactionFlag {
    pub fn new() -> Self {
        Self { flag: false }
    }

    pub fn check(&self) -> bool {
        self.flag
    }

    pub fn set(&mut self) {
        self.flag = true;
    }

    pub fn unset(&mut self) {
        self.flag = false;
    }
}

pub struct Config {
    pub bucket_files: HashMap<u128, String>,
}
