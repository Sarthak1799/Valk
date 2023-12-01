use super::{bloom_filter::BloomFilter, engine::IoResult, pager::Pager, storage_interface::Page};
use crate::constants::{
    BUCKET_CHILDREN, DUMMY_SORTED_TABLE_FILE_SIZE, FILTER_FP_RATE, PAGE_SIZE,
    SORTED_TABLE_FILTER_ITEM_COUNT,
};
use bincode;
use serde::{Deserialize, Serialize};
use std::io;

#[derive(Debug, Clone)]
pub struct SortedStorePage {
    pub id: u64,
    pub bytes: [u8; PAGE_SIZE],
}

impl SortedStorePage {}

impl Page for SortedStorePage {
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

#[derive(Debug, Clone)]
pub struct IndexPage {
    pub id: u64,
    pub bytes: [u8; PAGE_SIZE],
}

impl IndexPage {}

impl Page for IndexPage {
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
    fn new() -> Self {
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
    fn new() -> Self {
        Self {
            index_size: 0,
            bitmap_size: 0,
            filter_keys: [0, 0, 0, 0],
        }
    }
}

// SortedStoreTable file structure -
//------------START----------------
// [metadata_block]
// [filter_bitmap_pages]
// [data_page1]
// [data_page2]
// .
// .
// .
// .
// [data_pageN]
// -------------Footer-----------
// [index_page1]
// [index_page2]
// .
// .
// [index_pageN]
//---------------END--------------

#[derive(Debug)]
pub struct SortedStoreTable {
    table_pager: Pager<SortedStorePage>,
    index: TableIndex,
    metadata: TableMetadata,
    filter: BloomFilter<str>,
}

impl SortedStoreTable {
    fn new(path: &str) -> IoResult<Self> {
        let table_pager = Pager::<SortedStorePage>::new(path)?;
        let mut metadata = TableMetadata::new();
        let index = TableIndex::new();
        let filter = BloomFilter::<str>::new(SORTED_TABLE_FILTER_ITEM_COUNT, FILTER_FP_RATE);

        let bitmap = filter.get_bitmap();
        let keys = filter.get_keys();

        let index_enc = bincode::serialize(&index)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let filter_enc = bincode::serialize(&bitmap)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let filter_len = filter_enc.as_slice().len();
        let index_len = index_enc.as_slice().len();

        metadata.bitmap_size = filter_len as u64;
        metadata.index_size = index_len as u64;
        metadata.filter_keys = keys;

        let meta_enc = bincode::serialize(&metadata)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        table_pager.flush_arbitrary(0, meta_enc.as_slice())?;
        table_pager.flush_arbitrary(48, filter_enc.as_slice())?;
        table_pager.flush_arbitrary(
            DUMMY_SORTED_TABLE_FILE_SIZE - index_len as usize,
            index_enc.as_slice(),
        )?;

        Ok(Self {
            table_pager,
            index,
            metadata,
            filter,
        })
    }

    fn new_load_from_disk(path: &str) -> IoResult<Self> {
        let table_pager = Pager::<SortedStorePage>::new(path)?;

        let mut metadata_buffer = [0 as u8; 48];
        table_pager.read_arbitrary_from_offset(0 as usize, &mut metadata_buffer)?;
        let metadata = bincode::deserialize::<TableMetadata>(&metadata_buffer)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let num_pages = (metadata.bitmap_size as f64 / PAGE_SIZE as f64).ceil() as usize;
        let mut buff_vec = Vec::new();

        for _ in 0..num_pages {
            let buff = [0 as u8; PAGE_SIZE];
            buff_vec.extend_from_slice(&buff);
        }

        table_pager.read_arbitrary_from_offset(48, buff_vec.as_mut_slice())?;

        let bitmap = bincode::deserialize::<Vec<bool>>(&buff_vec)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let filter = BloomFilter::new_with_preload(0.01, bitmap, metadata.filter_keys);

        let num_pages_index = (metadata.index_size as f64 / PAGE_SIZE as f64).ceil() as usize;
        let mut index_buff_vec = Vec::new();

        for _ in 0..num_pages_index {
            let buff = [0 as u8; PAGE_SIZE];
            index_buff_vec.push(buff);
        }

        let mut final_index_buff_vec = index_buff_vec.concat();

        table_pager.read_arbitrary_from_offset(
            DUMMY_SORTED_TABLE_FILE_SIZE - metadata.index_size as usize,
            final_index_buff_vec.as_mut_slice(),
        )?;

        let index = bincode::deserialize::<TableIndex>(final_index_buff_vec.as_slice())
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        Ok(Self {
            table_pager,
            index,
            metadata,
            filter,
        })
    }

    pub fn get_val_offset(&mut self, item: String) -> IoResult<(usize, usize)> {
        let contains = &self.filter.get(item.as_str())?;

        if !contains {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "val not found".to_string(),
            ));
        }

        let index = self.index.search(&item)?;

        let offset = self.index.get_offset(index);

        let mut page_buff = [0 as u8; PAGE_SIZE];
        self.table_pager
            .read_arbitrary_from_offset(offset, &mut page_buff)?;

        let data = bincode::deserialize::<Vec<KeyValOffset>>(&page_buff)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let idx = data
            .binary_search_by_key(&item, |entry| entry.key.clone())
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
        let (offset, size) = (data[idx].val_offset, data[idx].size);

        Ok((offset, size))
    }

    fn set_a_single_val(&mut self) -> IoResult<()> {
        let index_item = TableIndexEntry {
            key: "item".to_string(),
            page_offset: 48 + self.metadata.bitmap_size as usize,
        };

        let item = vec![KeyValOffset {
            key: "item".to_string(),
            val_offset: 4591,
            size: 800,
        }];

        self.filter.set(index_item.key.as_str())?;
        self.index.index.push(index_item);

        let bitmap = self.filter.get_bitmap();

        let index_enc = bincode::serialize(&self.index)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let filter_enc = bincode::serialize(&bitmap)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let item_enc = bincode::serialize(&item)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let filter_len = filter_enc.as_slice().len();
        let index_len = index_enc.as_slice().len();

        self.metadata.bitmap_size = filter_len as u64;
        self.metadata.index_size = index_len as u64;

        let meta_enc = bincode::serialize(&self.metadata)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        self.table_pager.flush_arbitrary(0, meta_enc.as_slice())?;
        self.table_pager
            .flush_arbitrary(48, filter_enc.as_slice())?;
        self.table_pager
            .flush_arbitrary(48 + filter_len, item_enc.as_slice())?;
        self.table_pager.flush_arbitrary(
            DUMMY_SORTED_TABLE_FILE_SIZE - index_len,
            index_enc.as_slice(),
        )?;

        Ok(())
    }
}

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

// bucket metadata file structure -
// ..............START..................
// [metadata_block]
// [bucket_children_block]
// [bucket_index_page1]
// [bucket_index_page2]
// .
// .
// .
// .
// [bucket_index_pageN]
//................END...................

#[derive(Debug)]
pub struct Bucket {
    pub bucket_pager: Pager<IndexPage>,
    pub index: BucketIndex,
    pub metadata: BucketMetadata,
    pub children: ChildrenBuckets,
}

impl Bucket {
    fn new(path: &str) -> IoResult<Self> {
        let mut bucket_pager = Pager::<IndexPage>::new(path)?;
        let mut metadata_buffer = [0 as u8; 16];
        bucket_pager.read_arbitrary_from_offset(0 as usize, &mut metadata_buffer)?;

        let metadata = bincode::deserialize::<BucketMetadata>(&metadata_buffer)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let num_pages_children =
            (metadata.children_data_size as f64 / PAGE_SIZE as f64).ceil() as usize;
        let mut children_buff = Vec::new();

        for _ in 0..num_pages_children {
            let buff = [0 as u8; PAGE_SIZE];
            children_buff.extend_from_slice(&buff);
        }

        bucket_pager.read_arbitrary_from_offset(16, children_buff.as_mut_slice())?;

        let children = bincode::deserialize::<ChildrenBuckets>(children_buff.as_slice())
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let index = bucket_pager.read_pages::<BucketIndexEntry>(
            0,
            16 + metadata.children_data_size as usize,
            metadata.index_size as usize,
        )?;

        Ok(Self {
            bucket_pager,
            metadata,
            index: BucketIndex { index },
            children: children,
        })
    }
}

mod test {

    use crate::storage::{
        pager::Pager,
        sorted_store::{
            Bucket, BucketIndexEntry, BucketMetadata, ChildBucket, ChildrenBuckets, IndexPage,
            SortedStoreTable,
        },
    };
    use bincode;
    use std::fs;

    #[test]
    fn test() {
        let path = "/home/sarthak17/Desktop/disk_test";
        let file_creation_result = fs::File::create(path);
        if let Err(err) = file_creation_result {
            panic!("Failed to create the file: {}", err);
        }

        let pager = Pager::<IndexPage>::new(path).expect("Failed");
        let mut c = Vec::new();

        for i in 0..16 {
            if i & 1 == 1 {
                c.push(ChildBucket::Child(path.to_string()));
            } else {
                c.push(ChildBucket::Null);
            }
        }

        let mut v = Vec::new();

        for _ in 0..9 {
            v.push(BucketIndexEntry {
                key: "test_key0".to_string(),
                sorted_table_file: "some_file_name".to_string(),
            })
        }
        v.push(BucketIndexEntry {
            key: "test_key1".to_string(),
            sorted_table_file: "/home/sarthak17/Desktop/disk_test_sst".to_string(),
        });

        let childern = ChildrenBuckets { children: c };

        let enc1 = bincode::serialize(&childern).expect("Failed to enode");
        let sz1 = enc1.as_slice().len();
        let enc2 = bincode::serialize(&v).expect("Failed to enode");
        let sz2 = enc2.as_slice().len();

        let meta = BucketMetadata {
            index_size: sz2 as u64,
            children_data_size: sz1 as u64,
        };

        let enc3 = bincode::serialize(&meta).expect("Failed to enode");

        pager
            .flush_arbitrary(0, enc3.as_slice())
            .expect("Failed to flush");
        pager
            .flush_arbitrary(16, enc1.as_slice())
            .expect("Failed to flush");
        pager
            .flush_arbitrary(16 + sz1, enc2.as_slice())
            .expect("Failed to flush");

        let bucket = Bucket::new(path).expect("Failed");
        let fl = &bucket.index.index[9];
        let mut a = "".to_string();

        a = fl.sorted_table_file.clone();

        let sst = SortedStoreTable::new(a.as_str()).expect("failed");
        let mut sst1 = SortedStoreTable::new_load_from_disk(a.as_str()).expect("failed");

        sst1.set_a_single_val().expect("failed");
        drop(sst1);
        drop(sst);

        let mut sst2 = SortedStoreTable::new_load_from_disk(a.as_str()).expect("failed");
        let val = sst2.get_val_offset("item".to_string()).expect("failed");

        assert_eq!(val, (4591, 800));

        fs::remove_file(path).unwrap();
        fs::remove_file(a.as_str()).unwrap();
    }

    #[test]
    fn test_sst() {
        let path = "/home/sarthak17/Desktop/disk_test";
        let file_creation_result = fs::File::create(path);
        if let Err(err) = file_creation_result {
            panic!("Failed to create the file: {}", err);
        }

        let sst = SortedStoreTable::new(path).expect("failed");
        let sst1 = SortedStoreTable::new_load_from_disk(path).expect("failed");

        fs::remove_file(path).unwrap();
    }
}
