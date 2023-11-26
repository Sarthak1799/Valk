use super::{engine::IoResult, pager::Pager, storage_interface::Page};
use crate::constants::{BUCKET_CHILDREN, PAGE_SIZE};
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
pub struct TableIndexEntry {
    pub key: String,
    pub page_offset: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableIndex {
    pub index: Vec<TableIndexEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    pub index_size: u64,
}

#[derive(Debug)]
pub struct SortedStoreTable {
    table_pager: Pager<SortedStorePage>,
    index: TableIndex,
    metadata: TableMetadata,
}

impl SortedStoreTable {
    fn new(path: &str) -> IoResult<Self> {
        let table_pager = Pager::<SortedStorePage>::new(path)?;

        let mut metadata_buffer = [0 as u8; 8];
        table_pager.read_arbitrary_from_offset(0 as usize, &mut metadata_buffer)?;

        let metadata = bincode::deserialize::<TableMetadata>(&metadata_buffer)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let mut index_buff = [0 as u8; PAGE_SIZE];

        table_pager.read_arbitrary_from_offset(8, &mut index_buff)?;

        let index = bincode::deserialize::<TableIndex>(&index_buff)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        Ok(Self {
            table_pager,
            index,
            metadata,
        })
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

// [metadata_block]
// [bucket_children_block]
// [bucket_index_page1]
// [bucket_index_page2]
// .
// .
// .
// .
// [bucket_index_pageN]

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

        let mut children_buff = [0 as u8; PAGE_SIZE];

        bucket_pager.read_arbitrary_from_offset(16, &mut children_buff)?;

        let children = bincode::deserialize::<ChildrenBuckets>(&children_buff)
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

        for _ in 0..10 {
            v.push(BucketIndexEntry {
                key: "test_key".to_string(),
                sorted_table_file: "some_file_name".to_string(),
            })
        }

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

        println!("len is - {:?}", sz1);
        println!("len is - {:?}", sz2);

        let bucket = Bucket::new(path).expect("Failed");

        fs::remove_file(path).unwrap();
    }
}
