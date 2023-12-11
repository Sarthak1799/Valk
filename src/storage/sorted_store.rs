use super::{
    bloom_filter::BloomFilter, engine::IoResult, pager::Pager, storage_interface::Page,
    storage_types as types,
};
use crate::constants::{
    COMPACTION_BUFFER_LOOP_COUNT, DUMMY_SORTED_TABLE_DATA_SIZE, DUMMY_SORTED_TABLE_FILE_SIZE,
    FILTER_FP_RATE, PAGE_SIZE, SORTED_STORE_BUFFER_PAGE_COUNT, SORTED_TABLE_FILTER_ITEM_COUNT,
};
use bincode;
use siphasher::sip128::SipHasher24;
use std::{
    cmp::min,
    collections::{BinaryHeap, HashMap},
    fs, io,
    rc::Rc,
    sync::{Arc, RwLock},
    time::{SystemTime, UNIX_EPOCH},
};

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
    table_pager: Pager<types::SortedStorePage>,
    index: types::TableIndex,
    metadata: types::TableMetadata,
    filter: BloomFilter<str>,
}

impl SortedStoreTable {
    fn new(path: &str) -> IoResult<Self> {
        let table_pager = Pager::<types::SortedStorePage>::new(path)?;
        let metadata = types::TableMetadata::new();
        let index = types::TableIndex::new();
        let filter = BloomFilter::<str>::new(SORTED_TABLE_FILTER_ITEM_COUNT, FILTER_FP_RATE);

        let mut table = Self {
            table_pager,
            index,
            metadata,
            filter,
        };

        table.save_state(Vec::new())?;

        Ok(table)
    }

    fn new_load_from_disk(path: &str) -> IoResult<Self> {
        let table_pager = Pager::<types::SortedStorePage>::new(path)?;

        let mut metadata_buffer = [0 as u8; 56];
        table_pager.read_arbitrary_from_offset(0 as usize, &mut metadata_buffer)?;
        let metadata = bincode::deserialize::<types::TableMetadata>(&metadata_buffer)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let num_pages = (metadata.bitmap_size as f64 / PAGE_SIZE as f64).ceil() as usize;
        let mut buff_vec = Vec::with_capacity(num_pages * PAGE_SIZE);
        unsafe { buff_vec.set_len(num_pages * PAGE_SIZE) }

        // for _ in 0..num_pages {
        //     let buff = [0 as u8; PAGE_SIZE];
        //     buff_vec.extend_from_slice(&buff);
        // }

        table_pager.read_arbitrary_from_offset(56, buff_vec.as_mut_slice())?;

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

        let index = bincode::deserialize::<types::TableIndex>(final_index_buff_vec.as_slice())
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        Ok(Self {
            table_pager,
            index,
            metadata,
            filter,
        })
    }

    fn save_state(&mut self, final_buff: Vec<u8>) -> IoResult<()> {
        let bitmap = self.filter.get_bitmap();
        let keys = self.filter.get_keys();
        self.index.sort();

        let index_enc = bincode::serialize(&self.index)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let filter_enc = bincode::serialize(&bitmap)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let filter_len = filter_enc.as_slice().len();
        let index_len = index_enc.as_slice().len();
        let data_len = (final_buff.as_slice().len()).div_ceil(PAGE_SIZE) * PAGE_SIZE;

        self.metadata.bitmap_size = filter_len as u64;
        self.metadata.index_size = index_len as u64;
        self.metadata.data_size += data_len as u64;
        self.metadata.filter_keys = keys;

        let meta_enc = bincode::serialize(&self.metadata)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        self.table_pager.flush_arbitrary(0, meta_enc.as_slice())?;
        self.table_pager
            .flush_arbitrary(56, filter_enc.as_slice())?;
        self.table_pager
            .flush_arbitrary(56 + filter_len, final_buff.as_slice())?;
        self.table_pager.flush_arbitrary(
            DUMMY_SORTED_TABLE_FILE_SIZE - index_len as usize,
            index_enc.as_slice(),
        )?;

        Ok(())
    }

    pub fn write_data_pages(
        &mut self,
        data: Vec<types::KeyValOffset>,
    ) -> IoResult<Vec<types::KeyValOffset>> {
        let mut num_of_pages_left =
            (DUMMY_SORTED_TABLE_DATA_SIZE - self.metadata.data_size as usize) / PAGE_SIZE;
        let mut current_offset =
            (56 + self.metadata.bitmap_size + self.metadata.data_size) as usize;
        let mut final_buff = Vec::new();
        let mut curr_page: Vec<&types::KeyValOffset> = Vec::new();
        let mut curr_size = 0;

        // println!("data is - {:?}", data);

        for idx in 0..data.len() {
            let ele = &data[idx];
            let ele_size = bincode::serialized_size(&ele)
                .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?
                as usize;

            if curr_size + ele_size > PAGE_SIZE {
                let first = curr_page[0].key.clone();
                let enc = bincode::serialize(&curr_page)
                    .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
                let page_len = enc.as_slice().len();

                let mut page = Vec::with_capacity(PAGE_SIZE);
                unsafe { page.set_len(PAGE_SIZE) }
                page[0..page_len].copy_from_slice(enc.as_slice());

                let entry = types::TableIndexEntry::new(first, current_offset as usize);
                self.index.set(entry);
                final_buff.extend_from_slice(page.as_slice());

                current_offset += PAGE_SIZE;
                curr_size = 0;
                curr_page.clear();
                num_of_pages_left -= 1;

                if num_of_pages_left == 0 {
                    self.save_state(final_buff)?;

                    let mut leftover_elements = Vec::new();
                    leftover_elements.clone_from_slice(&data[idx..]);

                    return Ok(leftover_elements);
                }
            }

            curr_page.push(ele);
            curr_size += ele_size;
            self.filter.set(ele.key.as_str())?;
        }

        let first = curr_page[0].key.clone();
        let enc = bincode::serialize(&curr_page)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
        let page_len = enc.as_slice().len();

        let mut page = Vec::with_capacity(PAGE_SIZE);
        unsafe { page.set_len(PAGE_SIZE) }
        page[0..page_len].copy_from_slice(enc.as_slice());

        let entry = types::TableIndexEntry::new(first, current_offset as usize);
        self.index.set(entry);
        final_buff.extend_from_slice(page.as_slice());
        curr_page.clear();

        self.save_state(final_buff)?;

        Ok(Vec::new())
    }

    pub fn get_val_offset(&self, item: String) -> IoResult<types::KeyValOffset> {
        let contains = &self.filter.get(item.as_str())?;

        if !contains {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "val not found".to_string(),
            ));
        }

        let index = self.index.search(&item)?.ok_or(io::Error::new(
            io::ErrorKind::Other,
            "val not found".to_string(),
        ))?;

        let offset = self.index.get_offset(index);

        let mut page_buff = Vec::with_capacity(PAGE_SIZE);
        unsafe { page_buff.set_len(PAGE_SIZE) }
        self.table_pager
            .read_arbitrary_from_offset(offset, page_buff.as_mut_slice())?;

        let data = bincode::deserialize::<Vec<types::KeyValOffset>>(page_buff.as_slice())
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let idx = data
            .binary_search_by_key(&item, |entry| entry.key.clone())
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
        let val = data[idx].clone();

        Ok(val)
    }

    fn get_first_val(&self) -> IoResult<types::KeyValOffset> {
        let offset = 56 + self.metadata.bitmap_size as usize;
        let mut buff = Vec::with_capacity(PAGE_SIZE);
        unsafe { buff.set_len(PAGE_SIZE) }

        self.table_pager
            .read_arbitrary_from_offset(offset, buff.as_mut_slice())?;

        let data = bincode::deserialize::<Vec<types::KeyValOffset>>(buff.as_slice())
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        Ok(data[0].clone())
    }

    pub fn get_compaction_buffer(&mut self, offset: usize) -> IoResult<Option<Vec<u8>>> {
        let fixed_offset = self.metadata.bitmap_size as usize + 56;
        let new_offset = offset + fixed_offset;
        let capacity = SORTED_STORE_BUFFER_PAGE_COUNT * PAGE_SIZE;
        let max_offset = fixed_offset as u64 + self.metadata.data_size;
        let max_curr_offset = min((new_offset + capacity) as u64, max_offset);
        let curr_len = max_curr_offset as i64 - new_offset as i64;

        if curr_len <= 0 {
            return Ok(None);
        }

        let mut buff = Vec::with_capacity(curr_len as usize);
        unsafe { buff.set_len(curr_len as usize) }

        self.table_pager
            .read_arbitrary_from_offset(new_offset, buff.as_mut_slice())?;

        Ok(Some(buff))
    }

    // test function
    fn set_a_single_val(&mut self) -> IoResult<()> {
        let index_item = types::TableIndexEntry {
            key: "item".to_string(),
            page_offset: 56 + self.metadata.bitmap_size as usize,
        };

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?
            .as_nanos();

        let item = vec![types::KeyValOffset {
            key: "item".to_string(),
            header: types::LogHeader {
                page_id: 0,
                offset: 4591,
                size: 800,
                timestamp,
            },
            rec_type: types::RecordType::AppendOrUpdate,
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
        self.metadata.data_size += item_enc.as_slice().len() as u64;

        let meta_enc = bincode::serialize(&self.metadata)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        self.table_pager.flush_arbitrary(0, meta_enc.as_slice())?;
        self.table_pager
            .flush_arbitrary(56, filter_enc.as_slice())?;
        self.table_pager
            .flush_arbitrary(56 + filter_len, item_enc.as_slice())?;
        self.table_pager.flush_arbitrary(
            DUMMY_SORTED_TABLE_FILE_SIZE - index_len,
            index_enc.as_slice(),
        )?;

        Ok(())
    }
}

pub struct BucketsController {
    pub buckets: HashMap<u128, Rc<Bucket>>,
}

impl BucketsController {
    fn new() -> Self {
        Self {
            buckets: HashMap::new(),
        }
    }

    pub fn get_children_buckets(
        &mut self,
        hash: u128,
        lvl: u16,
        config: types::Config,
    ) -> IoResult<Vec<Rc<Bucket>>> {
        let new_lvl = lvl + 1;
        let mut children_hashes = Vec::new();
        let mut buckets = Vec::new();

        for cnt in 0..16 {
            let new_hash = hash | (cnt << (new_lvl * 4));
            children_hashes.push(new_hash);
        }

        for key in children_hashes {
            let contains = self.buckets.get(&key);
            if let Some(b) = contains {
                buckets.push(b.clone());
            } else {
                let b = self.get_bucket(&config, key)?;
                buckets.push(b);
            }
        }

        Ok(buckets)
    }

    fn get_bucket(&mut self, config: &types::Config, bucket_key: u128) -> IoResult<Rc<Bucket>> {
        let file = config.bucket_files.get(&bucket_key).ok_or(io::Error::new(
            io::ErrorKind::Other,
            "Bucket file not found in config".to_string(),
        ))?;

        let bucket = Bucket::new_load_from_disk(file.as_str())?;
        self.buckets.insert(bucket_key, Rc::new(bucket));

        let bucket = self
            .buckets
            .get(&bucket_key)
            .ok_or(io::Error::new(
                io::ErrorKind::Other,
                "Bucket not found".to_string(),
            ))?
            .clone();

        Ok(bucket)
    }

    pub fn get(
        &mut self,
        key: String,
        hasher: SipHasher24,
        config: &types::Config,
    ) -> IoResult<types::KeyValOffset> {
        let hashed = hasher.hash(key.as_bytes()).as_u128();
        let bucket_key = hashed & 15;
        let contains = self.buckets.get(&bucket_key);

        if let Some(bucket) = contains {
            bucket.get(key)
        } else {
            let bucket = self.get_bucket(config, bucket_key)?;
            bucket.get(key)
        }
    }
}

// bucket metadata file structure -
// ..............START..................
// [metadata_block]
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
    pub bucket_pager: Pager<types::IndexPage>,
    pub index: types::BucketIndex,
    pub metadata: types::BucketMetadata,
    //pub children: types::ChildrenBuckets,
    pub compaction_flag: Arc<RwLock<types::CompactionFlag>>,
}

impl Bucket {
    fn new(path: &str, lvl: u16, hash: u128) -> IoResult<Self> {
        let bucket_pager = Pager::<types::IndexPage>::new(path)?;
        let index = types::BucketIndex::new();
        let metadata = types::BucketMetadata::new(lvl, hash);
        // let children = types::ChildrenBuckets::new();

        let mut bucket = Self {
            bucket_pager,
            index,
            metadata,
            // children,
            compaction_flag: Arc::new(RwLock::new(types::CompactionFlag::new())),
        };

        bucket.save_state()?;

        Ok(bucket)
    }

    fn new_load_from_disk(path: &str) -> IoResult<Self> {
        let mut bucket_pager = Pager::<types::IndexPage>::new(path)?;
        let mut metadata_buffer = [0 as u8; 32];
        bucket_pager.read_arbitrary_from_offset(0 as usize, &mut metadata_buffer)?;

        let metadata = bincode::deserialize::<types::BucketMetadata>(&metadata_buffer)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        // let num_pages_children =
        //     (metadata.children_data_size as f64 / PAGE_SIZE as f64).ceil() as usize;
        // let mut children_buff = Vec::with_capacity(num_pages_children * PAGE_SIZE);
        // unsafe { children_buff.set_len(num_pages_children * PAGE_SIZE) }

        // // for _ in 0..num_pages_children {
        // //     let buff = [0 as u8; PAGE_SIZE];
        // //     children_buff.extend_from_slice(&buff);
        // // }

        // bucket_pager.read_arbitrary_from_offset(16, children_buff.as_mut_slice())?;

        // let children = bincode::deserialize::<types::ChildrenBuckets>(children_buff.as_slice())
        //     .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let index = bucket_pager.read_pages::<types::BucketIndexEntry>(
            0,
            32,
            metadata.index_size as usize,
        )?;

        Ok(Self {
            bucket_pager,
            metadata,
            index: types::BucketIndex { index },
            // children,
            compaction_flag: Arc::new(RwLock::new(types::CompactionFlag::new())),
        })
    }

    fn save_state(&mut self) -> IoResult<()> {
        let index_enc = bincode::serialize(&self.index.index)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        // let children_enc = bincode::serialize(&children)
        //     .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        // metadata.children_data_size = children_enc.as_slice().len() as u64;
        self.metadata.index_size = index_enc.as_slice().len() as u64;

        let meta_enc = bincode::serialize(&self.metadata)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        self.bucket_pager.flush_arbitrary(0, meta_enc.as_slice())?;
        self.bucket_pager
            .flush_arbitrary(32, index_enc.as_slice())?;
        // bucket_pager.flush_arbitrary(
        //     metadata.children_data_size as usize + 16,
        //     index_enc.as_slice(),
        // )?;

        Ok(())
    }

    fn test_and_set_flag(&mut self) -> IoResult<bool> {
        let read_flag = self
            .compaction_flag
            .read()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let flag = read_flag.check();
        drop(read_flag);

        let mut write_flag = self
            .compaction_flag
            .write()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        write_flag.set();

        Ok(flag)
    }

    fn unset_flag(&mut self) -> IoResult<()> {
        let mut write_flag = self
            .compaction_flag
            .write()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        write_flag.unset();

        Ok(())
    }

    pub fn get(&self, key: String) -> IoResult<types::KeyValOffset> {
        let file_idx = self.index.search(&key)?;
        let idx = file_idx.ok_or(io::Error::new(
            io::ErrorKind::Other,
            "element not found".to_string(),
        ))?;

        println!("nucket idx is - {}", idx);
        let file = &self.index.index[idx];
        println!("file is - {:?}", file);
        let sst = SortedStoreTable::new_load_from_disk(&file.sorted_table_file)?;
        //  println!("sst is - {:?}", sst);

        sst.get_val_offset(key)
    }

    pub fn get_compaction_tables_indices(
        &mut self,
        input: &types::CompactionBuffer,
    ) -> IoResult<(Option<usize>, Option<usize>)> {
        let first = input.type_buffer.first().ok_or(io::Error::new(
            io::ErrorKind::Other,
            "element not found".to_string(),
        ))?;

        let last = input.type_buffer.last().ok_or(io::Error::new(
            io::ErrorKind::Other,
            "element not found".to_string(),
        ))?;

        let start_file = self.index.search(&first.key)?;
        let end_file = self.index.search(&last.key)?;

        Ok((start_file, end_file))
    }

    fn get_compaction_tables(
        &self,
        start: Option<usize>,
        end: Option<usize>,
    ) -> IoResult<Vec<SortedStoreTable>> {
        let mut sst_vec = Vec::new();
        if start.is_none() && end.is_none() {
            return Ok(sst_vec);
        }

        let frst = start.ok_or(io::Error::new(
            io::ErrorKind::Other,
            "Index not found in bucket".to_string(),
        ))?;

        let lst = end.ok_or(io::Error::new(
            io::ErrorKind::Other,
            "Index not found in bucket".to_string(),
        ))?;

        for idx in frst..lst + 1 {
            let file = &self.index.index[idx];
            let sst = SortedStoreTable::new_load_from_disk(&file.sorted_table_file)?;
            sst_vec.push(sst);
        }

        Ok(sst_vec)
    }

    fn reset_index(
        &mut self,
        indices: (Option<usize>, Option<usize>),
        sst_vec: Vec<(String, SortedStoreTable)>,
    ) -> IoResult<()> {
        let (start, end) = indices;
        let is_index_empty = start.is_none() && end.is_none();

        let mut new_tables = Vec::new();
        let mut old_tables_for_deletion = Vec::new();

        for (path, sst) in sst_vec {
            let val = sst.get_first_val()?.key;

            new_tables.push(types::BucketIndexEntry {
                key: val,
                sorted_table_file: path,
            });
        }

        if !is_index_empty {
            let frst = start.ok_or(io::Error::new(
                io::ErrorKind::Other,
                "element not found".to_string(),
            ))?;

            let lst = end.ok_or(io::Error::new(
                io::ErrorKind::Other,
                "element not found".to_string(),
            ))?;

            for idx in 0..self.index.index.len() {
                if idx >= frst && idx <= lst {
                    continue;
                }
                new_tables.push(self.index.index[idx].clone());
            }

            old_tables_for_deletion = (frst..lst + 1)
                .into_iter()
                .map(|ind| self.index.index[ind].sorted_table_file.clone())
                .collect::<Vec<_>>();
        }

        // println!("bucket indx is - {:?}", new_tables);

        new_tables.sort();

        let new_index = types::BucketIndex { index: new_tables };
        self.index = new_index;

        self.save_state()?;

        for table in old_tables_for_deletion {
            fs::remove_file(table.as_str())?;
        }

        Ok(())
    }

    fn write_to_tables(
        &mut self,
        table_vec: &mut Vec<(String, SortedStoreTable)>,
        path_cnt: &mut i32,
        buffer: Vec<types::KeyValOffset>,
    ) -> IoResult<()> {
        let (_last_path, last_table) = table_vec.last_mut().ok_or(io::Error::new(
            io::ErrorKind::Other,
            "table not found".to_string(),
        ))?;

        let leftover_elements = last_table.write_data_pages(buffer)?;

        if leftover_elements.len() > 0 {
            // to be replaced with a dynamic file generator
            let new_path = format!("/home/sarthak17/Desktop/SomeSSTfile_{}", path_cnt);
            let mut new_table = SortedStoreTable::new(new_path.as_str())?;
            new_table.write_data_pages(leftover_elements)?;

            table_vec.push((new_path, new_table));
        }

        Ok(())
    }

    pub fn perform_merge(&mut self, input_buffer: types::CompactionBuffer) -> IoResult<()> {
        let flag = self.test_and_set_flag()?;
        if flag {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "failed to lock bucket for compaction".to_string(),
            ));
        }

        let mut offset = 0;
        let mut path_cnt = 0;

        // to be replaced with a dynamic file generator
        let path = format!("/home/sarthak17/Desktop/SomeSSTfile_{}", path_cnt);

        let curr_table = SortedStoreTable::new(path.as_str())?;

        let (start, end) = self.get_compaction_tables_indices(&input_buffer)?;

        // println!("reached2 {:?} {:?}", start, end);

        let mut tables = self.get_compaction_tables(start, end)?;

        let mut new_tables = Vec::new();
        new_tables.push((path, curr_table));

        let mut final_buffers = BinaryHeap::new();
        final_buffers.push(input_buffer);

        for _ in 0..COMPACTION_BUFFER_LOOP_COUNT {
            for table in &mut tables {
                let buffer = table.get_compaction_buffer(offset)?;
                if let Some(buff) = buffer {
                    let mut merge_vec = Vec::new();

                    for buf in buff.chunks(PAGE_SIZE) {
                        let item = bincode::deserialize::<Vec<types::KeyValOffset>>(&buf)
                            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

                        merge_vec.extend(item);
                    }

                    final_buffers.push(types::CompactionBuffer {
                        type_buffer: merge_vec,
                    });
                }
            }

            if final_buffers.is_empty() {
                break;
            }

            offset += SORTED_STORE_BUFFER_PAGE_COUNT * PAGE_SIZE;

            while final_buffers.len() > 1 {
                let first = final_buffers.pop().ok_or(io::Error::new(
                    io::ErrorKind::Other,
                    "failed to pop buffer from heap".to_string(),
                ))?;

                let second = final_buffers.pop().ok_or(io::Error::new(
                    io::ErrorKind::Other,
                    "failed to pop buffer from heap".to_string(),
                ))?;

                let merged_buff = merge_two(first.type_buffer, second.type_buffer);

                final_buffers.push(types::CompactionBuffer {
                    type_buffer: merged_buff,
                });
            }

            let output_buff = final_buffers.pop().ok_or(io::Error::new(
                io::ErrorKind::Other,
                "failed to pop buffer from heap".to_string(),
            ))?;

            self.write_to_tables(&mut new_tables, &mut path_cnt, output_buff.type_buffer)?;
        }

        // println!("tables are - {:?}", new_tables);

        self.reset_index((start, end), new_tables)?;

        self.unset_flag()?;

        Ok(())
    }
}

fn merge_two(
    one: Vec<types::KeyValOffset>,
    two: Vec<types::KeyValOffset>,
) -> Vec<types::KeyValOffset> {
    let mut idx1 = 0;
    let mut idx2 = 0;
    let len = one.len() + two.len();
    let mut output = Vec::with_capacity(len);
    // println!("reached6 {:?} {:?}", one, two);

    while idx1 < one.len() && idx2 < two.len() {
        if one[idx1].eq(&two[idx2]) {
            if one[idx1].rec_type == types::RecordType::Delete
                || two[idx2].rec_type == types::RecordType::Delete
            {
                idx1 += 1;
                idx2 += 1;
                continue;
            } else if one[idx1].header.timestamp < two[idx2].header.timestamp {
                output.push(one[idx1].clone());
                idx1 += 1;
            } else {
                output.push(two[idx2].clone());
                idx2 += 1;
            }
        } else if one[idx1] < two[idx2] {
            output.push(one[idx1].clone());
            idx1 += 1;
        } else {
            output.push(two[idx2].clone());
            idx2 += 1;
        }
    }

    while idx1 < one.len() {
        output.push(one[idx1].clone());
        idx1 += 1;
    }

    while idx2 < two.len() {
        output.push(two[idx2].clone());
        idx2 += 1;
    }

    output
}

mod test {

    use crate::storage::{
        pager::Pager,
        sorted_store::{Bucket, SortedStoreTable},
        storage_types::{
            BucketIndexEntry, BucketMetadata, ChildBucket, ChildrenBuckets, CompactionBuffer,
            IndexPage, InputBuffer, KeyValOffset, LogHeader, RecordType,
        },
    };
    use bincode;
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn test() {
        let path = "/home/sarthak17/Desktop/disk_test";
        let file_creation_result = fs::File::create(path);
        if let Err(err) = file_creation_result {
            panic!("Failed to create the file: {}", err);
        }

        let pager = Pager::<IndexPage>::new(path).expect("Failed");
        // let mut c = Vec::new();

        // for i in 0..16 {
        //     if i & 1 == 1 {
        //         c.push(ChildBucket::Child(path.to_string()));
        //     } else {
        //         c.push(ChildBucket::Null);
        //     }
        // }

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

        // let childern = ChildrenBuckets { children: c };

        // let enc1 = bincode::serialize(&childern).expect("Failed to enode");
        // let sz1 = enc1.as_slice().len();
        let enc2 = bincode::serialize(&v).expect("Failed to enode");
        let sz2 = enc2.as_slice().len();

        let meta = BucketMetadata {
            index_size: sz2 as u64,
            lvl: 0,
            hash: 0,
            // children_data_size: sz1 as u64,
        };

        let enc3 = bincode::serialize(&meta).expect("Failed to enode");

        pager
            .flush_arbitrary(0, enc3.as_slice())
            .expect("Failed to flush");
        pager
            .flush_arbitrary(32, enc2.as_slice())
            .expect("Failed to flush");
        // pager
        //     .flush_arbitrary(16 + sz1, enc2.as_slice())
        //     .expect("Failed to flush");

        let bucket = Bucket::new_load_from_disk(path).expect("Failed");
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

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("operation failed")
            .as_nanos();

        let item = KeyValOffset {
            key: "item".to_string(),
            header: LogHeader {
                page_id: 0,
                offset: 4591,
                size: 800,
                timestamp,
            },
            rec_type: RecordType::AppendOrUpdate,
        };

        assert_eq!(item, val);

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

    #[test]
    fn test_bucket() {
        let path = "/home/sarthak17/Desktop/disk_test";
        let file_creation_result = fs::File::create(path);
        if let Err(err) = file_creation_result {
            panic!("Failed to create the file: {}", err);
        }

        let bucket = Bucket::new(path, 0, 0).expect("failed");
        let bucket1 = Bucket::new_load_from_disk(path).expect("failed");

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_compaction() {
        let path = "/home/sarthak17/Desktop/disk_test";
        let file_creation_result = fs::File::create(path);
        if let Err(err) = file_creation_result {
            panic!("Failed to create the file: {}", err);
        }

        let mut bucket = Bucket::new(path, 0, 0).expect("failed");

        let timestamp1 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("operation failed")
            .as_nanos();
        let timestamp2 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("operation failed")
            .as_nanos();
        let timestamp3 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("operation failed")
            .as_nanos();

        let item1 = KeyValOffset {
            key: "item1".to_string(),
            header: LogHeader {
                page_id: 0,
                offset: 4591,
                size: 800,
                timestamp: timestamp1,
            },
            rec_type: RecordType::AppendOrUpdate,
        };
        let item2 = KeyValOffset {
            key: "item2".to_string(),
            header: LogHeader {
                page_id: 0,
                offset: 4591,
                size: 800,
                timestamp: timestamp2,
            },
            rec_type: RecordType::AppendOrUpdate,
        };
        let item3 = KeyValOffset {
            key: "item3".to_string(),
            header: LogHeader {
                page_id: 0,
                offset: 4591,
                size: 800,
                timestamp: timestamp3,
            },
            rec_type: RecordType::AppendOrUpdate,
        };

        let mut buffer = CompactionBuffer::new();
        buffer.insert(item1);
        buffer.insert(item2);
        buffer.insert(item3);

        bucket.perform_merge(buffer).expect("failed to compact");

        let one = bucket.get("item1".to_string()).expect("failed to get");
        let two = bucket.get("item3".to_string()).expect("failed to get");

        let timestamp4 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("operation failed")
            .as_nanos();
        let item4 = KeyValOffset {
            key: "item3".to_string(),
            header: LogHeader {
                page_id: 0,
                offset: 4591,
                size: 800,
                timestamp: timestamp4,
            },
            rec_type: RecordType::Delete,
        };

        let mut new_buffer = CompactionBuffer::new();
        new_buffer.insert(item4);

        bucket.perform_merge(new_buffer).expect("failed to compact");
        let _val = bucket.get("item3".to_string()).expect("failed to get");

        fs::remove_file(path).unwrap();
    }
}
