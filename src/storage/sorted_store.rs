use super::{
    bloom_filter::BloomFilter,
    engine::{GenericFindResult, IoResult},
    pager::Pager,
    storage_interface::{Compaction, CompactionPreprocessing},
    storage_types as types,
};
use crate::constants::{
    COMPACTION_BUFFER_LOOP_COUNT, DUMMY_SORTED_BUCKET_DATA_SIZE, DUMMY_SORTED_TABLE_DATA_SIZE,
    DUMMY_SORTED_TABLE_FILE_SIZE, FILTER_FP_RATE, PAGE_SIZE, SORTED_STORE_BUFFER_PAGE_COUNT,
    SORTED_TABLE_FILTER_ITEM_COUNT,
};
use bincode;
use rayon;
use siphasher::sip128::SipHasher24;
use std::{
    cmp::min,
    collections::{BinaryHeap, HashMap},
    fs, io,
    iter::zip,
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc, RwLock,
    },
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

    pub fn new_load_from_disk(path: &str) -> IoResult<Self> {
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

    pub fn get_data_size(&self) -> u64 {
        self.metadata.data_size
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

    pub fn get_val_offset(&self, item: String) -> GenericFindResult<types::KeyValOffset> {
        let contains = &self
            .filter
            .get(item.as_str())
            .map_err(|err| types::GenericFindErrorKind::IoError(err.to_string()))?;

        if !contains {
            return Err(types::GenericFindErrorKind::NotFound(
                "value not found".to_string(),
            ));
        }

        let index = self
            .index
            .search(&item)
            .ok_or(types::GenericFindErrorKind::NotFound(
                "value not found".to_string(),
            ))?;

        let offset = self.index.get_offset(index);

        let mut page_buff = Vec::with_capacity(PAGE_SIZE);
        unsafe { page_buff.set_len(PAGE_SIZE) }
        self.table_pager
            .read_arbitrary_from_offset(offset, page_buff.as_mut_slice())
            .map_err(|err| types::GenericFindErrorKind::IoError(err.to_string()))?;

        let data = bincode::deserialize::<Vec<types::KeyValOffset>>(page_buff.as_slice())
            .map_err(|err| types::GenericFindErrorKind::IoError(err.to_string()))?;

        let idx = data
            .binary_search_by_key(&item, |entry| entry.key.clone())
            .map_err(|err| types::GenericFindErrorKind::NotFound("value not found".to_string()))?;

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

    pub fn get_compaction_buffer(&self, offset: usize) -> IoResult<Option<Vec<u8>>> {
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

#[derive(Debug)]
pub struct BucketGlobalStateManager {
    pub pager: Pager<types::IndexPage>,
    pub metadata: types::BucketsControllerMetadata,
    pub initialized_buckets: Vec<u128>,
    pub curr_bucket_state: HashMap<u128, Arc<Bucket>>,
    pub reciever: Receiver<types::BucketStateFetchRequest>,
}

impl BucketGlobalStateManager {
    pub fn init() -> IoResult<()> {
        let pager = Pager::<types::IndexPage>::new("storage/buckets_state_manager.db")?;
        let metadata = types::BucketsControllerMetadata::new();
        let bytes = bincode::serialize(&metadata)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        pager.flush_arbitrary(0, bytes.as_slice())?;

        Ok(())
    }

    pub fn new_load_from_disk(
        reciever: Receiver<types::BucketStateFetchRequest>,
    ) -> IoResult<Self> {
        let pager = Pager::<types::IndexPage>::new("storage/buckets_state_manager.db")?;

        let mut buff = [0 as u8; 8];

        let metadata = bincode::deserialize::<types::BucketsControllerMetadata>(&mut buff)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
        let size = metadata.size;
        let mut init_buff = Vec::with_capacity(size);
        unsafe { init_buff.set_len(size) }

        let initialized_buckets = bincode::deserialize::<Vec<u128>>(init_buff.as_mut_slice())
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        Ok(Self {
            pager,
            metadata,
            initialized_buckets,
            curr_bucket_state: HashMap::new(),
            reciever,
        })
    }

    pub fn serve_req(mut self) -> IoResult<()> {
        while let Ok(req) = self.reciever.recv() {
            let (find_key, lvl, file, other_sender) = (req.key, req.lvl, req.file, req.sender);

            let maybe_bucket = self.curr_bucket_state.get(&find_key);

            if let Some(bucket) = maybe_bucket {
                other_sender
                    .send((Some(bucket.clone()), false))
                    .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
            } else {
                let present = self.initialized_buckets.iter().any(|key| key == &find_key);

                let bucket = if present {
                    let buck = Bucket::new_load_from_disk(file.as_str())?;
                    let ret = Arc::new(buck);
                    self.curr_bucket_state.insert(find_key, ret.clone());
                    ret
                } else {
                    let buck = Bucket::new(file.as_str(), lvl, find_key)?;
                    let ret = Arc::new(buck);
                    self.curr_bucket_state.insert(find_key, ret.clone());
                    self.initialized_buckets.push(find_key);
                    ret
                };

                other_sender
                    .send((Some(bucket.clone()), present))
                    .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct BucketsController {
    pub state_sender: Sender<types::BucketStateFetchRequest>,
}

impl BucketsController {
    pub fn new(sender: Sender<types::BucketStateFetchRequest>) -> Self {
        Self {
            state_sender: sender,
        }
    }

    pub fn get_children_buckets(
        &self,
        hash: u128,
        lvl: i32,
        config: &types::EngineConfig,
    ) -> IoResult<Vec<Arc<Bucket>>> {
        let new_lvl = (lvl + 1) as u32;
        let mut children_hashes = Vec::new();
        let mut buckets = Vec::new();

        for cnt in 0..16 {
            let new_hash = hash | (cnt << (new_lvl * 4));
            children_hashes.push(new_hash);
        }

        for key in children_hashes {
            let (bucket, _present) = self.get_bucket(config, key, new_lvl as u16)?;
            buckets.push(bucket);
        }

        Ok(buckets)
    }

    fn get_bucket(
        &self,
        config: &types::EngineConfig,
        bucket_key: u128,
        lvl: u16,
    ) -> IoResult<(Arc<Bucket>, bool)> {
        let file = config
            .bucket_files
            .get(&bucket_key)
            .ok_or(io::Error::new(
                io::ErrorKind::Other,
                "Bucket file not found in config".to_string(),
            ))?
            .clone();

        let (curr_sender, curr_reciever) = mpsc::channel();
        let contain_req =
            types::BucketStateFetchRequest::new_fetch_req(bucket_key, file, lvl, curr_sender);

        self.state_sender
            .send(contain_req)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let (bucket, present) = curr_reciever
            .recv()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let bucket = bucket.ok_or(io::Error::new(
            io::ErrorKind::Other,
            "failed to fetch bucket".to_string(),
        ))?;

        Ok((bucket, present))
    }

    pub fn get(&self, key: String, config: types::EngineConfig) -> IoResult<types::KeyValOffset> {
        let mut curr_lvl = 0;
        let mut bucket_key = 0;
        let hasher = &config.hasher;
        let hashed = hasher.hash(key.as_bytes()).as_u128();

        loop {
            let lvl_hash = (hashed >> (curr_lvl * 4)) & 15;
            bucket_key = bucket_key | (lvl_hash << (curr_lvl * 4));

            let (bucket, present) = self.get_bucket(&config, bucket_key, curr_lvl)?;
            if !present {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Value not found".to_string(),
                ));
            }

            let res = bucket.get(key.clone());
            curr_lvl += 1;

            match res {
                Ok(val) => return Ok(val),
                Err(types::GenericFindErrorKind::IoError(e)) => {
                    return Err(io::Error::new(io::ErrorKind::Other, e))
                }
                Err(types::GenericFindErrorKind::Other(e)) => {
                    return Err(io::Error::new(io::ErrorKind::Other, e))
                }
                Err(types::GenericFindErrorKind::NotFound(_e)) => continue,
            }
        }
    }

    pub fn trigger_compaction(
        &self,
        input_buffer: types::InputBuffer,
        config: types::EngineConfig,
    ) -> IoResult<()> {
        let hasher = config.hasher.clone();
        let buckets = self.get_children_buckets(0, -1, &config)?;

        let (sender, receiver) = mpsc::channel();

        for (bucket, buff) in zip(buckets, input_buffer.buffers) {
            let pass = sender.clone();

            rayon::spawn(move || {
                let _res = compaction_perform_merge(bucket, buff, pass)
                    .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()));
            })
        }

        while let Ok(res) = receiver.recv() {
            match res.result {
                Ok(_) => {
                    let bucket = res.bucket.clone();
                    let size = bucket.get_size()?;

                    if size > DUMMY_SORTED_BUCKET_DATA_SIZE {
                        let pass = sender.clone();

                        let metadata = bucket.metadata.clone();
                        let reader = metadata
                            .read()
                            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
                        let bucket_meta = reader.get_metadata();
                        let children = self.get_children_buckets(
                            bucket_meta.hash,
                            bucket_meta.lvl as i32,
                            &config,
                        )?;

                        rayon::spawn(move || {
                            let _res =
                                bucket
                                    .trigger_compaction(hasher, children, pass)
                                    .map_err(|err| {
                                        io::Error::new(io::ErrorKind::Other, err.to_string())
                                    });
                        })
                    }
                }
                Err(e) => {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("Compaction failed with error - {:?}", e),
                    ))
                }
            }
        }

        Ok(())
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
    pub index: Arc<RwLock<types::BucketIndex>>,
    pub metadata: Arc<RwLock<types::BucketMetadata>>,
    //pub children: types::ChildrenBuckets,
    pub compaction_flag: Arc<RwLock<types::GenericFlag>>,
}

impl Bucket {
    fn new(path: &str, lvl: u16, hash: u128) -> IoResult<Self> {
        let bucket_pager = Pager::<types::IndexPage>::new(path)?;
        let index = Arc::new(RwLock::new(types::BucketIndex::new()));
        let metadata = Arc::new(RwLock::new(types::BucketMetadata::new(lvl, hash)));
        // let children = types::ChildrenBuckets::new();

        let bucket = Self {
            bucket_pager,
            index,
            metadata,
            // children,
            compaction_flag: Arc::new(RwLock::new(types::GenericFlag::new())),
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
            metadata: Arc::new(RwLock::new(metadata)),
            index: Arc::new(RwLock::new(types::BucketIndex { index })),
            // children,
            compaction_flag: Arc::new(RwLock::new(types::GenericFlag::new())),
        })
    }

    fn save_state(&self) -> IoResult<()> {
        let reader = self
            .index
            .read()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let index_enc = bincode::serialize(reader.get_index())
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        drop(reader);

        // let children_enc = bincode::serialize(&children)
        //     .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        // metadata.children_data_size = children_enc.as_slice().len() as u64;

        let mut writer = self
            .metadata
            .write()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        writer.set_index_size(index_enc.as_slice().len() as u64);

        let meta_enc = bincode::serialize(writer.get_metadata())
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        drop(writer);

        self.bucket_pager.flush_arbitrary(0, meta_enc.as_slice())?;
        self.bucket_pager
            .flush_arbitrary(32, index_enc.as_slice())?;
        // bucket_pager.flush_arbitrary(
        //     metadata.children_data_size as usize + 16,
        //     index_enc.as_slice(),
        // )?;

        Ok(())
    }

    fn test_and_set_flag(&self) -> IoResult<bool> {
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

    fn unset_flag(&self) -> IoResult<()> {
        let mut write_flag = self
            .compaction_flag
            .write()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        write_flag.unset();

        Ok(())
    }

    pub fn get(&self, key: String) -> GenericFindResult<types::KeyValOffset> {
        let reader = self
            .index
            .read()
            .map_err(|err| types::GenericFindErrorKind::Other(err.to_string()))?;

        let idx = reader
            .search(&key)
            .ok_or(types::GenericFindErrorKind::NotFound(
                "not found in bucket index".to_string(),
            ))?;

        println!("nucket idx is - {}", idx);
        let file = &reader.index[idx];
        println!("file is - {:?}", file);
        let sst = SortedStoreTable::new_load_from_disk(&file.sorted_table_file)
            .map_err(|err| types::GenericFindErrorKind::IoError(err.to_string()))?;
        //  println!("sst is - {:?}", sst);

        sst.get_val_offset(key)
    }

    pub fn get_compaction_tables_indices(
        &self,
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

        let reader = self
            .index
            .read()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let start_file = reader.search(&first.key);
        let end_file = reader.search(&last.key);

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

        let reader = self
            .index
            .read()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        for idx in frst..lst + 1 {
            let file = &reader.index[idx];
            let sst = SortedStoreTable::new_load_from_disk(&file.sorted_table_file)?;
            sst_vec.push(sst);
        }

        Ok(sst_vec)
    }

    fn reset_index(
        &self,
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

            let reader = self
                .index
                .read()
                .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

            for idx in 0..reader.index.len() {
                if idx >= frst && idx <= lst {
                    continue;
                }
                new_tables.push(reader.index[idx].clone());
            }

            old_tables_for_deletion = (frst..lst + 1)
                .into_iter()
                .map(|ind| reader.index[ind].sorted_table_file.clone())
                .collect::<Vec<_>>();
        }

        // println!("bucket indx is - {:?}", new_tables);

        new_tables.sort();

        let mut writer = self
            .index
            .write()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        writer.set_new_index(new_tables);
        drop(writer);

        self.save_state()?;

        for table in old_tables_for_deletion {
            fs::remove_file(table.as_str())?;
        }

        Ok(())
    }

    fn write_to_tables(
        &self,
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

    pub fn get_size(&self) -> IoResult<usize> {
        let reader = self
            .index
            .read()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let mut data_size = 0;
        let _ = reader
            .get_index()
            .iter()
            .map(|idx: &types::BucketIndexEntry| -> IoResult<()> {
                let sst = SortedStoreTable::new_load_from_disk(&idx.sorted_table_file)?;
                data_size += sst.get_data_size();
                Ok(())
            });

        Ok(data_size as usize)
    }

    pub fn get_compaction_file_entry(&self) -> IoResult<types::BucketIndexEntry> {
        let reader = self
            .index
            .read()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let mut data_size = 0;
        let mut entry = types::BucketIndexEntry {
            key: "dummy".to_string(),
            sorted_table_file: "dummy".to_string(),
        };

        let _ = reader.index.iter().map(|idx| -> IoResult<()> {
            let sst = SortedStoreTable::new_load_from_disk(&idx.sorted_table_file)?;
            let curr_size = sst.get_data_size();
            if curr_size > data_size {
                data_size = curr_size;
                entry = idx.clone();
            }
            Ok(())
        });

        Ok(entry)
    }

    pub fn trigger_compaction(
        &self,
        hasher: SipHasher24,
        children: Vec<Arc<Bucket>>,
        sender: Sender<types::BucketMergeResult>,
    ) -> IoResult<()> {
        let entry = self.get_compaction_file_entry()?;
        let input_buffer = self.form_input_buffers(hasher, Some(entry))?;

        for (bucket, buff) in zip(children, input_buffer.buffers) {
            let pass = sender.clone();

            rayon::spawn(move || {
                compaction_perform_merge(bucket, buff, pass)
                    .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))
                    .ok();
            })
        }

        Ok(())
    }

    pub fn perform_merge(&self, input_buffer: types::CompactionBuffer) -> IoResult<()> {
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
        let mut path = format!("/home/sarthak17/Desktop/SomeSSTfile_{}", path_cnt);
        // if f {
        //     path = format!("/home/sarthak17/Desktop/SomeSSTfile_1");
        // }

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

pub fn compaction_perform_merge(
    bucket: Arc<Bucket>,
    input_buffer: types::CompactionBuffer,
    sender: Sender<types::BucketMergeResult>,
) -> IoResult<()> {
    let res = bucket.perform_merge(input_buffer);
    let send_result = types::BucketMergeResult::new(bucket, res);
    sender
        .send(send_result)
        .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
    Ok(())
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
            } else if one[idx1].header.timestamp > two[idx2].header.timestamp {
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
        let reader = bucket.index.read().expect("failed to get read lock");

        let fl = &reader.index[9];
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

        let bucket = Bucket::new(path, 0, 0).expect("failed");

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

        println!("vals are - {:?} {:?}", one, two);

        let timestamp4 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("operation failed")
            .as_nanos();
        let timestamp5 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("operation failed")
            .as_nanos();
        let timestamp6 = SystemTime::now()
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
        let item5 = KeyValOffset {
            key: "item4".to_string(),
            header: LogHeader {
                page_id: 0,
                offset: 4591,
                size: 800,
                timestamp: timestamp5,
            },
            rec_type: RecordType::AppendOrUpdate,
        };
        let item6 = KeyValOffset {
            key: "item5".to_string(),
            header: LogHeader {
                page_id: 0,
                offset: 4591,
                size: 800,
                timestamp: timestamp6,
            },
            rec_type: RecordType::AppendOrUpdate,
        };

        let mut new_buffer = CompactionBuffer::new();
        new_buffer.insert(item4);
        new_buffer.insert(item5);
        new_buffer.insert(item6);

        bucket.perform_merge(new_buffer).expect("failed to compact");
        let val1 = bucket.get("item4".to_string()).expect("failed to get");
        let val2 = bucket.get("item5".to_string()).expect("failed to get");

        println!("vals are - {:?} {:?}", val1, val2);

        let _val3 = bucket.get("item3".to_string()).expect("failed to get");

        fs::remove_file(path).unwrap();
    }
}
