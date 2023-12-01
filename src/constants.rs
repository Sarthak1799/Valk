pub const PAGE_SIZE: usize = 4096;
pub const SORTED_STORE_FILTER_SIZE: usize = 64;
pub const BUCKET_CHILDREN: usize = 16;
pub const SORTED_TABLE_FILTER_ITEM_COUNT: usize = 100;
pub const FILTER_FP_RATE: f64 = 0.01;
pub const DUMMY_SORTED_TABLE_FILE_SIZE: usize = 1024 * 1024;
#[cfg(feature = "skiplist")]
pub const SKIPLIST_MAX_LEVELS: i32 = 5;
