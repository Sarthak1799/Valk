pub const PAGE_SIZE: usize = 4096;
pub const SORTED_STORE_FILTER_SIZE: usize = 64;
pub const BUCKET_CHILDREN: usize = 16;
#[cfg(feature = "skiplist")]
pub const SKIPLIST_MAX_LEVELS: i32 = 5;
