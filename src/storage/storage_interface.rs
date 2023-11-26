// All the interfaces used for storage purposes
use super::engine::IoResult;
use crate::constants::PAGE_SIZE;

pub trait Storage<K, V> {
    fn store(&mut self, key: Option<K>, value: V) -> IoResult<()>;

    fn retrieve(&self, key: Option<K>) -> IoResult<Option<&V>>;
}

pub trait Page {
    fn new(id: u64, buffer: [u8; PAGE_SIZE]) -> Self;

    fn get_id(&self) -> u64;

    fn get_page_bytes(&mut self) -> &mut [u8; PAGE_SIZE];
}

//pub trait Header {}
