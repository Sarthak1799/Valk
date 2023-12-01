use super::engine::IoResult;
use bincode;
use fastrand as random;
use serde::Serialize;
use siphasher::sip::SipHasher;
use std::io;
use std::marker::PhantomData;

#[derive(Debug)]
pub struct BloomFilter<T: ?Sized> {
    bitmap: Vec<bool>,
    /// Size of the bit array.
    arr_size: usize,
    /// Number of hash functions.
    hash_num: u32,
    /// Two hash functions from which hash_num number of hashes are derived.
    hashers: [SipHasher; 2],
    /// keys for hashers
    keys: [u64; 4],
    _marker: PhantomData<T>,
}

impl<T: ?Sized> BloomFilter<T> {
    fn bitmap_size(items_count: usize, fp_rate: f64) -> usize {
        let ln2_2 = core::f64::consts::LN_2 * core::f64::consts::LN_2;
        ((-1.0f64 * items_count as f64 * fp_rate.ln()) / ln2_2).ceil() as usize
    }

    pub fn new(items_count: usize, fp_rate: f64) -> Self {
        let arr_size = Self::bitmap_size(items_count, fp_rate);
        let mut bitmap = Vec::with_capacity(arr_size);
        unsafe { bitmap.set_len(arr_size) };

        let hash_num = Self::optimal_num_of_hashes(fp_rate);
        let key1 = random::u64(..1000000000);
        let key2 = random::u64(..1000000000);
        let key3 = random::u64(..1000000000);
        let key4 = random::u64(..1000000000);

        let hasher1 = SipHasher::new_with_keys(key1, key2);
        let hasher2 = SipHasher::new_with_keys(key3, key4);

        let hashers = [hasher1, hasher2];

        BloomFilter {
            bitmap,
            arr_size,
            hash_num,
            hashers,
            keys: [key1, key2, key3, key4],
            _marker: PhantomData,
        }
    }

    pub fn new_with_preload(fp_rate: f64, bitmap: Vec<bool>, keys: [u64; 4]) -> Self {
        let arr_size = bitmap.len();

        let hash_num = Self::optimal_num_of_hashes(fp_rate);
        let key1 = keys[0];
        let key2 = keys[1];
        let key3 = keys[2];
        let key4 = keys[3];

        let hasher1 = SipHasher::new_with_keys(key1, key2);
        let hasher2 = SipHasher::new_with_keys(key3, key4);

        let hashers = [hasher1, hasher2];

        BloomFilter {
            bitmap,
            arr_size,
            hash_num,
            hashers,
            keys,
            _marker: PhantomData,
        }
    }

    fn optimal_num_of_hashes(fp_rate: f64) -> u32 {
        ((-1.0f64 * fp_rate.ln()) / core::f64::consts::LN_2).ceil() as u32
    }

    /// Calculate two hash values from which the k hashes are derived.
    fn hash_core(&self, item: &T) -> IoResult<(u64, u64)>
    where
        T: Serialize,
    {
        let hasher1 = &self.hashers[0];
        let hasher2 = &self.hashers[1];

        let enc = bincode::serialize(item)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let hash1 = hasher1.hash(enc.as_slice());
        let hash2 = hasher2.hash(enc.as_slice());

        Ok((hash1, hash2))
    }

    fn get_index(&self, h1: u64, h2: u64, k_i: u64) -> usize {
        h1.wrapping_add((k_i).wrapping_mul(h2)) as usize % self.arr_size
    }

    pub fn set(&mut self, item: &T) -> IoResult<()>
    where
        T: Serialize,
    {
        let (h1, h2) = self.hash_core(item)?;

        for k_i in 0..self.hash_num {
            let index = self.get_index(h1, h2, k_i as u64);

            self.bitmap[index] = true;
        }

        Ok(())
    }

    pub fn get(&mut self, item: &T) -> IoResult<bool>
    where
        T: Serialize,
    {
        let (h1, h2) = self.hash_core(item)?;

        for k_i in 0..self.hash_num {
            let index = self.get_index(h1, h2, k_i as u64);

            let entry = self.bitmap.get(index).ok_or(io::Error::new(
                io::ErrorKind::Other,
                "bitmap get failed".to_string(),
            ))?;

            if !entry {
                return Ok(false);
            }
        }

        Ok(true)
    }

    pub fn get_keys(&self) -> [u64; 4] {
        self.keys
    }

    pub fn get_bitmap(&self) -> Vec<bool> {
        self.bitmap.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bitmap_size(items_count: usize, fp_rate: f64) -> usize {
        let ln2_2 = core::f64::consts::LN_2 * core::f64::consts::LN_2;
        ((-1.0f64 * items_count as f64 * fp_rate.ln()) / ln2_2).ceil() as usize
    }

    #[test]
    fn insert() {
        let mut bloom = BloomFilter::new(100, 0.01);
        let item = "item".to_string();
        bloom.set(item.as_str()).expect("failed to set");

        let res = bloom.get(item.as_str()).expect("failed");
        assert!(res);
    }

    #[test]
    fn check_and_insert() {
        let arr_size = bitmap_size(100, 0.01);
        let mut bitmap = Vec::with_capacity(arr_size);
        unsafe { bitmap.set_len(arr_size) };

        let key1 = random::u64(..1000000000);
        let key2 = random::u64(..1000000000);
        let key3 = random::u64(..1000000000);
        let key4 = random::u64(..1000000000);

        let keys = [key1, key2, key3, key4];

        let mut bloom = BloomFilter::new_with_preload(0.01, bitmap, keys);

        let res1 = bloom.get("item_1").expect("failed");
        let res2 = bloom.get("item_2").expect("failed");
        assert!(!res1);
        assert!(!res2);

        bloom.set("item_1").expect("failed");

        let res3 = bloom.get("item_1").expect("failed");
        assert!(res3);
    }
}
