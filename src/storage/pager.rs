use crate::constants::PAGE_SIZE;
use crate::storage::{engine::IoResult, storage_interface::Page};
use bincode;
use serde::Serialize;
use std::cmp::min;
use std::{
    cmp::max,
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    sync::{Arc, RwLock},
};

pub type AppendInfo = (u64, usize, usize);

#[derive(Debug)]
pub struct Pager<P: Page> {
    file: File,
    pub pages: Vec<P>,
    current_offset: usize,
}

impl<P: Page> Pager<P> {
    pub fn new(path_str: &str) -> IoResult<Self> {
        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(&path_str)?;

        Ok(Self {
            file,
            pages: Vec::new(),
            current_offset: 0,
        })
    }

    pub fn clear(&mut self) {
        self.pages.clear();
        self.current_offset = 0;
    }

    pub fn set_page(&mut self, offset: usize) -> IoResult<()> {
        let len = max(0, offset - 16);
        let page_id = len / PAGE_SIZE;

        let mut buff = [0 as u8; PAGE_SIZE];

        if len > (page_id * PAGE_SIZE) {
            let mut reader = &self.file;
            let read_offset = page_id * PAGE_SIZE;
            // let mut handle = self.file.take(PAGE_SIZE as u64);
            // handle.read(&mut buff)?;
            reader.seek(SeekFrom::Start(read_offset as u64))?;
            reader.read(&mut buff)?;

            let page = P::new(page_id as u64, buff);
            self.pages.push(page);
        } else {
            let page = P::new(page_id as u64, buff);
            self.pages.push(page);
        }

        self.current_offset = len % PAGE_SIZE;

        Ok(())
    }

    pub fn contigious_append_to_page<T: Serialize>(&mut self, val: &T) -> IoResult<AppendInfo> {
        let bytes = bincode::serialize(val)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let size = bytes.as_slice().len();
        let current_offset = self.current_offset;
        let new_offset = current_offset + size;

        let current_page = self.pages.last_mut().ok_or(io::Error::new(
            io::ErrorKind::Other,
            "Page not found".to_string(),
        ))?;

        if new_offset >= PAGE_SIZE {
            let remaining_size = (PAGE_SIZE - current_offset) as isize;
            let mut new_page_size = size as isize - remaining_size;
            let mut new_page_offset = 0 as usize;

            let current_bytes = current_page.get_page_bytes();
            current_bytes[current_offset..PAGE_SIZE]
                .copy_from_slice(&bytes[new_page_offset..remaining_size as usize]);

            new_page_offset += remaining_size as usize;

            while new_page_size > 0 {
                let max_offset = min(PAGE_SIZE, new_page_size as usize);

                let buff = [0 as u8; PAGE_SIZE];
                let current_page = self.pages.last().ok_or(io::Error::new(
                    io::ErrorKind::Other,
                    "Page not found".to_string(),
                ))?;
                let mut page = P::new(current_page.get_id() + 1, buff);
                let current_bytes = page.get_page_bytes();

                current_bytes[0..max_offset].copy_from_slice(
                    &bytes[new_page_offset as usize..(new_page_offset + max_offset)],
                );

                new_page_offset += max_offset;
                new_page_size -= max_offset as isize;

                self.pages.push(page);
            }

            self.current_offset = 0;
        } else {
            let current_bytes = current_page.get_page_bytes();
            current_bytes[current_offset..new_offset as usize].copy_from_slice(bytes.as_slice());
        }

        let current_page_last = self.pages.last().ok_or(io::Error::new(
            io::ErrorKind::Other,
            "Page not found".to_string(),
        ))?;

        let page_id = current_page_last.get_id();

        let info = (page_id, self.current_offset, size);

        self.current_offset += size % PAGE_SIZE;

        Ok(info)
    }

    pub fn append_to_page<T: Serialize>(&mut self, val: T) -> IoResult<AppendInfo>
    where
        for<'a> T: serde::de::Deserialize<'a>,
    {
        let bytes = bincode::serialize(&val)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let size = bytes.as_slice().len();
        let current_offset = self.current_offset;
        let new_offset = current_offset + size;

        let current_page = self.pages.last_mut().ok_or(io::Error::new(
            io::ErrorKind::Other,
            "Page not found".to_string(),
        ))?;

        if new_offset < PAGE_SIZE {
            let curr_bytes = current_page.get_page_bytes();
            let mut curr_vec = deserialize_into_vectored_type::<T>(curr_bytes, current_offset)?;
            curr_vec.push(val);
            let vec_bytes = bincode::serialize(&curr_vec)
                .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
            curr_bytes[0..new_offset].copy_from_slice(vec_bytes.as_slice());
        } else {
            let buff = [0 as u8; PAGE_SIZE];
            let mut page = P::new(current_page.get_id() + 1, buff);
            let current_bytes = page.get_page_bytes();
            current_bytes.copy_from_slice(bytes.as_slice());

            self.pages.push(page);
        }

        let current_page_last = self.pages.last().ok_or(io::Error::new(
            io::ErrorKind::Other,
            "Page not found".to_string(),
        ))?;

        let page_id = current_page_last.get_id();

        let info = (page_id, self.current_offset, size);

        self.current_offset += size % PAGE_SIZE;

        Ok(info)
    }

    pub fn flush_pages(&mut self, store_offset: usize) -> IoResult<()> {
        let mut bytes = Vec::new();

        for page in &mut self.pages {
            let ref_page = page;

            let n = ref_page.get_page_bytes();

            bytes.extend_from_slice(n);
        }

        let mut file = &self.file;
        file.seek(SeekFrom::Start(store_offset as u64))?;
        file.write_all(bytes.as_slice())?;
        file.sync_all()?;

        self.pages.clear();
        self.current_offset = 0;

        Ok(())
    }

    pub fn read_pages<T>(&mut self, page_id: u64, offset: usize, size: usize) -> IoResult<Vec<T>>
    where
        for<'a> T: serde::de::Deserialize<'a>,
    {
        let mut page_id = page_id;
        let fixed_offset = page_id * PAGE_SIZE as u64 + offset as u64;
        let last_page = (fixed_offset as isize + size as isize) / PAGE_SIZE as isize;

        let mut buff = Vec::new();

        for _ in 0..last_page + 1 {
            let buf = [0 as u8; PAGE_SIZE];
            buff.push(buf);
        }

        let mut concat_buff = buff.concat();

        let mut reader = &self.file;
        reader.seek(SeekFrom::Start(fixed_offset))?;
        reader.read(concat_buff.as_mut_slice())?;

        let res = deserialize_into_vectored_type(&concat_buff, size)?;

        let iter = concat_buff.chunks(PAGE_SIZE);

        for buf in iter {
            let mut new_buff = [0 as u8; PAGE_SIZE];
            new_buff.copy_from_slice(buf);

            let page = P::new(page_id as u64, new_buff);

            page_id += 1;

            self.pages.push(page);
        }

        Ok(res)
    }

    pub fn flush_arbitrary(&self, offset: usize, data: &[u8]) -> IoResult<()> {
        let mut writer = &self.file;
        writer.seek(SeekFrom::Start(offset as u64))?;
        writer.write_all(data)?;
        writer.sync_all()
    }

    pub fn read_arbitrary_from_offset(&self, offset: usize, buffer: &mut [u8]) -> IoResult<()> {
        let mut reader = &self.file;
        reader.seek(SeekFrom::Start(offset as u64))?;
        let res = reader.read(buffer)?;
        Ok(())
    }
}

fn deserialize_into_type<'de, T: serde::Deserialize<'de>>(
    concat_buff: &'de [u8],
    size: usize,
) -> IoResult<Vec<T>> {
    let final_buff = &concat_buff[0..size];
    let len = 56 as usize;
    let mut curr = 0;
    let mut res = Vec::new();

    while curr + len < final_buff.len() {
        let value = bincode::deserialize::<T>(&final_buff[curr..len])
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
        curr += len;
        res.push(value);
    }

    Ok(res)
}

fn deserialize_into_vectored_type<'de, T: serde::Deserialize<'de>>(
    concat_buff: &'de [u8],
    size: usize,
) -> IoResult<Vec<T>> {
    let final_buff = &concat_buff[0..size];
    let value = bincode::deserialize::<Vec<T>>(&final_buff)
        .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

    Ok(value)
}

#[cfg(test)]
mod test {
    use crate::storage::{
        pager::Pager,
        storage_types::{LogPage, RecordType, ValueLog, ValueType},
    };
    use std::{fs, iter::zip};

    #[test]
    fn test_all() {
        let path = "/home/sarthak17/Desktop/disk_test";
        let _ = fs::File::create(path).unwrap();
        let manager = Pager::<LogPage>::new(path).expect("failed");
        //let data = b"ay yo sup";

        let key = "test_key".to_string();
        let bytes = [1, 2, 3];
        let mut value = Vec::new();
        value.extend_from_slice(&bytes);

        let value = ValueType::NumberArray(value);

        let log = ValueLog::new(key.clone(), value.clone(), RecordType::AppendOrUpdate);
        let log1 = ValueLog::new(key.clone(), value.clone(), RecordType::AppendOrUpdate);
        let mut log_vec = Vec::new();
        log_vec.push(log);
        log_vec.push(log1);

        let bytes = bincode::serialize(&log_vec).unwrap();

        manager
            .flush_arbitrary(0, bytes.as_slice())
            .expect("failed");

        // manager.flush_arbitrary(0, data).unwrap();
        let mut buffer = [0 as u8; 120];

        manager.read_arbitrary_from_offset(0, &mut buffer).unwrap();

        let meta = bincode::deserialize::<Vec<ValueLog>>(&buffer).unwrap();

        for (l, r) in zip(log_vec, meta) {
            assert_eq!(l, r);
        }

        fs::remove_file(path).unwrap();
    }
}
