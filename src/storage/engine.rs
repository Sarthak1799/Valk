use std::{
    io,
    sync::{mpsc, Arc},
};
pub type IoResult<T> = Result<T, io::Error>;
pub type GenericFindResult<T> = Result<T, types::GenericFindErrorKind>;
use crate::constants::LOG_MANAGER_BUFFER_SIZE;

use super::{
    log_manager::VLogController,
    pager::Pager,
    sorted_store::{BucketGlobalStateManager, BucketsController},
    storage_types as types,
};
use bincode;

#[derive(Debug)]
pub struct Engine<'a> {
    pub engine_pager: Pager<types::IndexPage>,
    pub metadata: types::EngineMetadata,
    pub config: types::EngineConfig,
    pub log_controller: VLogController<'a>,
    pub bucket_controller: Arc<BucketsController>,
}

impl<'a> Engine<'a> {
    fn new() -> IoResult<Self> {
        let path = "storage/engine.db";
        let log_path = "storage/log_manager.db";
        let engine_pager = Pager::<types::IndexPage>::new(path)?;

        let mut metadata_buffer = [0 as u8; 24];
        engine_pager.read_arbitrary_from_offset(0 as usize, &mut metadata_buffer)?;
        let metadata = bincode::deserialize::<types::EngineMetadata>(&metadata_buffer)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let mut bucket_vec_buff = Vec::with_capacity(metadata.bucket_files_size as usize);
        unsafe { bucket_vec_buff.set_len(metadata.bucket_files_size as usize) }
        engine_pager.read_arbitrary_from_offset(24 as usize, bucket_vec_buff.as_mut_slice())?;
        let bucket_vec = bincode::deserialize::<Vec<(u128, String)>>(bucket_vec_buff.as_slice())
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        rayon::ThreadPoolBuilder::new()
            .num_threads(16)
            .build_global()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let (sender, reciever) = mpsc::channel();

        let bucket_state_manager = BucketGlobalStateManager::new_load_from_disk(reciever)?;

        rayon::spawn(move || {
            let _res = bucket_state_manager.serve_req();
        });

        let config = types::EngineConfig::new_from_data(bucket_vec, metadata.hash_keys);
        let bucket_controller = Arc::new(BucketsController::new(sender.clone()));
        let log_controller = VLogController::new(log_path)?;

        Ok(Self {
            engine_pager,
            metadata,
            config,
            log_controller,
            bucket_controller,
        })
    }

    pub fn get(&mut self, key: String) -> IoResult<types::ValueType> {
        let exists_in_mem = self.log_controller.get(key.clone());
        match exists_in_mem {
            Ok(val) => Ok(val.value),
            Err(_e) => {
                let kval = self.bucket_controller.get(key, &self.config)?;
                let res = self
                    .log_controller
                    .get_val_from_log::<types::ValueLog>(kval)?;
                Ok(res[0].value.clone())
            }
        }
    }

    pub fn set(&mut self, key: String, val: types::ValueType) -> IoResult<()> {
        let log = types::ValueLog::new(key, val, types::RecordType::AppendOrUpdate);
        self.log_controller.append_log(log)?;

        let curr_size = self.log_controller.get_current_size() as usize;
        if curr_size >= LOG_MANAGER_BUFFER_SIZE {
            let config = self.config.clone();
            let inputs = self.log_controller.get_compaction_inputs(config.hasher)?;

            let pass = self.bucket_controller.clone();

            rayon::spawn(move || {
                let _res = pass.trigger_compaction(inputs, config);
            });
        }

        Ok(())
    }
}
