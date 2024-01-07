use std::{
    io,
    sync::{mpsc, Arc},
};
pub type IoResult<T> = Result<T, io::Error>;
pub type GenericFindResult<T> = Result<T, types::GenericFindErrorKind>;
pub use super::storage_types::ValueType;
use crate::{constants::LOG_MANAGER_BUFFER_SIZE, global_types};

use super::{
    log_manager::VLogController,
    pager::Pager,
    sorted_store::{BucketGlobalStateManager, BucketsController},
    storage_types as types,
};
use bincode;

#[derive(Debug)]
pub struct Engine {
    pub engine_pager: Pager<types::IndexPage>,
    pub metadata: types::EngineMetadata,
    pub config: types::EngineConfig,
    pub log_controller: Arc<VLogController>,
    pub bucket_controller: Arc<BucketsController>,
}

impl Engine {
    pub fn init() -> IoResult<()> {
        let path = "storage/engine.db";
        let log_path = "storage/log_manager.db";
        let engine_pager = Pager::<types::IndexPage>::new(path)?;
        let metadata = types::EngineMetadata::new();

        let bytes = bincode::serialize(&metadata)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
        engine_pager.flush_arbitrary(0, bytes.as_slice())?;

        VLogController::init(log_path)?;
        BucketGlobalStateManager::init()?;

        Ok(())
    }

    pub fn new() -> IoResult<Self> {
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
        let log_controller = Arc::new(VLogController::new_load_from_disk(log_path)?);

        Ok(Self {
            engine_pager,
            metadata,
            config,
            log_controller,
            bucket_controller,
        })
    }

    pub fn process_requests(
        &self,
        req: Vec<global_types::Request>,
        global_sender: mpsc::Sender<global_types::Response>,
    ) {
        req.into_iter().for_each(|req| {
            let req_id = req.id;

            let _res = match req.req_type {
                global_types::RequestType::Get(get_req) => {
                    self.get(get_req.key, req_id, global_sender.clone())
                }
                global_types::RequestType::Set(set_req) => {
                    self.set(set_req.key, set_req.val, req_id, global_sender.clone())
                }
            };
        })
    }

    pub fn get(
        &self,
        key: String,
        req_id: u32,
        global_sender: mpsc::Sender<global_types::Response>,
    ) -> IoResult<()> {
        let log_controller = self.log_controller.clone();
        let bucket_controller = self.bucket_controller.clone();
        let pass = global_sender;
        let config_clone = self.config.clone();

        rayon::spawn(move || {
            let exists_in_mem = log_controller.get(key.clone());

            let response = match exists_in_mem {
                Ok(val) => Ok(global_types::ResponseType::SuccessfulGet(val.value.clone())),
                Err(_e) => {
                    let res = bucket_controller
                        .get(key, config_clone)
                        .map_err(|e| global_types::GlobalError::Other(e.to_string()))
                        .and_then(|kval| {
                            log_controller
                                .get_val_from_log::<types::ValueLog>(kval)
                                .map_err(|e| global_types::GlobalError::Other(e.to_string()))
                                .and_then(|val| {
                                    Ok(global_types::ResponseType::SuccessfulGet(
                                        val[0].value.clone(),
                                    ))
                                })
                        });
                    res
                }
            };

            let res = global_types::Response::new(req_id, response);
            pass.send(res).unwrap();
        });

        Ok(())
    }

    pub fn set(
        &self,
        key: String,
        val: types::ValueType,
        req_id: u32,
        global_sender: mpsc::Sender<global_types::Response>,
    ) -> IoResult<()> {
        let log_controller = self.log_controller.clone();
        let bucket_controller = self.bucket_controller.clone();
        let pass = global_sender;
        let config_clone = self.config.clone();

        rayon::spawn(move || {
            let log = types::ValueLog::new(key, val, types::RecordType::AppendOrUpdate);
            let append_res = log_controller.append_log(log);

            let response = append_res
                .map_err(|e| global_types::GlobalError::Other(e.to_string()))
                .and_then(|_| {
                    log_controller
                        .get_current_size()
                        .map_err(|e| global_types::GlobalError::Other(e.to_string()))
                        .and_then(|sz| {
                            if sz as usize >= LOG_MANAGER_BUFFER_SIZE {
                                log_controller
                                    .get_compaction_inputs(config_clone.hasher)
                                    .map_err(|e| global_types::GlobalError::Other(e.to_string()))
                                    .and_then(|inputs| {
                                        let pass = bucket_controller.clone();
                                        rayon::spawn(move || {
                                            let _res =
                                                pass.trigger_compaction(inputs, config_clone);
                                        });
                                        Ok(global_types::ResponseType::SuccessfulSet)
                                    })
                            } else {
                                Ok(global_types::ResponseType::SuccessfulSet)
                            }
                        })
                });

            let res = global_types::Response::new(req_id, response);
            pass.send(res).unwrap();
        });

        Ok(())
    }
}
