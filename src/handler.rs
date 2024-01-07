use super::global_types;
use crate::storage::engine::{Engine, IoResult};
use once_cell::sync::Lazy;
use std::sync::mpsc;

static ENGINE: Lazy<Engine> = Lazy::new(|| {
    let engine = Engine::new().expect("Failed to initialize engine");
    engine
});

pub fn init() -> IoResult<()> {
    Engine::init()
}

pub fn process_requests(
    req: Vec<global_types::RequestType>,
) -> IoResult<Vec<global_types::Response>> {
    let mut cnt = 0;
    let batch_req = req
        .into_iter()
        .map(|req| {
            let r = global_types::Request {
                id: cnt,
                req_type: req,
            };
            cnt += 1;
            r
        })
        .collect::<Vec<_>>();

    let (sender, reciever) = mpsc::channel();
    let mut response_vec = Vec::new();

    ENGINE.process_requests(batch_req, sender);

    while let Ok(res) = reciever.recv() {
        response_vec.push(res);
        if response_vec.len() as u32 == cnt - 1 {
            response_vec.sort_by_key(|res| res.id);
            return Ok(response_vec);
        }
    }

    Ok(response_vec)
}
