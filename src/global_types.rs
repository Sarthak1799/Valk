use crate::storage::engine;

pub type ResponseResult = Result<ResponseType, GlobalError>;
#[derive(Debug, Clone)]
pub struct Request {
    pub id: u32,
    pub req_type: RequestType,
}

#[derive(Debug, Clone)]
pub enum RequestType {
    Get(GetRequest),
    Set(SetRequest),
    Delete(String),
}

#[derive(Debug, Clone)]
pub struct GetRequest {
    pub key: String,
}

#[derive(Debug, Clone)]
pub struct SetRequest {
    pub key: String,
    pub val: engine::ValueType,
}

#[derive(Debug, Clone)]
pub enum ResponseType {
    SuccessfulGet(engine::ValueType),
    SuccessfulSet,
}

#[derive(Debug, Clone)]
pub struct Response {
    pub id: u32,
    pub result: ResponseResult,
}

impl Response {
    pub fn new(id: u32, result: ResponseResult) -> Self {
        Self { id, result }
    }
}

#[derive(Debug, Clone)]
pub enum GlobalError {
    IoError(String),
    Other(String),
}
