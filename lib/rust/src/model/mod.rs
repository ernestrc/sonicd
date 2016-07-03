use serde_json::Value;

pub mod protocol;

#[derive(Debug)]
pub struct Acknowledge;

#[derive(Debug)]
pub struct TypeMetadata(pub Vec<(String, Value)>);

pub struct QueryProgress {
    pub progress: f64,
    pub total: Option<f64>,
    pub unit: Option<String>,
}

#[derive(Debug)]
pub struct Log(pub String);

#[derive(Debug)]
pub struct OutputChunk(pub Vec<Value>);

/// Signals when a stream is done. Inner option
/// is an error if there was one
#[derive(Debug)]
pub struct Done(pub Option<String>);


/// Marker trait for messages that client can send to server
pub trait Command {}

#[derive(Debug)]
pub struct Query {
    pub id: Option<String>,
    pub query: String,
    pub trace_id: Option<String>,
    pub auth: Option<String>,
    pub config: Value,
}

impl Command for Query {}

#[derive(Debug)]
pub struct Authenticate {
    pub user: String,
    pub key: String,
    pub trace_id: Option<String>,
}

impl Command for Authenticate {}
