use serde_json::Value;

#[derive(Debug)]
pub struct Acknowledge;

#[derive(Debug)]
pub struct TypeMetadata(pub Vec<(String, Value)>);

#[derive(Debug)]
pub enum QueryStatus {
    Queued,
    Started,
    Running,
    Waiting,
    Finished,
}

#[derive(Debug)]
pub struct QueryProgress {
    pub status: QueryStatus,
    pub progress: f64,
    pub total: Option<f64>,
    pub units: Option<String>,
}

#[derive(Debug)]
pub struct OutputChunk(pub Vec<Value>);

/// Signals when a stream is done. Inner option
/// is an error if there was one
#[derive(Debug)]
pub struct Done(pub Option<String>);


/// Marker trait for messages sent from client to server
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

pub mod protocol {
    #[cfg(feature = "serde_macros")]
    include!("protocol.rs.in");

    #[cfg(not(feature = "serde_macros"))]
    include!(concat!(env!("OUT_DIR"), "/protocol.rs"));
}
