use serde_json::Value;
use error::Result;

#[derive(Debug)]
pub enum QueryStatus {
    Queued,
    Started,
    Running,
    Waiting,
    Finished,
}

#[derive(Debug)]
pub enum SonicMessage {
    // client ~> server
    Acknowledge,

    Query {
        id: Option<String>,
        query: String,
        trace_id: Option<String>,
        auth: Option<String>,
        config: Value,
    },

    Authenticate {
        user: String,
        key: String,
        trace_id: Option<String>,
    },

    // client <~ server
    TypeMetadata(Vec<(String, Value)>),

    QueryProgress {
        status: QueryStatus,
        progress: f64,
        total: Option<f64>,
        units: Option<String>,
    },

    OutputChunk(Vec<Value>),

    Done(Option<String>),
}

impl SonicMessage {
    // DoneWithQueryExecution error
    pub fn done<T>(e: Result<T>) -> SonicMessage {
        let variation = match e {
            Ok(_) => None,
            Err(e) => Some(format!("{}", e).to_owned()),
        };
        SonicMessage::Done(variation).into()
    }

    pub fn into_json(self) -> Value {
        let msg: protocol::ProtoSonicMessage = From::from(self);

        ::serde_json::to_value(&msg)
    }

    pub fn from_slice(slice: &[u8]) -> Result<SonicMessage> {
        let msg = try!(::serde_json::from_slice::<protocol::ProtoSonicMessage>(slice));
        msg.into_msg()
    }

    pub fn from_bytes(buf: Vec<u8>) -> Result<SonicMessage> {
        Self::from_slice(buf.as_slice())
    }

    pub fn into_bytes(self) -> Result<Vec<u8>> {
        let s = try!(::serde_json::to_string(&self.into_json()));
        Ok(s.into_bytes())
    }
}

pub mod protocol {
    #[cfg(feature = "serde_macros")]
    include!("protocol.rs.in");

    #[cfg(not(feature = "serde_macros"))]
    include!(concat!(env!("OUT_DIR"), "/protocol.rs"));
}
