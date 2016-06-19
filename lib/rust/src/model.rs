use std::fmt;
use std::collections::BTreeMap;
use serde_json::Value;
use std::str::FromStr;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Query {
    pub id: Option<String>,
    pub query: String,
    pub trace_id: Option<String>,
    pub auth_token: Option<String>,
    pub config: Value,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SonicMessage {
    pub e: String,
    pub v: Option<String>,
    pub p: Option<Value>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientConfig {
    pub sonicd: String,
    pub http_port: u16,
    pub tcp_port: u16,
    pub sources: BTreeMap<String, Value>,
}

impl ClientConfig {
    pub fn empty() -> ClientConfig {
        ClientConfig {
            sonicd: "0.0.0.0".to_string(),
            http_port: 9111,
            tcp_port: 10001,
            sources: BTreeMap::new(),
        }
    }
}

/// This type represents all possible errors that can occur 
/// when interacting with a sonicd server
#[derive(Debug)]
pub enum Error {
    Io(::nix::Error),
    Connect(::std::io::Error),
    SerDe(String),
    GetAddr(::std::io::Error),
    ParseAddr(::std::net::AddrParseError),
    ProtocolError(String),
    HttpError(::curl::Error),
    StreamError(String),
    OtherError(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &Error::Io(ref err) => write!(f, "{}", err),
            &Error::Connect(ref err) => write!(f, "{}", err),
            &Error::SerDe(ref err) => write!(f, "{}", err),
            &Error::GetAddr(ref err) => write!(f, "{}", err),
            &Error::ParseAddr(ref err) => write!(f, "{}", err),
            &Error::ProtocolError(ref err) => write!(f, "{}", err),
            &Error::HttpError(ref err) => write!(f, "{}", err),
            &Error::StreamError(ref rec) => write!(f, "{}", rec), 
            &Error::OtherError(ref rec) => write!(f, "{}", rec), 
        }
    }
}

/// Helper alias for `Result` objects that return `Error`.
pub type Result<T> = ::std::result::Result<T, Error>;

impl Query {
    pub fn from_msg(msg: SonicMessage) -> Result<Query> {
        match (msg.e.as_ref(), &msg.v, &msg.p) {
            ("Q", &Some(ref query), &Some(Value::Object(ref payload))) => {

                let trace_id = payload.get("trace_id").and_then(|t| t.as_string().map(|t| t.to_owned()));
                let auth_token = payload.get("auth").and_then(|a| a.as_string().map(|a| a.to_owned()));
                let config = try!(payload.get("config")
                                  .map(|c| c.to_owned())
                                  .ok_or_else(|| Error::ProtocolError("missing 'config' in query message payload".to_owned())));

                Ok(Query {
                    id: None,
                    trace_id: trace_id,
                    query: query.to_owned(),
                    auth_token: auth_token,
                    config: config,
                })
            }
            _ => {
                Err(Error::SerDe(format!("message cannot be deserialized into a query: {:?}",
                                         &msg)))
            }
        }
    }

    pub fn into_msg(self) -> SonicMessage {
        let mut payload = BTreeMap::new();

        payload.insert("config".to_owned(), self.config.clone());
        payload.insert("auth".to_owned(), 
                       self.auth_token.clone().map(|s| Value::String(s)).unwrap_or_else(|| Value::Null));
        payload.insert("trace_id".to_owned(), 
                       self.trace_id.clone().map(|s| Value::String(s)).unwrap_or_else(|| Value::Null));

        SonicMessage {
            e: "Q".to_owned(),
            v: Some(self.query.clone()),
            p: Some(Value::Object(payload)),
        }
    }

    pub fn into_json(self) -> Value {
        ::serde_json::to_value::<SonicMessage>(&self.into_msg())
    }
}

impl SonicMessage {

    pub fn into_json(self) -> Value {
        ::serde_json::to_value::<SonicMessage>(&self)
    }

    pub fn ack() -> SonicMessage {
        SonicMessage {
            e: "A".to_owned(),
            v: None,
            p: None,
        }
    }

    pub fn into_bytes(self) -> Vec<u8> {
        ::serde_json::to_string(&self).unwrap().into_bytes()
    }

    pub fn from_slice(slice: &[u8]) -> Result<SonicMessage> {
        ::serde_json::from_slice::<SonicMessage>(slice).map_err(|e| {
            let json_str = ::std::str::from_utf8(slice);
            Error::SerDe(format!("error unmarshalling SonicMessage '{:?}': {}", json_str, e))
        })
    }

    // DoneWithQueryExecution error
    pub fn done<T>(e: Result<T>) -> SonicMessage {
        let (s, e) = if e.is_ok() {
            ("success".to_owned(), Value::Null)
        } else {
            ("error".to_owned(),
            Value::Array(vec![Value::String(format!("{}", e.err().unwrap()))]))
        };
        SonicMessage {
            e: "D".to_owned(),
            v: Some(s),
            p: Some(e),
        }
    }

    pub fn from_bytes(buf: Vec<u8>) -> Result<SonicMessage> {
        Self::from_slice(buf.as_slice())
    }

    fn payload_into_errors(payload: Option<Value>) -> Vec<String> {
        payload.map(|payload| {
            match payload {
                Value::Array(data) => {
                    data.iter()
                        .map(|s| String::from_str(s.as_string().unwrap()).unwrap())
                        .collect::<Vec<String>>()
                }
                e => panic!("expecting JSON array got: {:?}", e),
            }
        })
        .unwrap_or_else(|| Vec::new())
    }
}
