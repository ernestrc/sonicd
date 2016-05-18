use std::fmt;
use std::collections::BTreeMap;
use serde_json::Value;
use std::str::FromStr;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Query {
    pub query_id: Option<String>,
    pub query: String,
    pub config: Value,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SonicMessage {
    pub event_type: String,
    pub variation: Option<String>,
    pub payload: Option<Value>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Receipt {
    pub success: bool,
    pub errors: Vec<String>,
    pub message: Option<String>,
    pub request_id: Option<String>,
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
    HttpError(::curl::ErrCode),
    StreamError(Receipt),
    OtherError(String),
}

impl fmt::Display for Receipt {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {

        if let Some(msg) = self.message.clone() {
            write!(f, "{}\n", msg).unwrap();
        }

        for e in self.errors.iter() {
            write!(f, "Error: {}\n", e).unwrap();
        }
        Ok(())
    }
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
        match (msg.event_type.as_ref(), &msg.variation, &msg.payload) {
            ("Q", &Some(ref query), &Some(ref config)) => {
                Ok(Query {
                    query_id: None,
                    query: query.to_owned(),
                    config: config.to_owned(),
                })
            }
            _ => {
                Err(Error::SerDe(format!("message cannot be deserialized into a query: {:?}",
                                         &msg)))
            }
        }
    }

    pub fn into_msg(&self) -> SonicMessage {
        SonicMessage {
            event_type: "Q".to_owned(),
            variation: Some(self.query.clone()),
            payload: Some(self.config.clone()),
        }
    }
}

impl SonicMessage {
    pub fn ack() -> SonicMessage {
        SonicMessage {
            event_type: "A".to_owned(),
            variation: None,
            payload: None,
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
            event_type: "D".to_owned(),
            variation: Some(s),
            payload: Some(e),
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



    pub fn into_rec(self) -> Receipt {
        Receipt {
            success: self.variation.unwrap() == "success",
            errors: SonicMessage::payload_into_errors(self.payload),
            message: None,
            request_id: None,
        }
    }
}

impl Receipt {
    pub fn success() -> Receipt {
        Receipt {
            success: true,
            errors: vec![],
            message: None,
            request_id: None,
        }
    }

    pub fn error(e: Error) -> Receipt {
        Receipt {
            success: false,
            errors: vec![format!("{}", e)],
            message: None,
            request_id: None,
        }
    }

    pub fn reduce(v: Vec<Receipt>) -> Receipt {
        v.iter().fold(Receipt::success(), move |mut acc, rec| {
            acc.errors.extend(rec.errors.iter().cloned());
            let msg = match (acc.message.clone(), rec.message.clone()) {
                (Some(a), Some(b)) => Some(a + "\n" + &b),
                (None, Some(b)) => Some(b),
                (Some(a), None) => Some(a),
                (None, None) => None,
            };
            Receipt {
                success: acc.success && rec.success,
                message: msg,
                errors: acc.errors.clone(),
                request_id: rec.request_id.clone().or_else(|| acc.request_id.clone()),
            }
        })
    }
}
