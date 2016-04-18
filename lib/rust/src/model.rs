use std::fmt;
use std::collections::BTreeMap;
use serde_json::Value;
use std::str::FromStr;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Query {
    pub query_id: Option<String>,
    pub query: String,
    pub source: String,
    pub config: BTreeMap<String, Value>,
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
        }
    }
}

/// Helper alias for `Result` objects that return `Error`.
pub type Result<T> = ::std::result::Result<T, Error>;

impl Query {
    pub fn into_msg(&self) -> SonicMessage {
        SonicMessage {
            event_type: "Q".to_owned(),
            variation: Some(self.query.clone()),
            payload: Some(Value::Object(self.config.clone())),
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

    pub fn error(msg: String) -> Receipt {
        Receipt {
            success: false,
            errors: vec![msg],
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
