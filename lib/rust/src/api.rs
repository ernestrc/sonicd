use model::{Query, Error, Result, SonicMessage};
use curl::easy::Easy;
use tcp;
use std::net::TcpStream;
use std::os::unix::io::AsRawFd;
use std::io::Write;
use std::collections::BTreeMap;
use serde_json::Value;

static API_VERSION: &'static str = "v1";

fn build_uri(scheme: &str, host: &str, http_port: &u16, path: &str) -> String {
    format!("{}://{}:{}/{}/{}",
            scheme,
            host,
            http_port,
            API_VERSION,
            path)
}

pub fn run(query: Query, host: &str, tcp_port: &u16) -> Result<Vec<SonicMessage>> {

    let mut buf = Vec::new();

    {
        let fn_buf = |msg| {
            buf.push(msg);
        };

        try!(stream(query.into_msg(), host, tcp_port, fn_buf, |_| {}, |_| {}));
    }

    Ok(buf)
}

pub fn stream<O, P, M>(command: SonicMessage,
                       addr: &str,
                       port: &u16,
                       mut output: O,
                       mut progress: P,
                       mut metadata: M)
    -> Result<()>
    where O: FnMut(SonicMessage) -> (),
          P: FnMut(SonicMessage) -> (),
          M: FnMut(SonicMessage) -> ()
{
    let addr = try!(tcp::get_addr(addr, port));

    debug!("resolved host and port to addr {}", addr);

    let mut stream = try!(TcpStream::connect(&addr).map_err(|e| Error::Connect(e)));

    // set timeout 10s
    stream.set_read_timeout(Some(::std::time::Duration::new(10, 0))).unwrap();

    debug!("framing command {:?}", &command);

    // frame command
    let fbytes = tcp::frame(command.into_json());

    debug!("framed command into {} bytes", fbytes.len());

    // send query
    stream.write(&fbytes.as_slice()).unwrap();

    let fd = stream.as_raw_fd();

    let res: Result<()>; 

    loop {
        match tcp::read_message(&fd) {
            Ok(msg) => {
                match msg.e.as_ref() {
                    "O" => output(msg),
                    "P" => progress(msg),
                    "T" => metadata(msg),
                    "D" => {
                        if let Some(error) = msg.v.map(|s| Error::StreamError(s)) {
                            res = Err(error)
                        } else {
                            res = Ok(());
                        };
                        let fbytes = tcp::frame(SonicMessage::ack().into_json());
                        // send ack
                        stream.write(&fbytes.as_slice()).unwrap();
                        debug!("disconnected");
                        break;
                    }
                    _ => {}
                }
            }
            Err(r) => {
                res = Err(r);
                break;
            }
        }
    }

    return res;
}

pub fn version(host: &str, http_port: &u16) -> Result<String> {

    let url = build_uri("http", host, http_port, "version");
    let mut handle = Easy::new();
    let mut buf = Vec::new();

    try!(handle.url(&url).map_err(|e| Error::HttpError(e)));

    try!(handle.perform().map_err(|e| Error::HttpError(e)));


    {
        let mut transfer = handle.transfer();

        try!(transfer.write_function(|data| {
            buf.extend_from_slice(data);
            Ok(data.len())
        })
             .map_err(|e| Error::HttpError(e)));

        transfer.perform().unwrap();
    }


    let s = try!(String::from_utf8(buf).map_err(|e| Error::SerDe(e.to_string())));

    Ok(s)
}

pub fn authenticate(user: String, key: String, host: &str, tcp_port: &u16) -> Result<String> {

    let mut payload = BTreeMap::new();

    payload.insert("user".to_owned(), Value::String(user));

    let command = SonicMessage {
        e: "H".to_owned(),
        v: Some(key),
        p: Some(Value::Object(payload))

    };

    let mut buf: Vec<SonicMessage> = Vec::new();

    {
        let fn_buf = |msg| {
            buf.push(msg);
        };

        try!(stream(command, host, tcp_port, fn_buf, |_| {}, |_| {}));
    }

    let token = match buf.iter().find(|m| m.e == "O".to_owned()) {
        Some(m) => m.clone().p.unwrap().as_array().unwrap().get(0).unwrap().as_string().unwrap().to_owned(),
        None => {
            return Err(Error::ProtocolError("no messages return by authenticate command".to_owned()));
        }
    };

    Ok(token)
}
