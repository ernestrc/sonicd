use model::{Query, Error, Result, SonicMessage, Receipt};
use std::collections::HashMap;
use curl::http::{Response, Request, Handle};
use curl::http::handle::Method;
use std::net::TcpStream;
use std::io::Write;
use std::os::unix::io::AsRawFd;

static API_VERSION: &'static str = "v1";

fn build_uri(scheme: &str, host: &str, http_port: &u16, path: &str) -> String {
    format!("{}://{}:{}/{}/{}",
            scheme,
            host,
            http_port,
            API_VERSION,
            path)
}

fn request(path: &str,
           body: Option<&str>,
           method: Method,
           host: &str,
           http_port: &u16,
           client: &mut Handle)
           -> Result<Response> {

    let url = build_uri("http", host, http_port, path);

    let mut headers = HashMap::new();
    headers.insert("Content-Type", "application/json");

    let req = Request::new(client, method)
                  .uri(url)
                  .headers(headers.into_iter());

    let res = match body {
        Some(b) => req.body(b).exec(),
        None => req.exec(),
    };

    res.map_err(|e| Error::HttpError(e))
}

pub fn stream<O, P, M>(query: Query,
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
    let addr = try!(::tcp::get_addr(addr, port));

    debug!("resolved host and port to addr {}", addr);

    let mut res = Receipt::error(Error::ProtocolError("protocol error. socket closed before done"
                                                          .to_owned()));

    let mut stream = try!(TcpStream::connect(&addr).map_err(|e| Error::Connect(e)));

    // set timeout 10s
    stream.set_read_timeout(Some(::std::time::Duration::new(10, 0))).unwrap();

    // frame query
    let fbytes = ::tcp::frame(query.into_msg());

    // send query
    stream.write(&fbytes.as_slice()).unwrap();

    let fd = stream.as_raw_fd();

    loop {
        match ::tcp::read_message(&fd) {
            Ok(msg) => {
                match msg.event_type.as_ref() {
                    "O" => output(msg),
                    "P" => progress(msg),
                    "T" => metadata(msg),
                    "D" => {
                        res = msg.into_rec();
                        let fbytes = ::tcp::frame(SonicMessage::ack());
                        // send ack
                        stream.write(&fbytes.as_slice()).unwrap();
                        debug!("disconnected");
                        break;
                    }
                    _ => {}
                }
            }
            Err(r) => {
                res = Receipt::error(r);
                break;
            }
        }
    }

    if res.success {
        Ok(())
    } else {
        Err(Error::StreamError(res))
    }
}

pub fn run(query: Query, host: &str, tcp_port: &u16) -> Result<Vec<SonicMessage>> {

    let mut buf = Vec::new();

    {
        let fn_buf = |msg| {
            buf.push(msg);
        };

        try!(stream(query, host, tcp_port, fn_buf, |_| {}, |_| {}));
    }

    Ok(buf)
}

pub fn version(host: &str, http_port: &u16) -> Result<String> {
    let mut client = Handle::new();
    let r = try!(request("version", None, Method::Get, host, http_port, &mut client));
    String::from_utf8(r.move_body()).map_err(|e| Error::SerDe(format!("{}", e)))
}
