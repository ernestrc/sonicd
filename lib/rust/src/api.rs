use model::{Query, Error, Result, SonicMessage};
use std::collections::HashMap;
use curl::http::{Response, Request, Handle};
use curl::http::handle::Method;

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

pub fn run(query: Query, host: &str, tcp_port: &u16) -> Result<Vec<SonicMessage>> {

    let mut buf = Vec::new();

    {
        let fn_buf = |msg| {
            buf.push(msg);
        };

        try!(::tcp::stream(query, host, tcp_port, fn_buf, |_| {}, |_| {}));
    }

    Ok(buf)
}

pub fn version(host: &str, http_port: &u16) -> Result<String> {
    let mut client = Handle::new();
    let r = try!(request("version", None, Method::Get, host, http_port, &mut client));
    String::from_utf8(r.move_body()).map_err(|e| Error::SerDe(format!("{}", e)))
}
