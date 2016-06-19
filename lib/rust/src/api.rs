use model::{Query, Error, Result, SonicMessage};
use curl::easy::Easy;

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

        try!(stream(query, host, tcp_port, fn_buf, |_| {}, |_| {}));
    }

    Ok(buf)
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
    unimplemented!()
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
