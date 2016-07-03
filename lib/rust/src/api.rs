use model::*;
use model::protocol::{SonicMessage, MessageKind};
use error::{ErrorKind, Result};
use curl::easy::Easy;
use net;
use std::net::TcpStream;
use std::os::unix::io::AsRawFd;
use std::io::Write;
use std::fmt::Debug;
use std::net::SocketAddr;

pub fn run(query: Query, addr: SocketAddr) -> Result<Vec<OutputChunk>> {

    let mut buf = Vec::new();

    {
        let fn_buf = |msg| {
            buf.push(msg);
        };

        try!(stream(query, addr, fn_buf, |_| {}, |_| {}));
    }

    Ok(buf)
}

pub fn stream<C, O, P, M>(command: C,
                          addr: SocketAddr,
                          mut output: O,
                          mut progress: P,
                          mut metadata: M)
                          -> Result<()>
    where O: FnMut(OutputChunk) -> (),
          P: FnMut(QueryProgress) -> (),
          M: FnMut(TypeMetadata) -> (),
          C: Command + Debug + Into<SonicMessage>
{

    let mut stream = try!(TcpStream::connect(&addr));

    // set timeout 10s
    try!(stream.set_read_timeout(Some(::std::time::Duration::new(10, 0))));

    debug!("framing command {:?}", &command);

    // frame command
    let fbytes = try!(net::frame(command.into()));

    debug!("framed command into {} bytes", fbytes.len());

    // send query
    try!(stream.write(&fbytes.as_slice()));

    let fd = stream.as_raw_fd();

    let res: Result<()>;

    loop {
        let msg = try!(net::read_message(&fd));
        match msg.event_type {
            MessageKind::OutputKind => output(try!(msg.into())),
            MessageKind::ProgressKind => progress(try!(msg.into())),
            MessageKind::LogKind => info!("{}", try!(msg.into::<Log>()).0),
            MessageKind::TypeMetadataKind => metadata(try!(msg.into())),
            MessageKind::DoneKind => {
                let d: Done = try!(msg.into());
                if let Some(error) = d.0 {
                    res = Err(ErrorKind::QueryError(error).into())
                } else {
                    res = Ok(());
                };
                let msg: SonicMessage = Acknowledge.into();
                let fbytes = try!(net::frame(msg));
                // send ack
                try!(stream.write(&fbytes.as_slice()));
                debug!("disconnected");
                break;
            }
            MessageKind::QueryKind |
            MessageKind::AuthKind |
            MessageKind::AcknowledgeKind => {}
        }
    }

    return res;
}

pub fn authenticate(user: String,
                    key: String,
                    addr: SocketAddr,
                    trace_id: Option<String>)
                    -> Result<String> {

    let auth = Authenticate {
        key: key,
        user: user,
        trace_id: trace_id,
    };
    let mut buf: Vec<OutputChunk> = Vec::new();

    {
        let fn_buf = |msg| {
            buf.push(msg);
        };

        try!(stream(auth, addr, fn_buf, |_| {}, |_| {}));
    }

    let OutputChunk(data) =
        try!(buf.into_iter().next().ok_or_else(|| ErrorKind::Proto("no messages returned")));

    let x = try!(data.into_iter().next().ok_or_else(|| ErrorKind::Proto("output is empty")));
    let s = try!(x.as_string().ok_or_else(|| ErrorKind::Proto("token is not a string")));
    Ok(s.to_owned())
}
