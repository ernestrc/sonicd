use model::*;
use std::fmt::Debug;
use model::protocol::{SonicMessage, MessageKind};
use error::{ErrorKind, Result};
use std::net::TcpStream;
use std::os::unix::io::AsRawFd;
use std::io::Write;
use io::*;
use std::net::ToSocketAddrs;

pub fn run<A: ToSocketAddrs>(query: Query, addr: A) -> Result<Vec<OutputChunk>> {

    let mut buf = Vec::new();

    {
        let fn_buf = |msg| {
            buf.push(msg);
        };

        try!(stream(query, addr, fn_buf, |_| {}, |_| {}));
    }

    Ok(buf)
}

pub fn stream<C, O, P, M, A>(command: C,
                             addr: A,
                             mut output: O,
                             mut progress: P,
                             mut metadata: M)
                             -> Result<()>
    where O: FnMut(OutputChunk) -> (),
          P: FnMut(QueryProgress) -> (),
          M: FnMut(TypeMetadata) -> (),
          C: Command + Debug + Into<SonicMessage>,
          A: ToSocketAddrs
{

    let mut stream = try!(TcpStream::connect(addr));

    // set timeout 10s
    try!(stream.set_read_timeout(Some(::std::time::Duration::new(10, 0))));

    debug!("framing command {:?}", &command);

    // frame command
    let fbytes = try!(frame(command.into()));

    debug!("framed command into {} bytes", fbytes.len());

    // send query
    try!(stream.write(&fbytes.as_slice()));

    let fd = stream.as_raw_fd();

    let res: Result<()>;

    loop {
        let msg = try!(read_message(fd));
        match msg.event_type {
            MessageKind::OutputKind => output(try!(msg.into())),
            MessageKind::ProgressKind => progress(try!(msg.into())),
            MessageKind::TypeMetadataKind => metadata(try!(msg.into())),
            MessageKind::DoneKind => {
                let d: Done = try!(msg.into());
                if let Some(error) = d.0 {
                    res = Err(ErrorKind::QueryError(error).into())
                } else {
                    res = Ok(());
                };
                let msg: SonicMessage = Acknowledge.into();
                let fbytes = try!(frame(msg));
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

pub fn authenticate<A: ToSocketAddrs>(user: String,
                                      key: String,
                                      addr: A,
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

    let OutputChunk(data) = try!(buf.into_iter()
        .next()
        .ok_or_else(|| ErrorKind::Proto("no messages returned".to_owned())));

    let x =
        try!(data.into_iter().next().ok_or_else(|| ErrorKind::Proto("output is empty".to_owned())));
    let s = try!(x.as_str().ok_or_else(|| ErrorKind::Proto("token is not a string".to_owned())));
    Ok(s.to_owned())
}
