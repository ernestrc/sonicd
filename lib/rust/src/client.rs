use model::*;
use model::protocol::{SonicMessage, MessageKind};
use error::{ErrorKind, Result};
use std::io::Write;
use std::sync::mpsc::Receiver;
use std::fmt::Debug;
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::slice;
use nix::sys::socket::*;
use nix::sys::epoll::*;
use nix::unistd;
use std::os::raw::c_int;
use std::os::unix::io::{RawFd, AsRawFd};
use io::NO_INTEREST;

static DEFAULT_TIMEOUT: &'static u64 = &10;
static INIT_CONN_CAP: &'static usize = &5;

pub struct Client<A: ToSocketAddrs, F: HandlerFactory<H>, H: Handler> {
    rx: Receiver<SonicMessage>,
    factory: H,
    addr: A,
    connections: Vec<Connection>,
    timeout: u64, // in sec
    epfd: RawFd,
}

impl<A: ToSocketAddrs> Drop for Client<A> {
    fn drop(&mut self) {
        match unistd::close(self.epfd) {
            Ok(_) => debug!("closed {}", self.epfd),
            Err(e) => error!("close {}: {}", self.epfd, e),
        }
    }
}

impl<A: ToSocketAddrs> Client<A> {
    fn create_connection(&mut self) -> Result<Connection> {
        let mut stream = try!(TcpStream::connect(self.addr));
        try!(stream.set_read_timeout(Some(::std::time::Duration::new(self.timeout, 0))));

        let clifd = stream.as_raw_fd();
        let interests = EpollEvent {
            events: EPOLLET | EPOLLIN | EPOLLOUT | EPOLLERR | EPOLLRDHUP,
            data: clifd as u64,
        };

        try!(epoll_ctl(self.epfd, EpollOp::EpollCtlAdd, clifd, &interests));

        let conn = Connection {
            fd: clifd,
            trace_id: self.connections.len() as i32,
            stream: stream,
            epfd: self.epfd,
        };

        self.connections.push(conn);

        Ok(conn)
    }

    pub fn new(addr: A, rx: Receiver<SonicMessage>, timeout: Option<u64>) -> Result<Client<A>> {
        let epfd = try!(epoll_create());
        Ok(Client {
            addr: addr,
            rx: rx,
            connections: Vec::with_capacity(*INIT_CONN_CAP),
            timeout: timeout.unwrap_or_else(|| *DEFAULT_TIMEOUT),
            epfd: epfd,
        })
    }

    fn run(&mut self, query: Query) {

        let conn = try!(self.create_connection());

        // TODO refactor out into an IoHandler/Selector and re-use in other places
        loop {

            let EVN = 1;
            let mut evts: Vec<EpollEvent> = Vec::with_capacity(EVN);
            let dst = unsafe { slice::from_raw_parts_mut(evts.as_mut_ptr(), evts.capacity()) };
            let cnt = perror!("epoll_wait", epoll_wait(self.epfd, dst, -1));
            unsafe { evts.set_len(cnt) }

            for ev in evts {

                let clifd: c_int = ev.data as i32;


                let events = ev.events;

                // get handler
                if events.contains(EPOLLRDHUP) {
                    shutdown(clifd, Shutdown::Both).unwrap();
                    debug!("successfully closed clifd {}", clifd);

                    perror!("epoll_ctl",
                            epoll_ctl(self.epfd, EpollOp::EpollCtlDel, clifd, &NO_INTEREST));
                    debug!("unregistered interests for {}", clifd);

                    break;
                } else {

                    let mut handler: &mut Rc<Box<Handler>> = match handlers.entry(clifd) {
                        Occupied(entry) => entry.into_mut(),
                        Vacant(entry) => {
                            entry.insert(Rc::new(Box::new(IoHandler::new(epfd, clifd))))
                        }
                    };

                    if events.contains(EPOLLERR) {
                        error!("socket error on {}", clifd);
                    }

                    if events.contains(EPOLLIN) {
                        debug!("socket of {} is readable", clifd);
                        // TODO stop handler and notify client
                        perror!("on_readable()", handler.on_readable());
                    }

                    if events.contains(EPOLLOUT) {
                        debug!("socket of {} is writable", clifd);
                        perror!("on_writable()", handler.on_writable());
                    }
                }
            }
        }
    }

    // fn stream<C>(&self, command: C) -> Result<()>
    // where C: Command + Debug + Into<SonicMessage>
    // {
    //
    // let conn = try!(TcpStream::connect(self.addr));
    //
    // set timeout 10s
    // try!(stream.set_read_timeout(Some(::std::time::Duration::new(10, 0))));
    //
    // debug!("framing command {:?}", &command);
    //
    // frame command
    // let fbytes = try!(io::frame(command.into()));
    //
    // debug!("framed command into {} bytes", fbytes.len());
    //
    // send query
    // try!(stream.write(&fbytes.as_slice()));
    //
    // let fd = stream.as_raw_fd();
    //
    // let res: Result<()>;
    //
    // loop {
    // let msg = try!(io::read_message(&fd));
    // match msg.event_type {
    // MessageKind::OutputKind => output(try!(msg.into())),
    // MessageKind::ProgressKind => progress(try!(msg.into())),
    // MessageKind::TypeMetadataKind => metadata(try!(msg.into())),
    // MessageKind::DoneKind => {
    // let d: Done = try!(msg.into());
    // if let Some(error) = d.0 {
    // res = Err(ErrorKind::QueryError(error).into())
    // } else {
    // res = Ok(());
    // };
    // let msg: SonicMessage = Acknowledge.into();
    // let fbytes = try!(io::frame(msg));
    // send ack
    // try!(stream.write(&fbytes.as_slice()));
    // debug!("disconnected");
    // break;
    // }
    // MessageKind::QueryKind |
    // MessageKind::AuthKind |
    // MessageKind::AcknowledgeKind => {}
    // }
    // }
    //
    // return res;
    // }
    //
    // fn authenticate<A: ToSocketAddrs>(user: String,
    // key: String,
    // addr: A,
    // trace_id: Option<String>)
    // -> Result<String> {
    //
    // let auth = Authenticate {
    // key: key,
    // user: user,
    // trace_id: trace_id,
    // };
    // let mut buf: Vec<OutputChunk> = Vec::new();
    //
    // {
    // let fn_buf = |msg| {
    // buf.push(msg);
    // };
    //
    // try!(stream(auth, addr, fn_buf, |_| {}, |_| {}));
    // }
    //
    // let OutputChunk(data) = try!(buf.into_iter()
    // .next()
    // .ok_or_else(|| ErrorKind::Proto("no messages returned".to_owned())));
    //
    // let x = try!(data.into_iter()
    // .next()
    // .ok_or_else(|| ErrorKind::Proto("output is empty".to_owned())));
    // let s = try!(x.as_str()
    // .ok_or_else(|| ErrorKind::Proto("token is not a string".to_owned())));
    // Ok(s.to_owned())
    // }
}
