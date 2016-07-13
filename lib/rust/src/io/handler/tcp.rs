use std::os::unix::io::RawFd;
use std::io::{Cursor, Write, Read};
use std::cell::RefCell;
use std::marker::Sized;
use std::rc::Rc;

use byteorder::{BigEndian, ByteOrder};
use bytes::{MutBuf, MutByteBuf, ByteBuf, Buf};

use model::protocol::SonicMessage;
use error::*;
//use {Handler, Factory, Action};
use super::super::connection::Connection;
use source::Source;

const MEGABYTE: usize = 1024 * 1024;

/*
#[derive(PartialEq)]
pub enum Protocol {
    Echo,
    Sonic,
}

pub enum TcpAction {
    New(Protocol, RawFd),
    Notify(usize, RawFd),
}

impl Action for TcpAction {
    fn encode(self) -> u64 {
        match self {
            TcpAction::Notify(id, fd) => ((fd as u64) << 31) | ((id as u64) << 15) | 0,
            TcpAction::New(Protocol::Sonic, fd) => ((fd as u64) << 31) | (0 << 15) | 1,
            TcpAction::New(Protocol::Echo, fd) => ((fd as u64) << 31) | (1 << 15) | 1,
        }
    }
}

impl<C: Write + Read> Factory<TcpHandler<C>, TcpAction> for Tcp {

    fn process<F: FnOnce(usize) -> TcpHandler<C>>(&self, action: TcpAction) -> Result<Option<F>> {
        unimplemented!()
        // Action::New(kind, fd) => {
        //
        // let idm = self.handlers.insert_with(|id| {
        // RefCell::new(match kind {
        // HandlerKind::Echo => Box::new(EchoHandler::new(id, fd)),
        // HandlerKind::Tcp => Box::new(TcpHandler::new(id, epfd, fd)),
        // })
        // });
        //
        // match idm {
        // Some(id) => {
        // respects set of events of the original caller
        // but encodes Notify action in data
        // let new = EpollEvent {
        // events: ev.events | EPOLLET | EPOLLHUP | EPOLLRDHUP,
        // data: Self::encode(Action::Notify(id, fd)),
        // };
        //
        // perror!("reregister()", Self::reregister(epfd, fd, new));
        // self.notify(fd, id, ev.events)
        // }
        // None => error!("epoll {} reached maximum number of handlers", epfd),
        // }
        // }
        // Action::Notify(id, fd) => self.notify(fd, id, ev.events),
        // }
    }
}

// TODO close fds and unregister events from epoll
// TODO implement flags to keep state for edge-triggered
pub struct TcpHandler<C: Write + Read> {
    // only one handler at a time can perform mut borrows
    id: usize,
    epfd: RawFd,
    sockw: bool,
    sockr: bool,
    conn: C,
    source: Option<Box<Source>>,
    buf: Option<ByteBuf>,
    mut_buf: Option<MutByteBuf>,
    mbuf: Vec<SonicMessage>,
}

impl TcpHandler<Connection> {
    /// Instantiate a new I/O handler
    pub fn new(id: usize, epfd: RawFd, clifd: RawFd) -> TcpHandler<Connection> {
        let conn = Connection::new(epfd, clifd);
        TcpHandler {
            id: id,
            epfd: epfd,
            sockw: false,
            sockr: false,
            conn: conn,
            source: None,
            buf: Some(ByteBuf::none()),
            mut_buf: Some(ByteBuf::mut_with_capacity(MEGABYTE)),
            mbuf: Vec::new(),
        }
    }
}

impl<C: Write + Read> TcpHandler<C> {
    pub fn try_into_buf(&mut self, buf: &[u8]) -> Result<usize> {

        let mut read = 0;
        let total = buf.len();

        while total > 4 {

            let length = BigEndian::read_u32(&buf[0..4]);

            if total as u32 == (length + 4_u32) {
                let bytes = &buf[4..length as usize];
                let msg = try!(SonicMessage::from_slice(bytes));
                self.mbuf.push(msg);
                read += bytes.len();
            }
        }
        Ok(read)
    }
}

// TODO check for overflow
impl<C: Write + Read> Handler for TcpHandler<C> {

    fn id(&self) -> usize {
        self.id
    }

    fn on_error(&mut self) -> Result<()> {
        unimplemented!()
    }

    fn on_close(&mut self) -> Result<()> {
        unimplemented!()
    }

    fn on_readable(&mut self) -> Result<()> {
        self.sockr = true;

        trace!("on_readable()");

        let mut buf: MutByteBuf = try!(self.mut_buf
            .take()
            .ok_or(ErrorKind::UnexpectedState("io handler not ready for on_readable()")));

        self.sockr = false;

        let read = try!(self.conn.read(unsafe { buf.mut_bytes() }));

        match read {
            0 => {
                trace!("on_readable() socket not ready");
                self.mut_buf = Some(buf);
                Ok(())
            }
            cnt => {
                unsafe { buf.advance(cnt) };
                trace!("on_readable() bytes {}", cnt);

                let buf = buf.flip();

                // FIXME here is where it should be handed into source
                // let parsed = try!(self.try_into_buf((&buf).bytes()));
                let parsed = 0;

                if parsed == cnt {
                    trace!("on_readable() complete");
                    self.mut_buf = Some(buf.flip());
                } else {
                    trace!("on_readable() incomplete");
                    self.mut_buf = Some(buf.resume());
                }
                Ok(())
            }
        }
    }

    fn on_writable(&mut self) -> Result<()> {
        trace!("on_writable()");
        self.sockw = true;

        let mut buf: ByteBuf = try!(self.buf
            .take()
            .ok_or(ErrorKind::UnexpectedState("io handler not ready for on_writable()")));

        let cnt = try!(self.conn.write((&buf).bytes()));

        if cnt > 0 {
            buf.advance(cnt)
        }

        self.mut_buf = Some(buf.flip());
        Ok(())
    }
}*/
