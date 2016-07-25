use std::os::unix::io::RawFd;
use std::slice;
use std::cell::RefCell;

use nix::unistd;

use io::controller::sync::EpollProtocol;
use model::protocol::*;
use io::handler::Handler;
use io::{read, write, read_message, frame, EpollFd};
use error::Result;

const BUF_CAP: usize = 1024 * 1024;

/// Handler that echoes incoming bytes
///
/// For benchmarking I/O throuput and latency
pub struct EchoHandler {
    sockw: bool,
    clifd: RawFd,
    pos: usize,
    limit: usize,
    buf: RefCell<Vec<u8>>,
}

impl EchoHandler {
    pub fn new(clifd: RawFd) -> EchoHandler {
        trace!("new()");
        EchoHandler {
            clifd: clifd,
            sockw: false,
            pos: 0,
            limit: 0,
            buf: RefCell::new(Vec::with_capacity(BUF_CAP)),
        }
    }
}

impl EchoHandler {
    fn try_write_sock(&self, buf: &[u8]) -> Result<Option<usize>> {
        let n = try!(write(self.clifd, buf));
        Ok(n)
    }

    fn try_read_sock(&self, buf: &mut [u8]) -> Result<Option<usize>> {
        let n = try!(read(self.clifd, buf));
        Ok(n)
    }
}

impl Handler for EchoHandler {
    fn on_error(&mut self) -> Result<()> {
        trace!("on_error()");
        panic!("bye!");
    }

    fn on_close(&mut self) -> Result<()> {
        trace!("on_close()");
        Ok(())
    }

    fn on_readable(&mut self) -> Result<()> {
        trace!("on_readable()");
        {
            let mut buf = self.buf.borrow_mut();
            let dst = unsafe { slice::from_raw_parts_mut(buf[self.limit..].as_mut_ptr(), buf.capacity()) };
            let read = try!(self.try_read_sock(dst));

            match read {
                Some(cnt) => {
                    unsafe { buf.set_len(cnt) }
                    self.limit += cnt;
                    trace!("on_readable() bytes {}", cnt);
                }
                None => {
                    trace!("on_readable() socket not ready");
                }
            }
        }
        if self.sockw {
            self.on_writable()
        } else {
            Ok(())
        }
    }

    fn on_writable(&mut self) -> Result<()> {
        trace!("on_writable()");

        let mut buf = self.buf.borrow_mut();
        let dst = &mut buf[self.pos..self.limit];

        if !dst.is_empty() {
            if let Some(cnt) = try!(self.try_write_sock(dst)) {
                trace!("on_writable() bytes {}", cnt);
                self.pos += cnt;
                // naive buffering
                if self.pos == self.limit {
                    self.pos = 0;
                    self.limit = 0;
                }
            } else {
                trace!("on_writable(): socket not ready");
            }
        } else {
            trace!("on_writable(): empty buf");
            self.sockw = true;
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub struct EchoEpollProtocol;

impl EpollProtocol for EchoEpollProtocol {

    type Protocol = usize;

    fn new(&self, p: usize, fd: RawFd, _: EpollFd) -> Box<Handler> {
        trace!("new()");
        match p {
            0 => Box::new(EchoHandler::new(fd)),
            1 => Box::new(SonicEchoHandler::new(fd)),
            i => panic!("not an expected protocol: {}", i),
        }
    }
}


/// Handler that echoes sonic messages
///
/// TODO buffering: right now it blocks
/// until one message is read
pub struct SonicEchoHandler {
    fd: RawFd,
    done: bool,
    buf: Vec<SonicMessage>,
}

impl SonicEchoHandler {
    pub fn new(fd: RawFd) -> SonicEchoHandler {
        SonicEchoHandler {
            fd: fd,
            done: false,
            buf: Vec::new()
        }
    }
}

impl Handler for SonicEchoHandler {

    fn on_error(&mut self) -> Result<()> {
        error!("on_error() SonicEchoHandler");
        Ok(())
    }

    fn on_close(&mut self) -> Result<()> {
        trace!("on_close()");
        if !self.done {
            Err("closed unexpectedly".into())
        } else {
            Ok(())
        }
    }
    fn on_readable(&mut self) -> Result<()> {
        trace!("on_readable()");
        let msg = try!(read_message(self.fd));
        debug!("read_message: {:?}", msg);
        match msg.event_type {
            MessageKind::AcknowledgeKind => {
                try!(unistd::close(self.fd));
            },
            _ => {
                self.buf.push(msg);
            }
        }
        Ok(())
    }

    fn on_writable(&mut self) -> Result<()> {
        trace!("on_writable()");
        while !self.buf.is_empty() {

            let msg = self.buf.remove(0);
            let event_type = msg.event_type;
            let framed = try!(frame(&msg));

            match try!(write(self.fd, framed.as_slice())) {
                Some(_) => {
                    debug!("write message: {:?}", &msg);
                    match event_type {
                        MessageKind::DoneKind => {
                            self.done = true
                        },
                        _ => {}
                    }
                },
                None => {
                    self.buf.insert(0, msg);
                    debug!("write: EAGAIN");
                    break;
                }
            }
        }
        Ok(())
    }
}
