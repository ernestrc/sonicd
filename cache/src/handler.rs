use sonicd::io::poll::Epoll;
use sonicd::io::controller::sync::Factory;
use sonicd::io::handler::Handler;

use error::{Result, ErrorKind, Error};
use sonicd::SonicMessage;
use sonicd::io::{read, write};
use std::os::unix::io::RawFd;
use std::cell::RefCell;
use std::io::{Cursor, Write, Read};
use byteorder::{BigEndian, ByteOrder};
use bytes::{MutBuf, MutByteBuf, ByteBuf, Buf};

const MEGABYTE: usize = 1024 * 1024;

#[derive(Clone, Copy)]
pub struct EchoFactory;

impl Factory for EchoFactory {
    fn new(&self, p: usize, fd: RawFd) -> Box<Handler> {
        Box::new(EchoHandler::new(fd))
    }
}

// TODO close fds and unregister events from epoll
// TODO implement flags to keep state for edge-triggered
pub struct EchoHandler {
    sockw: bool,
    clifd: RawFd,
    buf: RefCell<Vec<u8>>,
}

impl EchoHandler {
    // Instantiate a new I/O
    pub fn new(clifd: RawFd) -> EchoHandler {
        EchoHandler {
            clifd: clifd,
            sockw: false,
            buf: RefCell::new(Vec::with_capacity(MEGABYTE)),
        }
    }
}

impl EchoHandler {
    fn try_write_sock(&self, buf: &[u8]) -> ::sonicd::Result<Option<usize>> {
        let n = try!(write(self.clifd, buf));
        Ok(n)
    }

    fn try_read_sock(&self, buf: &mut [u8]) -> ::sonicd::Result<Option<usize>> {
        let n = try!(read(self.clifd, buf));
        Ok(n)
    }
}

// TODO check for overflow
impl Handler for EchoHandler {
    fn on_error(&mut self) -> ::sonicd::Result<()> {
        panic!("bye!");
    }

    fn on_close(&mut self) -> ::sonicd::Result<()> {
        trace!("on_close()");
        Ok(())
    }

    fn on_readable(&mut self) -> ::sonicd::Result<()> {

        trace!("on_readable()");

        let read = try!(self.try_read_sock(self.buf.borrow_mut().as_mut_slice()));

        match read {
            Some(cnt) => {
                trace!("on_readable() bytes {}", cnt);
                if self.sockw {
                    try!(self.on_writable());
                }
            }
            None => {
                trace!("on_readable() socket not ready");
            }
        }
        Ok(())
    }

    fn on_writable(&mut self) -> ::sonicd::Result<()> {
        self.sockw = true;
        trace!("on_writable()");

        let mut borrow = self.buf.borrow_mut();

        if !borrow.is_empty() {
            let slice = borrow.as_mut_slice();
            try!(self.try_write_sock(slice));
        } else {
            try!(self.try_write_sock(b"echo"));
        }
        Ok(())
    }
}
