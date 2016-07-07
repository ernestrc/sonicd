use error::{Result, ErrorKind, Error};
use sonicd::SonicMessage;
use sonicd::io::{read, write};
use std::os::unix::io::RawFd;
use std::io::{Cursor, Write, Read};
use byteorder::{BigEndian, ByteOrder};
use bytes::{MutBuf, MutByteBuf, ByteBuf, Buf};

const MEGABYTE: usize = 1024 * 1024;

pub trait Handler {
    fn on_readable(&mut self) -> Result<()>;
    fn on_writable(&mut self) -> Result<()>;
}

// TODO close fds and unregister events from epoll
// TODO implement flags to keep state for edge-triggered
pub struct IoHandler {
    sockw: bool,
    sockr: bool,
    epfd: RawFd,
    clifd: RawFd,
    buf: Option<ByteBuf>,
    mut_buf: Option<MutByteBuf>,
    mbuf: Vec<SonicMessage>,
}

impl IoHandler {
    /// Instantiate a new I/O
    pub fn new(epfd: RawFd, clifd: RawFd) -> IoHandler {
        IoHandler {
            sockw: false,
            sockr: false,
            epfd: epfd,
            clifd: clifd,
            buf: Some(ByteBuf::none()),
            mut_buf: Some(ByteBuf::mut_with_capacity(MEGABYTE)),
            mbuf: Vec::new(),
        }
    }
}

impl IoHandler {
    fn try_read_buf(&mut self, buf: &[u8]) -> Result<usize> {

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

    fn try_write_sock(&mut self, buf: &[u8]) -> Result<Option<usize>> {
        let n = try!(write(self.clifd, buf));
        Ok(n)
    }

    fn try_read_sock(&mut self, buf: &mut [u8]) -> Result<Option<usize>> {
        let n = try!(read(self.clifd, buf));
        Ok(n)
    }
}

// TODO check for overflow
impl Handler for IoHandler {
    fn on_readable(&mut self) -> Result<()> {
        self.sockr = true;

        trace!("on_readable()");

        let mut buf: MutByteBuf = try!(self.mut_buf
            .take()
            .ok_or(|| {
                ErrorKind::UnexpectedState.into()
                    .chain_err(|| "io handler not ready for on_readable()".into())
            }));

        let read = try!(self.try_read_sock(unsafe { buf.mut_bytes() }));

        match read {
            Some(cnt) => {
                unsafe { buf.advance(cnt) };
                trace!("on_readable() bytes {}", cnt);

                let buf = buf.flip();

                let parsed = try!(self.try_read_buf((&buf).bytes()));

                if parsed == cnt {
                    trace!("on_readable() complete");
                    self.mut_buf = Some(buf.flip());
                } else {
                    trace!("on_readable() incomplete");
                    self.mut_buf = Some(buf.resume());
                }
                Ok(())
            }
            None => {
                trace!("on_readable() socket not ready");
                self.mut_buf = Some(buf);
                Ok(())
            }
        }
    }

    fn on_writable(&mut self) -> Result<()> {
        trace!("on_writable()");
        self.sockw = true;

        let mut buf: ByteBuf = try!(self.buf
            .take()
            .ok_or(|| "io handler not ready for on_writable()".into()));

        let res = try!(self.try_write_sock((&buf).bytes()));

        if let Some(cnt) = res {
            unsafe { buf.advance(cnt) };
        }
        self.mut_buf = Some(buf.flip());
        Ok(())
    }
}
