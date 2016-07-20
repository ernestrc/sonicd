use model::protocol::SonicMessage;
use error::{Result, Error};
use std::io::Cursor;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use nix::unistd;
use std::os::unix::io::RawFd;
use nix::sys::epoll::{EpollEvent, EpollEventKind};

const DEFAULT_BUF_SIZE: usize = 1024 * 1024;
const MAX_MSG_SIZE: usize = 1024 * 1024;

#[macro_export]
macro_rules! eintr {
    ($syscall:expr, $name:expr, $($arg:expr),*) => {{
        let res;
        loop {
            match $syscall($($arg),*) {
                Ok(m) => {
                    res = Ok(Some(m));
                    break;
                },
                Err(::nix::Error::Sys(::nix::errno::EAGAIN)) => {
                    trace!("{}: EAGAIN: socket not ready", $name);
                    res = Ok(None);
                    break;
                },
                Err(::nix::Error::Sys(::nix::errno::EINTR)) => {
                    debug!("{}: EINTR: re-submitting syscall", $name);
                    continue;
                },
                Err(err) => {
                    res = Err(err);
                    break;
                }
            }
        }
        res
    }}
}

#[macro_export]
macro_rules! eagain {
    ($syscall:expr, $name:expr, $($arg:expr),*) => {{
        let res;
        loop {
            match $syscall($($arg),*) {
                Ok(m) => {
                    res = Ok(m);
                    break;
                },
                Err(::nix::Error::Sys(a@::nix::errno::EINTR)) |
                    Err(::nix::Error::Sys(a@::nix::errno::EAGAIN))=> {
                        debug!("{}: {}: re-submitting syscall", $name, a);
                        continue;
                    },
                    Err(err) => {
                        res = Err(err);
                        break;
                    }
            }
        }
        res
    }}
}

#[macro_export]
macro_rules! report_err {
    ($name:expr, $err:expr) => {{
        let e: Error = $err;
        error!("{}: {}\n{:?}", $name, e, e.backtrace());

        for e in e.iter().skip(1) {
            error!("caused_by: {}", e);
        }
    }}
}

/// helper to short-circuit loops and log error
#[macro_export]
macro_rules! perrorr {
    ($name:expr, $res:expr) => {{
        match $res {
            Ok(s) => s,
            Err(err) => {
                let err: Error = err.into();
                report_err!($name, err);
                return;
            }
        }
    }}
}

#[macro_export]
macro_rules! perror {
    ($name:expr, $res:expr) => {{
        match $res {
            Ok(s) => s,
            Err(e) => {
                report_err!($name, e.into());
            },
        }
    }}
}

lazy_static! {
    pub static ref NO_INTEREST: EpollEvent = {
        EpollEvent {
            events: EpollEventKind::empty(),
            data: 0,
        }
    };
}

pub fn write(fd: RawFd, buf: &[u8]) -> Result<Option<usize>> {
    let b = try!(eintr!(unistd::write, "unistd::write", fd, buf));

    Ok(b)
}

pub fn read(fd: RawFd, buf: &mut [u8]) -> Result<Option<usize>> {

    let b = try!(eintr!(unistd::read, "unistd::read", fd, buf));

    trace!("unistd::read {:?} bytes", b);

    Ok(b)
}

pub fn read_next(len: usize, fd: RawFd, buf: &mut [u8]) -> Result<usize> {
    let b = try!(eagain!(unistd::read, "unistd::read", fd, buf));

    if b == len {
        // result as intended
        Ok(b)
    } else if b > 0 {
        // signal interruped the read or error occurred
        // or less than 'len' bytes were available for read.
        // reissuing the read will either indicate
        // the cause of the error or read the remaining bytes
        let rem = len - b;
        let mut rembuf = vec!(0; rem);

        debug!("unistd::read {} bytes: intended {}", b, len);

        let r = read_next(rem, fd, rembuf.as_mut_slice());
        buf.split_at_mut(b).1.copy_from_slice(rembuf.as_slice());
        r
    } else {
        // EOF reached, no data to read at this point
        debug!("unistd::read 0 bytes: EOF");
        Ok(b)
    }
}

pub fn read_message(fd: RawFd) -> Result<SonicMessage> {

    let len_buf = &mut [0; 4];

    // read length header bytes
    try!(read_next(4, fd, len_buf));

    let mut rdr = Cursor::new(len_buf);

    // decode length header
    let len = try!(rdr.read_i32::<BigEndian>()) as usize;

    let mut buf = vec!(0; len);

    // read message bytes
    try!(read_next(len, fd, buf.as_mut_slice()));

    SonicMessage::from_slice(buf.as_slice())
}

pub fn frame(msg: &SonicMessage) -> Result<Vec<u8>> {
    let qbytes = try!(msg.as_bytes());

    let qlen = qbytes.len() as i32;
    let mut fbytes = Vec::new();

    try!(fbytes.write_i32::<BigEndian>(qlen));

    fbytes.extend(qbytes.as_slice());
    Ok(fbytes)
}

pub mod handler;
pub mod connection;
pub mod poll;
pub mod buf;
pub mod controller;

pub use self::handler::Handler;
pub use self::poll::{Epoll, EpollFd};
pub use self::controller::sync::{SyncController, EpollProtocol, Action};
pub use self::controller::server::{Server, ServerImpl, SimpleMux, SimpleMuxConfig};
pub use self::controller::logging::{LoggingBackend, SimpleLogging};
pub use self::controller::Controller;
