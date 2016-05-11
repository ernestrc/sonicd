use model::{SonicMessage, Error, Result};
use std::net::SocketAddr;
use std::fmt::Display;
use std::io::Cursor;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use nix::unistd;
use nix::errno::Errno::*;

fn read(len: usize, fd: i32, buf: &mut [u8]) -> Result<usize> {
    match unistd::read(fd, buf) {
        Ok(b) => {
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
                let r = read(rem, fd, rembuf.as_mut_slice());
                buf.split_at_mut(b).1.copy_from_slice(rembuf.as_slice());
                r
            } else {
                // EOF reached, no data to read at this point
                debug!("unistd::read 0 bytes: EOF");
                Ok(b)
            }
        }
        Err(::nix::Error::Sys(EAGAIN)) => {
            debug!("unistd::read: EAGAIN, resubmitting read");
            read(len, fd, buf)
        }
        Err(::nix::Error::Sys(EINTR)) => {
            debug!("unistd::read: EINTR, resubmitting read");
            read(len, fd, buf)
        }
        Err(e) => Err(Error::Io(e)),
    }
}

fn parse_addr<T: Display>(addr: T, port: &u16) -> Result<SocketAddr> {
    format!("{}:{}", addr, port)
        .parse::<SocketAddr>()
        .map_err(|e| Error::ParseAddr(e))
}

pub fn frame(msg: SonicMessage) -> Vec<u8> {
    let qbytes = msg.into_bytes();
    let qlen = qbytes.len() as i32;
    let mut fbytes = Vec::new();
    fbytes.write_i32::<BigEndian>(qlen).unwrap();
    fbytes.extend(qbytes.as_slice());
    fbytes
}

pub fn read_message(fd: &i32) -> Result<SonicMessage> {

    let len_buf = &mut [0; 4];

    // read length header bytes
    try!(read(4, *fd, len_buf));

    let mut rdr = Cursor::new(len_buf);

    // decode length header
    let len = rdr.read_i32::<BigEndian>().unwrap() as usize;

    let mut buf = vec!(0; len);

    // read message bytes
    try!(read(len, *fd, buf.as_mut_slice()));

    SonicMessage::from_slice(buf.as_slice())
}

pub fn get_addr(addr: &str, port: &u16) -> Result<SocketAddr> {
    parse_addr(addr, port).or_else(|_| {
        ::std::net::lookup_host(&addr)
            .map_err(|e| Error::GetAddr(e))
            .and_then(|mut a| {
                a.next()
                 .unwrap()
                 .map_err(|e| Error::GetAddr(e))
                 .map(|a| SocketAddr::new(a.ip(), *port))
            })
    })
}
