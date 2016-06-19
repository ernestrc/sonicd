use model::{SonicMessage, Error, Result};
use std::net::SocketAddr;
use std::fmt::Display;
use std::io::Cursor;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use io::read;

fn parse_addr<T: Display>(addr: T, port: &u16) -> Result<SocketAddr> {
    format!("{}:{}", addr, port)
        .parse::<SocketAddr>()
        .map_err(|e| Error::ParseAddr(e))
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

fn frame(msg: ::serde_json::Value) -> Vec<u8> {
    let qbytes = ::serde_json::to_string(&msg).unwrap().into_bytes();
    let qlen = qbytes.len() as i32;
    let mut fbytes = Vec::new();
    fbytes.write_i32::<BigEndian>(qlen).unwrap();
    fbytes.extend(qbytes.as_slice());
    fbytes
}
