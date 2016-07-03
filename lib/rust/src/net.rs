use model::protocol::SonicMessage;
use error::{ErrorKind, Result};
use std::io::Cursor;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use io::read_next;

pub fn read_message(fd: &i32) -> Result<SonicMessage> {

    let len_buf = &mut [0; 4];

    // read length header bytes
    try!(read_next(4, *fd, len_buf));

    let mut rdr = Cursor::new(len_buf);

    // decode length header
    let len = try!(rdr.read_i32::<BigEndian>()
        .map_err(|e| ErrorKind::BigEndianError(e))) as usize;

    let mut buf = vec!(0; len);

    // read message bytes
    try!(read_next(len, *fd, buf.as_mut_slice()));

    SonicMessage::from_slice(buf.as_slice())
}

pub fn frame(msg: SonicMessage) -> Result<Vec<u8>> {
    let qbytes = try!(msg.into_bytes());

    let qlen = qbytes.len() as i32;
    let mut fbytes = Vec::new();

    try!(fbytes.write_i32::<BigEndian>(qlen)
        .map_err(|e| ErrorKind::BigEndianError(e)));

    fbytes.extend(qbytes.as_slice());
    Ok(fbytes)
}
