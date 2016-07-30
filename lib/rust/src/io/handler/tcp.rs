use std::os::unix::io::RawFd;
use std::cell::RefCell;
use std::io::Cursor;

use nix::unistd;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use model::protocol::*;
use model::*;
use model;
use io::buf::ByteBuffer;
use source::{Source, StreamOut};
use io::handler::Handler;
use io::{read, write, frame, MAX_MSG_SIZE, EpollFd};
use error::{Result, ErrorKind, Error};

#[derive(Debug)]
pub struct TcpHandler {
    epfd: EpollFd,
    sockw: bool,
    sockr: bool,
    clifd: RawFd,
    ibuf: ByteBuffer,
    obuf: ByteBuffer,
    closing: bool,
    source: Option<RefCell<Box<Source>>>,
}

impl TcpHandler {
    pub fn new(clifd: RawFd, epfd: EpollFd) -> TcpHandler {
        trace!("new()");
        TcpHandler {
            epfd: epfd,
            clifd: clifd,
            sockw: false,
            sockr: false,
            ibuf: ByteBuffer::new(),
            obuf: ByteBuffer::new(),
            closing: false,
            source: None,
        }
    }
}

impl TcpHandler {
    fn fill_buf(&mut self) -> Result<usize> {
        trace!("fill_buf()");
        {
            let read = try!(read(self.clifd, From::from(&mut self.ibuf)));

            match read {
                Some(cnt) => {
                    self.ibuf.extend(cnt);
                    trace!("on_readable() bytes {}", cnt);
                    Ok(cnt)
                }
                None => {
                    trace!("on_readable() socket not ready");
                    Ok(0)
                }
            }
        }
    }

    fn oflush(&mut self) -> Result<Option<usize>> {
        trace!("oflush()");
        if let Some(cnt) = try!(write(self.clifd, From::from(&self.obuf))) {
            trace!("oflush(): {:?} bytes", &cnt);
            self.obuf.consume(cnt);
            Ok(Some(cnt))
        } else {
            self.sockw = false;
            Ok(None)
        }
    }

    fn obuffer(&mut self, msg: SonicMessage) -> Result<()> {
        trace!("obuffer()");

        let bytes = try!(frame(msg));
        try!(self.obuf.write(bytes.as_slice()));

        Ok(())
    }

    fn ovbuffer(&mut self, mut msgs: Vec<SonicMessage>) -> Result<()> {
        trace!("ovbuffer()");

        let len = msgs.len();
        for msg in msgs.drain(..len) {
            try!(self.obuffer(msg));
        }

        Ok(())
    }

    fn done(&mut self, err: Option<Error>) -> Result<()> {
        trace!("done()");
        self.closing = true;
        self.obuffer(SonicMessage::Done(err.map(|e| format!("{:?}", e))).into())
    }

    fn read_message(&mut self) -> Result<Option<SonicMessage>> {
        trace!("read_message()");

        let len_buf = &mut [0; 4];

        // read length header bytes
        try!(self.ibuf.read(len_buf));

        let mut rdr = Cursor::new(len_buf);

        // decode length header
        let len = try!(rdr.read_i32::<BigEndian>()) as usize;
        trace!("read_message(): len: {:?}", len);

        if self.ibuf.len() >= len + 4 {
            // result as intended
            let msg = {
                let buf: &[u8] = From::from(&self.ibuf);
                try!(SonicMessage::from_slice(buf.split_at(4).1.split_at(len).0))
            };

            self.ibuf.consume(len);

            trace!("read_message(): {:?}", &msg);
            Ok(Some(msg))

        } else {
            trace!("read_message(): < {:?} bytes were available for read", &len);
            Ok(None)
        }
    }

    fn close(&self) -> Result<()> {
        try!(unistd::close(self.clifd));
        Ok(())
    }
}

impl Handler for TcpHandler {
    fn on_error(&mut self) -> Result<()> {
        error!("socket error: {:?}", self);
        Ok(())
    }

    fn on_close(&mut self) -> Result<()> {
        trace!("on_close()");
        if !self.closing {
            error!("client closed unexpectedly");
        }
        Ok(())
    }

    fn on_readable(&mut self) -> Result<()> {
        trace!("on_readable()");

        // first msg from client
        if self.source.is_none() {

            let cnt = try!(self.fill_buf());

            if cnt + self.ibuf.len() > 4 {

                if let Some(msg) = try!(self.read_message()) {

                    match msg {
                        SonicMessage::QueryMsg(query) => {

                            self.source = Some(RefCell::new(try!(Source::new(query, self.epfd))));

                            if self.sockw {
                                return self.on_writable();
                            }

                        },

                        SonicMessage::AuthenticateMsg(_) => {
                            try!(self.done(Some("not implemented!".into())))
                        },

                        cmd => try!(self.done(Some(format!("unexpected cmd: {:?}", cmd).into()))),
                    }
                }
            }

            Ok(())

        } else if self.closing {
            let cnt = try!(self.fill_buf());

            if cnt + self.ibuf.len() > 4 {
                match try!(self.read_message()) {
                    Some(SonicMessage::Acknowledge) => try!(self.close()),
                    Some(m) => try!(self.done(Some(format!("protocol error: expected ack but {:?} found", m).into()))),
                    None => {},
                }
            }

            Ok(())

        } else {
            self.done(Some(ErrorKind::Proto("unexpected client message".to_owned()).into()))
        }
    }

    fn on_writable(&mut self) -> Result<()> {
        trace!("on_writable()");

        let source = match self.source.take() {
            Some(s) => {
                {
                    let mut source = s.borrow_mut();

                    // get next batch from source
                    match try!(source.next()) {
                        StreamOut::Message(msg) => {
                            try!(self.obuffer(msg));

                            if let Some(cnt) = try!(self.oflush()) {
                                trace!("on_writable(): written {} bytes", cnt);
                            } else {
                                // would block
                                self.sockw = false;
                            }
                        },
                        StreamOut::Idle => {
                            self.sockw = true;
                        },
                        StreamOut::Completed => {
                            try!(self.done(None));
                            try!(self.oflush());
                        }
                    }
                }

                Some(s)
            }

            None => {

                if !self.obuf.is_empty() {
                    try!(self.oflush());
                } else {
                    self.sockw = true;
                }

                None
            }
        };

        self.source = source;

        Ok(())
    }
}
