use std::os::unix::io::RawFd;
use std::cell::RefCell;
use std::io::Cursor;

use nix::unistd;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use model::protocol::*;
use model::*;
use model;
use io::buf::ByteBuffer;
use source::Source;
use io::handler::Handler;
use io::{read, write, frame, MAX_MSG_SIZE, EpollFd};
use error::{Result, ErrorKind};

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
            self.obuf.consume(cnt);
            Ok(Some(cnt))
        } else {
            self.sockw = false;
            Ok(None)
        }
    }

    fn obuffer(&mut self, msg: &SonicMessage) -> Result<()> {
        trace!("obuffer()");

        let bytes = try!(msg.as_bytes());
        try!(self.obuf.write(bytes.as_slice()));

        Ok(())
    }

    fn ovbuffer(&mut self, msgs: Vec<SonicMessage>) -> Result<()> {
        trace!("ovbuffer()");

        for msg in msgs.iter() {
            try!(self.obuffer(msg));
        }

        Ok(())
    }

    fn done(&mut self, done: model::Done) -> Result<()> {
        trace!("done()");
        self.closing = true;
        self.obuffer(&done.into())
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

    fn receive(&mut self, kind: MessageKind) -> Result<Option<SonicMessage>> {
        if let Some(msg) = try!(self.read_message()) {
            if msg.event_type == kind {
                Ok(Some(msg))
            } else {
                let err = ErrorKind::Proto(format!("unexpected message kind {:?}", msg.event_type)
                    .to_owned());
                Err(err.into())
            }
        } else {
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

                if let Some(msg) = try!(self.receive(MessageKind::QueryKind)) {

                    let query: Query = try!(msg.into());
                    debug!("recv {:?}", query);

                    self.source = Some(RefCell::new(try!(Source::new(query, self.epfd))));

                    if self.sockw {
                        return self.on_writable();
                    }
                }
            }

            Ok(())

        } else if self.closing {
            let cnt = try!(self.fill_buf());

            if cnt + self.ibuf.len() > 4 {
                if try!(self.receive(MessageKind::AcknowledgeKind)).is_some() {
                    try!(self.close());
                }
            }

            Ok(())

        } else {
            let err: Result<()> = Err(ErrorKind::Proto("unexpected client message".to_owned())
                .into());
            self.done(model::Done::new(err))
        }
    }

    fn on_writable(&mut self) -> Result<()> {
        trace!("on_writable()");

        match *&self.source {
            Some(ref s) => {
                let mut source = s.borrow_mut();

                let mut res: Result<()> = Ok(());

                // loop until EAGAIN, source exhausted or source is done
                loop {
                    // get next batch from source
                    if let Some(msgs) = try!(source.next()) {
                        // source exhausted
                        if msgs.is_empty() {
                            self.sockw = true;
                            break;
                        } else {
                            try!(self.ovbuffer(msgs));

                            if let Some(cnt) = try!(self.oflush()) {
                                trace!("on_writable(): written {} bytes", cnt);
                            } else {
                                // would block
                                self.sockw = false;
                                break;
                            }
                        }
                    } else {
                        res = self.done(Done(None));
                        break;
                    }
                }

                res
            }

            None => {

                if !self.obuf.is_empty() {
                    try!(self.oflush());
                } else {
                    self.sockw = true;
                }

                Ok(())

            }
        }

        // if self.source.is_some() {
        //
        // let s = &self.source.unwrap();
        // let mut source = s.borrow_mut();
        //
        // let mut res: Result<()> = Ok(());
        //
        // loop until EAGAIN, source exhausted or source is done
        // loop {
        // get next batch from source
        // if let Some(msgs) = try!(source.next()) {
        // source exhausted
        // if msgs.is_empty() {
        // self.sockw = true;
        // break;
        // } else {
        // try!(self.ovbuffer(msgs));
        //
        // if let Some(cnt) = try!(self.oflush()) {
        // trace!("on_writable(): written {} bytes", cnt);
        // } else {
        // would block
        // self.sockw = false;
        // break;
        // }
        // }
        // } else {
        // res = self.done(Done(None));
        // break;
        // }
        // }
        //
        // res
        //
        // } else {
        //
        // if !self.obuf.is_empty() {
        // try!(self.oflush());
        // } else {
        // self.sockw = true;
        // }
        //
        // Ok(())
        // }
    }
}
