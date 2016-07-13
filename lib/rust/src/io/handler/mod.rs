use std::os::unix::io::RawFd;

use nix::unistd;

use model::protocol::*;
use error::*;

pub mod tcp;
pub mod server;

pub trait Handler {

    fn on_error(&mut self) -> Result<()>;

    fn on_close(&mut self) -> Result<()>;

    fn on_readable(&mut self) -> Result<()>;

    fn on_writable(&mut self) -> Result<()>;

}

pub struct EchoHandler {
    fd: RawFd,
    done: bool,
    buf: Vec<SonicMessage>,
}

impl EchoHandler {
    pub fn new(fd: RawFd) -> EchoHandler {
        EchoHandler {
            fd: fd,
            done: false,
            buf: Vec::new()
        }
    }
}

impl Handler for EchoHandler {

    fn on_error(&mut self) -> Result<()> {
        error!("on_error() EchoHandler");
        Ok(())
    }

    fn on_close(&mut self) -> Result<()> {
        if !self.done {
            Err("closed unexpectedly".into())
        } else {
            Ok(())
        }
    }
    fn on_readable(&mut self) -> Result<()> {
        trace!("on_readable()");
        let msg = try!(super::read_message(self.fd));
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
            let framed = try!(super::frame(&msg));

            match try!(super::write(self.fd, framed.as_slice())) {
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
