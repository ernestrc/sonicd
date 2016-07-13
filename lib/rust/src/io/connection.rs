use std::os::unix::io::RawFd;
use std::io::{Write, Read};
use std::{fmt, io};

use nix::unistd;

use error::Error;

pub struct Connection {
    epfd: RawFd,
    pub fd: RawFd,
}

impl Connection {
    pub fn new(epfd: RawFd, fd: RawFd) -> Connection {
        Connection {
            epfd: epfd,
            fd: fd,
        }
    }
}

// TODO not sure if needed?
//impl Drop for Connection {
//    fn drop(&mut self) {
//        perror!("unisdtd::close", unistd::close(self.fd));
//        debug!("closed and unregistered {}", self);
//    }
//}

impl Read for Connection {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if let Some(b) = try!(eintr!(unistd::read, "unistd::read", self.fd, buf)) {
            Ok(b)
        } else {
            warn!("tried to write {} but socket not ready", self);
            Ok(0)
        }
    }
}

impl Write for Connection {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if let Some(b) = try!(eintr!(unistd::write, "unistd::write", self.fd, buf)) {
            Ok(b)
        } else {
            warn!("tried to read {} bytes from {} but socket not ready",
                  buf.len(),
                  self);
            Ok(0)
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        warn!("tried to flush {}", self);
        Ok(())
    }
}

impl fmt::Display for Connection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Connection({}/{})", self.fd, self.epfd)
    }
}
