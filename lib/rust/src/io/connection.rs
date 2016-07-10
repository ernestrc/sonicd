use std::os::unix::io::RawFd;
use std::io::{Write, Read};
use std::{fmt, io};

use nix::unistd;
use nix::sys::epoll::*;

use super::NO_INTEREST;
use error::Error;

pub struct Connection {
    epfd: RawFd,
    fd: RawFd,
}

impl Connection {
    pub fn new(epfd: RawFd, fd: RawFd) -> Connection {
        Connection {
            epfd: epfd,
            fd: fd,
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        perror!("unisdtd::close", unistd::close(self.fd));
        perror!("epoll_ctl",
                epoll_ctl(self.epfd, EpollOp::EpollCtlDel, self.fd, &NO_INTEREST));
        debug!("closed and unregistered {}", self);
    }
}

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
