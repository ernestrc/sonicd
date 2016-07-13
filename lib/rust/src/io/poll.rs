use std::os::unix::io::RawFd;
use std::{slice, fmt};
use std::boxed::Box;
use std::cell::RefCell;
use std::marker::PhantomData;

use nix::sys::epoll::*;
use nix::unistd;
use slab::Slab;

use error::{Result, Error};
use io::handler::Handler;
use io::controller::Controller;

static EVENTS_N: &'static usize = &1000;
static LOOP_MS: &'static isize = &-1;
static MAX_HANDLERS: &'static usize = &8192;

lazy_static! {
    static ref NO_INTEREST: EpollEvent = {
        EpollEvent {
            events: EpollEventKind::empty(),
            data: 0,
        }
    };
}

pub struct Epoll<C: Controller> {
    pub epfd: EpollFd,
    loop_ms: isize,
    controller: C,
    buf: Vec<EpollEvent>,
}

impl<C: Controller> Epoll<C> {

    pub fn from_fd(epfd: EpollFd, controller: C, loop_ms: isize) -> Epoll<C> {
        Epoll {
            epfd: epfd,
            loop_ms: loop_ms,
            controller: controller,
            buf: Vec::with_capacity(*EVENTS_N),
        }
    }

    pub fn new_with<F>(loop_ms: isize, newctl: F) -> Result<Epoll<C>>
        where F: FnOnce(EpollFd) -> C
    {

        let fd = try!(epoll_create());

        let epfd = EpollFd { fd: fd };

        let controller = newctl(epfd);

        Ok(Epoll {
            epfd: epfd,
            loop_ms: loop_ms,
            controller: controller,
            buf: Vec::with_capacity(*EVENTS_N),
        })
    }

    fn wait(&self, dst: &mut [EpollEvent]) -> Result<usize> {
        let cnt = try!(epoll_wait(self.epfd.fd, dst, self.loop_ms));
        Ok(cnt)
    }

    #[inline]
    fn run_once(&mut self) -> Result<()> {

        let dst = unsafe { slice::from_raw_parts_mut(self.buf.as_mut_ptr(), self.buf.capacity()) };
        let cnt = try!(self.wait(dst));
        unsafe { self.buf.set_len(cnt) }

        for ev in self.buf.iter() {
            perror!("controller.ready()", self.controller.ready(&ev));
        }
        Ok(())
    }

    pub fn run(&mut self) -> Result<()> {
        while !self.controller.is_terminated() {
            perror!("loop()", self.run_once());
        }
        Ok(())
    }
}

impl<C: Controller> Drop for Epoll<C> {
    fn drop(&mut self) {
        let _ = unistd::close(self.epfd.fd);
    }
}

#[derive(Debug, Copy, Clone)]
pub struct EpollFd {
    pub fd: RawFd,
}

unsafe impl Send for EpollFd {}

impl EpollFd {
    pub fn new(fd: RawFd) -> EpollFd {
        EpollFd { fd: fd }
    }
    fn ctl(&self, op: EpollOp, interest: &EpollEvent, fd: RawFd) -> Result<()> {
        try!(epoll_ctl(self.fd, op, fd, interest));
        Ok(())
    }

    pub fn reregister(&self, fd: RawFd, interest: &EpollEvent) -> Result<()> {
        try!(self.ctl(EpollOp::EpollCtlMod, interest, fd));
        Ok(())
    }

    pub fn register(&self, fd: RawFd, interest: &EpollEvent) -> Result<()> {
        try!(self.ctl(EpollOp::EpollCtlAdd, interest, fd));
        Ok(())
    }

    pub fn unregister(&self, fd: RawFd) -> Result<()> {
        try!(self.ctl(EpollOp::EpollCtlDel, &NO_INTEREST, fd));
        Ok(())
    }
}

impl fmt::Display for EpollFd {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self.fd)
    }
}

impl From<EpollFd> for i32 {
    fn from(epfd: EpollFd) -> i32 {
        epfd.fd
    }
}


#[cfg(test)]
mod tests {
    use {Controller, Result, Error};
    use io::*;
    use super::*;
    use nix::sys::epoll::*;
    use ::std::sync::mpsc::*;
    use nix::fcntl::{O_NONBLOCK, O_CLOEXEC};
    use nix::unistd;

    struct ChannelController {
        epfd: EpollFd,
        tx: Sender<EpollEvent>,
        terminated: bool
    }

    impl Controller for ChannelController {

        fn is_terminated(&self) -> bool {
            self.terminated
        }

        fn ready(&mut self, events: &EpollEvent) -> Result<()> {
            self.tx.send(*events).unwrap();
            Ok(())
        }
    }

    #[test]
    fn notify_controller() {

        let (tx, rx) = channel();

        let loop_ms = 10;

        let mut poll = Epoll::new_with(loop_ms, |epfd| {
                ChannelController {
                    epfd: epfd,
                    tx: tx,
                    terminated: false,
                }
            })
            .unwrap();

        let (rfd, wfd) = unistd::pipe2(O_NONBLOCK | O_CLOEXEC).unwrap();

        let interest = EpollEvent {
            events: EPOLLOUT | EPOLLIN,
            data: rfd as u64,
        };

        poll.epfd.register(rfd, &interest).unwrap();

        poll.run_once();

        let ev = rx.recv().unwrap();

        unistd::write(wfd, b"hello!").unwrap();

        assert!(ev.events.contains(EPOLLOUT));
        assert!(ev.data == rfd as u64);
    }
}
