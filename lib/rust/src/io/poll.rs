use std::os::unix::io::RawFd;
use std::slice;
use std::boxed::Box;
use std::cell::RefCell;

use nix::sys::epoll::*;
use slab::Slab;

use error::{Result, Error};
use super::handler::{Handler, HandlerKind, EchoHandler};
use super::handler::tcp::TcpHandler;

static EVENTS_N: &'static usize = &10;
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

#[derive(Debug)]
pub struct Epoll {
    pub epfd: RawFd,
    loop_ms: isize,
    handlers: Slab<RefCell<Box<Handler>>, usize>,
}

pub enum Action {
    New(HandlerKind, RawFd),
    Notify(usize, RawFd),
}

impl Epoll {
    pub fn from_raw_fd(epfd: RawFd) -> Epoll {
        Epoll {
            epfd: epfd,
            loop_ms: *LOOP_MS,
            handlers: Slab::new_starting_at(0, *MAX_HANDLERS),
        }
    }

    pub fn new() -> Result<Epoll> {
        let epfd = try!(epoll_create());
        Ok(Self::from_raw_fd(epfd))
    }

    pub fn reregister(epfd: RawFd, fd: RawFd, interest: EpollEvent) -> Result<()> {
        try!(epoll_ctl(epfd, EpollOp::EpollCtlMod, fd, &interest));
        Ok(())
    }

    pub fn register(epfd: RawFd, fd: RawFd, interest: EpollEvent) -> Result<()> {
        try!(epoll_ctl(epfd, EpollOp::EpollCtlAdd, fd, &interest));
        Ok(())
    }

    pub fn unregister(epfd: RawFd, fd: RawFd) -> Result<()> {
        try!(epoll_ctl(epfd, EpollOp::EpollCtlDel, fd, &NO_INTEREST));
        Ok(())
    }

    pub fn decode(data: u64) -> Action {
        let arg1 = ((data >> 15) & 0xffff) as usize;
        let fd = (data >> 31) as i32;
        match data & 0x7fff {
            0 => Action::Notify(arg1, fd),
            1 => {
                match arg1 {
                    0 => Action::New(HandlerKind::Tcp, fd),
                    1 => Action::New(HandlerKind::Echo, fd),
                    k => panic!("unrecognized handler kind: {}", k),
                }
            }
            a => panic!("unrecognized action: {}", a),
        }
    }

    pub fn encode(action: Action) -> u64 {
        match action {
            Action::Notify(id, fd) => ((fd as u64) << 31) | ((id as u64) << 15) | 0,
            Action::New(HandlerKind::Tcp, fd) => ((fd as u64) << 31) | (0 << 15) | 1,
            Action::New(HandlerKind::Echo, fd) => ((fd as u64) << 31) | (1 << 15) | 1,
        }
    }

    #[inline]
    fn notify(&mut self, fd: RawFd, id: usize, events: EpollEventKind) {

        if events.contains(EPOLLRDHUP) || events.contains(EPOLLHUP) {

            debug!("socket {}: EPOLLHUP", fd);
            // droped when out of scope
            // handler should take care of cleaning resources
            // outside of interests on this epoll facility
            match self.handlers.remove(id) {
                Some(handler) => perror!("on_close()", handler.borrow_mut().on_close()),
                None => error!("on_close(): handler not found"),
            }
            perror!("unregister()", Self::unregister(self.epfd, fd));
        } else {

            let handler = &self.handlers[id];

            if events.contains(EPOLLERR) {
                error!("socket {}: EPOLERR", fd);
                perror!("on_error()", handler.borrow_mut().on_error());
            }

            if events.contains(EPOLLIN) {
                debug!("socket {}: EPOLLIN", fd);
                perror!("on_readable()", handler.borrow_mut().on_readable());
            }

            if events.contains(EPOLLOUT) {
                debug!("socket {}: EPOLLOUT", fd);
                perror!("on_writable()", handler.borrow_mut().on_writable());
            }
        }
    }

    #[inline]
    fn run_once(&mut self) {
        let epfd = self.epfd;
        // FIXME unsafe
        let mut evts: Vec<EpollEvent> = Vec::with_capacity(*EVENTS_N);
        let dst = unsafe { slice::from_raw_parts_mut(evts.as_mut_ptr(), evts.capacity()) };
        let cnt = perrorr!("epoll_wait", epoll_wait(epfd, dst, self.loop_ms));
        unsafe { evts.set_len(cnt) }

        for ev in evts {
            match Self::decode(ev.data) {
                Action::New(kind, fd) => {

                    let idm = self.handlers.insert_with(|id| {
                        RefCell::new(match kind {
                            HandlerKind::Echo => Box::new(EchoHandler::new(id, fd)),
                            HandlerKind::Tcp => Box::new(TcpHandler::new(id, epfd, fd)),
                        })
                    });

                    match idm {
                        Some(id) => {
                            // respects set of events of the original caller
                            // but encodes Notify action in data
                            let new = EpollEvent {
                                events: ev.events | EPOLLET | EPOLLHUP | EPOLLRDHUP,
                                data: Self::encode(Action::Notify(id, fd)),
                            };

                            perror!("reregister()", Self::reregister(epfd, fd, new));
                            self.notify(fd, id, ev.events)
                        }
                        None => error!("epoll {} reached maximum number of handlers", epfd),
                    }
                }
                Action::Notify(id, fd) => self.notify(fd, id, ev.events),
            }
        }
    }

    pub fn run(&mut self) {
        loop {
            self.run_once()
        }
    }
}


#[cfg(test)]
mod tests {
    use {Epoll, Action, HandlerKind};
    use io::*;
    use nix::sys::epoll::*;
    use nix::fcntl::{O_NONBLOCK, O_CLOEXEC};
    use nix::unistd::pipe2;

    #[test]
    fn decode_encode_new_action() {
        let data = Epoll::encode(Action::New(HandlerKind::Tcp, ::std::i32::MAX));

        if let Action::New(kind, fd) = Epoll::decode(data) {
            assert!(kind == HandlerKind::Tcp);
            assert!(fd == ::std::i32::MAX);
        } else {
            panic!("action is not Action::New")
        }
    }

    #[test]
    fn decode_encode_notify_action() {
        let data = Epoll::encode(Action::Notify(10110, 0));

        if let Action::Notify(id, fd) = Epoll::decode(data) {
            assert!(id == 10110);
            assert!(fd == 0);
        } else {
            panic!("action is not Action::Notify")
        }
    }

    // #[test]
    // fn notify_handler() {
    // let mut poll = Epoll::new().unwrap();
    // let (rfd, wfd) = pipe2(O_NONBLOCK | O_CLOEXEC).unwrap();
    //
    // let interest = EpollEvent {
    // events: EPOLLET | EPOLLOUT,
    // data: Epoll::encode(Action::New(HandlerKind::Echo, rfd)),
    // };
    //
    // Epoll::register(poll.epfd, rfd, interest).unwrap();
    //
    // poll.run_once();
    // read_message(rfd);
    // }
}
