use std::os::unix::io::RawFd;
use std::cell::RefCell;
use std::boxed::Box;

use nix::unistd;
use slab::Slab;

use error::{Result, Error};
use io::controller::*;
use io::handler::*;
use io::poll::*;

pub struct SyncController<F: EpollProtocol> {
    epfd: EpollFd,
    handlers: Slab<RefCell<Box<Handler>>, usize>,
    eproto: F,
    terminated: bool
}

impl<F: EpollProtocol> SyncController<F> {
    pub fn new(epfd: EpollFd, eproto: F, max_handlers: usize) -> SyncController<F> {
        trace!("new()");
        SyncController {
            epfd: epfd,
            handlers: Slab::new(max_handlers),
            eproto: eproto,
            terminated: false,
        }
    }

    fn notify(&mut self, fd: RawFd, id: usize, events: EpollEventKind) -> Result<()> {
        trace!("notify()");

        if events.contains(EPOLLRDHUP) || events.contains(EPOLLHUP) {

            trace!("socket's fd {}: EPOLLHUP", fd);
            match self.handlers.remove(id) {
                Some(handler) => perror!("on_close()", handler.borrow_mut().on_close()),
                None => error!("on_close(): handler not found"),
            }

            perror!("unregister()", self.epfd.unregister(fd));
            perror!("close()", unistd::close(fd));

        } else {

            let handler = &mut self.handlers[id];

            if events.contains(EPOLLERR) {
                trace!("socket's fd {}: EPOLERR", fd);
                perror!("on_error()", handler.borrow_mut().on_error());
            }

            if events.contains(EPOLLIN) {
                trace!("socket's fd {}: EPOLLIN", fd);
                perror!("on_readable()", handler.borrow_mut().on_readable());
            }

            if events.contains(EPOLLOUT) {
                trace!("socket's fd {}: EPOLLOUT", fd);
                perror!("on_writable()", handler.borrow_mut().on_writable());
            }
        }
        Ok(())
    }
}

impl<F: EpollProtocol> Controller for SyncController<F> {

    fn is_terminated(&self) -> bool {
        self.terminated
    }

    fn ready(&mut self, ev: &EpollEvent) -> Result<()> {
        trace!("ready()");

        match self.eproto.decode(ev.data) {

            Action::New(proto, fd) => {
                let id = try!(self.handlers
                    .insert(RefCell::new(self.eproto.new(proto, fd)))
                    .map_err(|_| "reached maximum number of handlers"));

                let action: Action<F> = Action::Notify(id, fd);

                let interest = EpollEvent {
                    events: EPOLLIN | EPOLLOUT | EPOLLET | EPOLLHUP | EPOLLRDHUP,
                    data: self.eproto.encode(action),
                };

                try!(self.epfd.reregister(fd, &interest));
                try!(self.notify(fd, id, ev.events));
            }

            Action::Notify(id, fd) => {
                try!(self.notify(fd, id, ev.events));
            }

        }
        Ok(())
    }
}

pub type HandlerId = usize;

pub enum Action<P: EpollProtocol> {
    New(P::Protocol, RawFd),
    Notify(HandlerId, RawFd),
}

pub trait EpollProtocol
    where Self: Sized + Send + Copy
{
    type Protocol: From<usize> + Into<usize>;

    fn new(&self, p: Self::Protocol, fd: RawFd) -> Box<Handler>;

    fn encode(&self, action: Action<Self>) -> u64 {
        match action {
            Action::Notify(id, fd) => ((fd as u64) << 31) | ((id as u64) << 15) | 0,
            Action::New(protocol, fd) => {
                let protocol: usize = protocol.into();
                ((fd as u64) << 31) | ((protocol as u64) << 15) | 1
            }
        }
    }

    fn decode(&self, data: u64) -> Action<Self> {
        let arg1 = ((data >> 15) & 0xffff) as usize;
        let fd = (data >> 31) as i32;
        match data & 0x7fff {
            0 => Action::Notify(arg1, fd),
            1 => Action::New(From::from(arg1), fd),
            a => panic!("unrecognized action: {}", a),
        }
    }
}

#[cfg(test)]
mod tests {
    use Result;
    use io::poll::*;
    use io::handler::Handler;
    use super::*;
    use nix::sys::epoll::*;

    struct TestEpollProtocol;

    const PROTO1: usize = 1;

    struct TestHandler {
        proto: usize,
        on_close: bool,
        on_error: bool,
        on_readable: bool,
        on_writable: bool,
    }

    impl Handler for TestHandler {
        fn on_error(&mut self) -> Result<()> {
            self.on_error = true;
            Ok(())
        }

        fn on_close(&mut self) -> Result<()> {
            self.on_close = true;
            Ok(())
        }

        fn on_readable(&mut self) -> Result<()> {
            self.on_readable = true;
            Ok(())
        }

        fn on_writable(&mut self) -> Result<()> {
            self.on_writable = true;
            Ok(())
        }
    }

    impl EpollProtocol for TestEpollProtocol {
        fn new(&self, p: usize) -> Box<Handler> {
            Box::new(TestHandler {
                proto: p,
                on_close: false,
                on_error: false,
                on_readable: false,
                on_writable: false,
            })
        }
    }

    #[test]
    fn decode_encode_new_action() {
        let test = TestEpollProtocol;
        let data = test.encode(Action::New(PROTO1, ::std::i32::MAX));

        if let Action::New(protocol, fd) = test.decode(data) {
            assert!(protocol == PROTO1);
            assert!(fd == ::std::i32::MAX);
        } else {
            panic!("action is not Action::New")
        }
    }

    #[test]
    fn decode_encode_notify_action() {
        let test = TestEpollProtocol;
        let data = Action::<TestEpollProtocol>::encode(Action::Notify(10110, 0));

        if let Action::Notify(id, fd) = test.decode(data) {
            assert!(id == 10110);
            assert!(fd == 0);
        } else {
            panic!("action is not Action::Notify")
        }
    }

    // TODO #[test]
    // TODO fn notify_events() {
    // TODO     let test = SyncController {

    // TODO     }
    // TODO }
}
