use std::os::unix::io::RawFd;
use std::cell::RefCell;
use std::boxed::Box;

use nix::sys::epoll::*;
use slab::Slab;

use error::Result;
use io::controller::*;
use io::handler::*;
use io::poll::*;

pub struct SyncController<F: Factory> {
    epfd: EpollFd,
    handlers: Slab<RefCell<Box<Handler>>, usize>,
    factory: F,
}

impl<F: Factory> SyncController<F> {
    pub fn new(epfd: EpollFd, factory: F, capacity: usize) -> SyncController<F> {
        SyncController {
            epfd: epfd,
            handlers: Slab::new(capacity),
            factory: factory,
        }
    }

    fn notify(&mut self, fd: RawFd, id: usize, events: EpollEventKind) -> Result<()> {

        if events.contains(EPOLLRDHUP) || events.contains(EPOLLHUP) {

            debug!("socket {}: EPOLLHUP", fd);
            // droped when out of scope
            // handler should take care of cleaning resources
            // outside of interests on this epoll facility
            match self.handlers.remove(id) {
                Some(handler) => try!(handler.borrow_mut().on_close()),
                None => error!("on_close(): handler not found"),
            }

            try!(self.epfd.unregister(fd));

        } else {

            let handler = &mut self.handlers[id];

            if events.contains(EPOLLERR) {
                error!("socket {}: EPOLERR", fd);
                try!(handler.borrow_mut().on_error());
            }

            if events.contains(EPOLLIN) {
                debug!("socket {}: EPOLLIN", fd);
                try!(handler.borrow_mut().on_readable());
            }

            if events.contains(EPOLLOUT) {
                debug!("socket {}: EPOLLOUT", fd);
                try!(handler.borrow_mut().on_writable());
            }
        }
        Ok(())
    }
}

impl<F: Factory> Controller for SyncController<F> {
    fn ready(&mut self, ev: &EpollEvent) -> Result<()> {

        match self.factory.decode(ev.data) {

            Action::New(proto, fd) => {
                let id = try!(self.handlers
                    .insert(RefCell::new(self.factory.new(proto)))
                    .map_err(|_| "reached maximum number of handlers"));

                let action: Action<F> = Action::Notify(id, fd);

                // respects set of events of the original caller
                // but encodes Notify action in data
                let interest = EpollEvent {
                    events: ev.events | EPOLLET | EPOLLHUP | EPOLLRDHUP,
                    data: action.encode(),
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

pub enum Action<F: Factory> {
    New(F::Protocol, RawFd),
    Notify(HandlerId, RawFd),
}

impl<F: Factory> Action<F> {
    pub fn encode(self) -> u64 {
        match self {
            Action::Notify(id, fd) => ((fd as u64) << 31) | ((id as u64) << 15) | 0,
            Action::New(protocol, fd) => {
                let protocol: usize = protocol.into();
                ((fd as u64) << 31) | ((protocol as u64) << 15) | 1
            }
        }
    }
}

pub trait Factory
    where Self: Sized
{
    type Protocol: From<usize> + Into<usize>;

    fn new(&self, p: Self::Protocol) -> Box<Handler>;

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

    struct TestFactory;

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

    impl Factory for TestFactory {
        type Protocol = usize;
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
        let test = TestFactory;
        let data = Action::<TestFactory>::encode(Action::New(PROTO1, ::std::i32::MAX));

        if let Action::New(protocol, fd) = test.decode(data) {
            assert!(protocol == PROTO1);
            assert!(fd == ::std::i32::MAX);
        } else {
            panic!("action is not Action::New")
        }
    }

    #[test]
    fn decode_encode_notify_action() {
        let test = TestFactory;
        let data = Action::<TestFactory>::encode(Action::Notify(10110, 0));

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
