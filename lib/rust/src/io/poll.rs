use std::os::unix::io::RawFd;
use std::io::{Cursor, Write, Read};
use std::{fmt, slice};
use std::collections::HashMap;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::rc::{Rc, Weak};
use std::os::raw::c_int;
use std::boxed::Box;
use std::cell::{RefCell, RefMut};
use std::borrow::BorrowMut;
use std::ops::DerefMut;
use std::iter::Iterator;
use std::clone::Clone;

use byteorder::{BigEndian, ByteOrder};
use bytes::{MutBuf, MutByteBuf, ByteBuf, Buf};
use nix::unistd;
use nix::sys::epoll::*;
use slab::Slab;

use error::{Result, ErrorKind, Error};
use super::{NO_INTEREST, read, write};
use super::handler::{Handler, IoHandler};
use source::{Source, EmptySource};

static EVENTS_N: &'static usize = &10;
static LOOP_MS: &'static isize = &-1;
static MAX_HANDLERS: &'static usize = &1024;

/// Abstraction over an epoll facility.
///
/// Events can be registered directly with this structure's registry
/// or from another context with epoll_ctl. It is recommended that
/// if you do this, you use EPOLLONESHOT so that it doesn't need to re-register
///
/// When one of the events received is from an unrecognized file descriptor, 
/// a new default handler will be allocated and stored in the registry's slab,
/// and this poll will re-subscribe
///
/// HandlerFactory
/// will be used to create an instance of a Handler.
///
/// This mechanism has the following advantatges:
/// - No synchronization between Poll instances by using `epoll_ctl` and the factory handler.
/// - Registering arbitrary implementors of `Handler` in scenarios where a specialized
/// handler is needed.
pub struct Poll {
    pub epfd: RawFd,
    loop_ms: isize,
    registry: Rc<Registry>,
}

pub struct Token(usize);

pub struct Registry {
    pub epfd: RawFd,
    handlers: RefCell<Slab<RefCell<Box<Handler>>, Token>>
}

impl Registry {
    pub fn new(epfd: RawFd, slab_capacity: usize) -> Registry {
        Registry {
            epfd: epfd,
            handlers: RefCell::new(Slab::new_starting_at(0, slab_capacity)),
        }
    }

    pub fn remove(&self, fd: &RawFd) -> Option<RefCell<Box<Handler>>> {
        self.handlers.borrow_mut().remove(*fd as usize)
    }

    pub fn contains(&self, fd: &RawFd) -> bool {
        self.handlers.borrow().contains(*fd as usize)
    }

    pub fn register(&self, fd: RawFd, handler: Box<Handler>, interest: EpollEvent) -> Result<()> {
        let mut handlers = self.handlers.borrow_mut();
        if handlers.contains(fd as usize) {
            Err(format!("there is already one handler registered for {}", fd).into())
        } else {
            try!(epoll_ctl(self.epfd, EpollOp::EpollCtlAdd, fd, &interest));
            handlers.insert(fd, RefCell::new(handler));
            Ok(())
        }
    }

    pub fn unregister(&self, fd: RawFd) {
        self.handlers.borrow_mut().remove(&fd);

        perror!("epoll_ctl",
                epoll_ctl(self.epfd, EpollOp::EpollCtlDel, fd, &NO_INTEREST));
        debug!("unregistered interests for {}", fd);
    }
}

impl Poll {
    pub fn from_raw_fd(epfd: RawFd) -> Poll {
        let registry = Registry::new(epfd);
        Poll {
            epfd: epfd,
            loop_ms: *LOOP_MS,
            registry: Rc::new(registry),
        }
    }

    pub fn new() -> Result<Poll> {
        let epfd = try!(epoll_create());
        Ok(Self::from_raw_fd(epfd))
    }

    pub fn poll(&mut self) {
        loop {

            // FIXME unsafe
            let mut evts: Vec<EpollEvent> = Vec::with_capacity(*EVENTS_N);
            let dst = unsafe { slice::from_raw_parts_mut(evts.as_mut_ptr(), evts.capacity()) };
            let cnt = perror_continue!("epoll_wait", epoll_wait(self.epfd, dst, self.loop_ms));
            unsafe { evts.set_len(cnt) }

            for ev in evts {

                let clifd: c_int = ev.data as i32;
                let events = ev.events;

                // get handler
                if events.contains(EPOLLRDHUP) {

                    if let Some(handler) = self.registry.borrow_mut().remove(&clifd) {
                        perror!("on_close()", handler.borrow_mut().on_close());
                    } else {
                        error!("on_close(): handler not found");
                    }

                    perror!("epoll_ctl",
                            epoll_ctl(self.epfd, EpollOp::EpollCtlDel, clifd, &NO_INTEREST));
                    debug!("unregistered interests for {}", clifd);
                } else {

                    let handler: &RefCell<Box<Handler>> = self.registry
                        .borrow_mut()
                        .get_handler(&clifd, || unimplemented!());

                    if events.contains(EPOLLERR) {
                        error!("socket error on {}", clifd);
                        perror!("on_error()", handler.borrow_mut().on_error());
                    }

                    if events.contains(EPOLLIN) {
                        debug!("socket of {} is readable", clifd);
                        perror!("on_readable()", handler.borrow_mut().on_readable());
                    }

                    if events.contains(EPOLLOUT) {
                        debug!("socket of {} is writable", clifd);
                        perror!("on_writable()", handler.borrow_mut().on_writable());
                    }
                }
            }
        }
    }
}
