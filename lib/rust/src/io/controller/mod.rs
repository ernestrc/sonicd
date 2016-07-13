use std::os::unix::io::RawFd;
use std::marker::PhantomData;

use slab::Slab;
use nix::sys::epoll::*;

use io::handler::Handler;
use io::poll::{Epoll, EpollFd};
use error::Result;

pub mod sync;

pub trait Controller where Self: Sized {

    fn ready(&mut self, events: &EpollEvent) -> Result<()>;

}
