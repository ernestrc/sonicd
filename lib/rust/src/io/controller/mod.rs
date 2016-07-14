use nix::sys::epoll::*;

use io::poll::EpollFd;
use error::Result;

pub mod sync;
pub mod server;

pub trait Controller where Self: Sized {

    fn is_terminated(&self) -> bool;

    fn ready(&mut self, events: &EpollEvent) -> Result<()>;

}
