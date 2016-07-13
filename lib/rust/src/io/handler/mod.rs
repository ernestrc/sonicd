use std::os::unix::io::RawFd;

use nix::unistd;

use model::protocol::*;
use error::*;

pub mod tcp;
pub mod echo;

pub trait Handler {

    fn on_error(&mut self) -> Result<()>;

    fn on_close(&mut self) -> Result<()>;

    fn on_readable(&mut self) -> Result<()>;

    fn on_writable(&mut self) -> Result<()>;

}
