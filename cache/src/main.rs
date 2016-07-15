#![feature(split_off)]
extern crate nix;
extern crate env_logger;
extern crate threadpool;
extern crate byteorder;
extern crate num_cpus;
extern crate bytes;
#[macro_use] extern crate error_chain;
#[macro_use] extern crate lazy_static;
#[macro_use] extern crate log;
#[macro_use] extern crate sonicd;

use std::sync::mpsc::channel;
use std::option::Option;
use nix::sys::epoll::*;
use std::sync::mpsc;
use std::rc::Rc;
use std::boxed::Box;
use std::cell::Cell;
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::{thread, fmt, net, marker, slice};
use std::collections::HashMap;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::os::raw::c_int;
use std::os::unix::io::{RawFd, AsRawFd};
use std::io::Write;

use nix::sys::socket::*;
use nix::sys::signalfd::*;
use nix::sys::signal::{SIGINT, SIGTERM};
use nix::unistd;

use sonicd::io::*;
use sonicd::io::handler::echo::EchoHandler;

mod error;

use error::*;

static VERSION: &'static str = env!("CARGO_PKG_VERSION");
static COMMIT: Option<&'static str> = option_env!("SONICD_COMMIT");

#[derive(Clone, Copy)]
struct EchoProtocol;

impl EpollProtocol for EchoProtocol {

    type Protocol = usize;

    fn new(&self, _: usize, fd: RawFd) -> Box<Handler> {
        Box::new(EchoHandler::new(fd))
    }
}

struct EnvLogger;

impl LoggingBackend for EnvLogger {
    
    fn setup(&self, epfd: &EpollFd) -> ::sonicd::Result<()> {
        env_logger::init().unwrap();
        Ok(())
    }
}

impl Controller for EnvLogger {

    fn is_terminated(&self) -> bool {
        false
    }

    fn ready(&mut self, events: &EpollEvent) -> ::sonicd::Result<()> {
        unimplemented!()
    }
}

// TODO config module with defaults and overrides
fn main() {

    let c = COMMIT.unwrap_or_else(|| "dev");
    info!("starting sonicd cache v.{} ({})", VERSION, c);

    let config = SimpleMuxConfig::new(("127.0.0.1", 10003)).unwrap();

    Server::bind(SimpleMux::new(EchoProtocol, config).unwrap(), EnvLogger);

}
