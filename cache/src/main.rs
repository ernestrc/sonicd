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

use sonicd::io::poll::{Epoll, EpollFd};
use sonicd::io::controller::sync::{SyncController, Action};
use sonicd::io::handler::{Handler};
use sonicd::io::controller::server::*;
use sonicd::io::handler::echo::*;

mod error;

use error::*;

static VERSION: &'static str = env!("CARGO_PKG_VERSION");
static COMMIT: Option<&'static str> = option_env!("SONICD_COMMIT");

// TODO config module with defaults and overrides
fn run() -> Result<()> {
    let addr = ("127.0.0.1",10003);
    let max_conn = 500_000;
    let loop_ms = -1;

    let sockf = SOCK_CLOEXEC | SOCK_NONBLOCK;
    let protocol = 0;
    // setsockopt(srvfd, SOL_SOCKET, &1).unwrap();

    let mut srvp = try!(Epoll::new_with(loop_ms, |cepfd| {
        Server::new(cepfd, addr, EchoFactory, max_conn, sockf, protocol).unwrap()
    }));

    try!(srvp.run());
    Ok(())
}

fn main() {

    env_logger::init().unwrap();

    let c = COMMIT.unwrap_or_else(|| "dev");
    info!("starting sonicd cache v.{} ({})", VERSION, c);

    run().unwrap();

}
// ::ws::listen("127.0.0.1:9111", |out| WsHandler::new(out, count.clone())) .unwrap();
// use threadpool::ThreadPool;

// let n_workers = 4;
// let n_jobs = 8;
// let pool = ThreadPool::new(n_workers);

// let (tx, rx) = channel();

// for i in 0..n_jobs {
//    let tx = tx.clone();

//    pool.execute(move || {
//        tx.send(i).unwrap();
//    });
// }

// let count = Rc::new(Cell::new(0));

// assert_eq!(rx.iter().take(n_jobs).fold(0, |a, b| a + b), 28);
