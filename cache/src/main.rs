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
use sonicd::io::handler::Handler;

mod error;
mod handler;

use handler::*;
use error::*;

static VERSION: &'static str = env!("CARGO_PKG_VERSION");
static COMMIT: Option<&'static str> = option_env!("SONICD_COMMIT");

// TODO config module with defaults and overrides
fn run() -> Result<RawFd> {
    let addr = SockAddr::Inet(InetAddr::from_std(&("127.0.0.1:10003".parse().unwrap())));
    let max_conn = 1024;

    let sockf = SOCK_CLOEXEC | SOCK_NONBLOCK;
    let protocol = 0;
    let srvfd = socket(AddressFamily::Inet, SockType::Stream, sockf, protocol).unwrap() as i32;
    // setsockopt(srvfd, SOL_SOCKET, &1).unwrap();

    try!(eagain!(bind, "bind", srvfd, &addr));
    debug!("bind: success fd {} to {}", srvfd, addr);

    try!(eagain!(listen, "listen", srvfd, max_conn));
    debug!("listen: success fd {}: {}", srvfd, addr);

    let cpus = num_cpus::get();
    debug!("detected {} (v)cpus", cpus);

    let mut epfds: Vec<EpollFd> = Vec::with_capacity(cpus);

    for _ in 1..cpus {

        let fd = try!(epoll_create());

        epfds.push(EpollFd::new(fd));

        thread::spawn(move || {
            let mut epoll = Epoll::new(-1, |epfd| {
                SyncController::new(epfd, TestFactory, 1000)
            }).unwrap();

            epoll.run()
        });
    }

    debug!("created epoll instances: {:?}", epfds);

    // TODO refactor to use Server handler
    // which should also handle signals using signalfd
    let cepfd = try!(epoll_create());

    debug!("created connections epoll instance: {:?}", cepfd);

    let ceinfo = EpollEvent {
        events: EPOLLET | EPOLLIN | EPOLLOUT,
        data: srvfd as u64,
    };

    try!(epoll_ctl(cepfd, EpollOp::EpollCtlAdd, srvfd, &ceinfo));


    let mut mask = SigSet::empty();
    mask.add(SIGINT).unwrap();
    mask.add(SIGTERM).unwrap();
    mask.thread_block().unwrap();

    let mut signals = SignalFd::with_flags(&mask, SFD_NONBLOCK).unwrap();
    let sfd = signals.as_raw_fd();

    let siginfo = EpollEvent {
        events: EPOLLIN | EPOLLOUT,
        data: sfd as u64,
    };

    try!(epoll_ctl(cepfd, EpollOp::EpollCtlAdd, sfd, &siginfo));

    let io_cpus = cpus - 1;
    let mut accepted: u64 = 0;

    loop {
        let mut evts: Vec<EpollEvent> = Vec::with_capacity(max_conn);
        let dst = unsafe { slice::from_raw_parts_mut(evts.as_mut_ptr(), evts.capacity()) };
        // Wait for epoll events for at most timeout_ms milliseconds
        if let Ok(Some(cnt)) = eintr!(epoll_wait, "epoll_wait", cepfd, dst, -1) {
            unsafe { evts.set_len(cnt) }
            for ev in evts {

                match ev.data {
                    fd if fd == srvfd as u64 => {
                        // TODO!!! Keep track of what fd is assigned to what epoll instance
                        // and register interests for closed connections to do better
                        // load balancing
                        match eagain!(accept4, "accept4", srvfd, sockf) {
                            Ok(clifd) => {
                                debug!("accept4: acceted new tcp client {}", &clifd);


                                // round robin
                                let next = (accepted % io_cpus as u64) as usize;

                                let epfd: EpollFd = *epfds.get(next).unwrap();

                                let info = EpollEvent {
                                    events: EPOLLONESHOT | EPOLLIN | EPOLLOUT | EPOLLHUP | EPOLLRDHUP,
                                    data: Action::<TestFactory>::encode(Action::New(0, ::std::i32::MAX))
                                };

                                debug!("assigned client to next {} epoll instance {}", &next, &epfd);

                                perror!("epoll_ctl", epfd.register(clifd, &info));

                                debug!("epoll_ctl: registered interests for {}", clifd);

                                accepted += 1;
                            },
                            Err(e) => error!("accept4: {}", e),
                        }

                    },
                    signal => {
                        match signals.read_signal() {
                            Ok(Some(sig)) => {
                                std::io::stderr().write(format!("received signal: {:?}", sig.ssi_signo).as_bytes());
                                std::io::stderr().flush();
                                perror!("unisdtd::close", unistd::close(srvfd));
                                std::process::exit(1);
                            },
                            Ok(None) => (),
                            Err(err) => (), // some error happend
                        }
                    }
                }
            }
        }
    }
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
