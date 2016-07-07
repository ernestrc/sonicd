extern crate nix;
extern crate env_logger;
extern crate threadpool;
extern crate byteorder;
extern crate num_cpus;
extern crate bytes;
extern crate log4rs;
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
// use nix::c_int;
// use nix::fcntl::{O_NONBLOCK, O_CLOEXEC};
// use nix::errno::{EWOULDBLOCK, EINPROGRESS};
use nix::sys::socket::*;
use std::os::raw::c_int;
use std::os::unix::io::RawFd;

mod error;
mod handler;
use error::*;
use handler::*;

static VERSION: &'static str = env!("CARGO_PKG_VERSION");
static COMMIT: Option<&'static str> = option_env!("SONICD_COMMIT");

// TODO use select! in main loop for signals
// TODO https://github.com/BurntSushi/chan-signal
// use nix::sys::signal;
fn ssig() {}

macro_rules! perror {
    ($name:expr, $res:expr) => {{
        match $res {
            Ok(s) => s,
            Err(err) => {
                let err: Error = err.into();
                error!("{}: {}\n{:?}", $name, err, err.backtrace());
                continue;
            }
        }
    }}
}

lazy_static! {
    static ref NO_INTEREST: EpollEvent = {
        EpollEvent {
            events: EpollEventKind::empty(),
            data: 0,
        }
    };
}

fn ginterest(fd: RawFd) -> EpollEvent {
    EpollEvent {
        events: EPOLLET | EPOLLIN | EPOLLOUT | EPOLLERR | EPOLLRDHUP,
        data: fd as u64,
    }
}

// TODO proper config module with defaults and overrides
fn _main() -> Result<()> {
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

    let mut epfds: Vec<RawFd> = Vec::with_capacity(cpus);

    // epoll_wait with no timeout
    let loop_ms = -1 as isize;

    // TODO change epoll register to be edge triggered, 
    // TODO bench all epoll instances monitoring all files with edge triggered
    // vs one epoll instance per fd with level triggered
    // vs one epoll instance per fd with edge triggered
    for _ in 1..cpus {

        let epfd = try!(epoll_create());
        epfds.push(epfd);

        thread::spawn(move || {

            let mut handlers: HashMap<i32, Rc<Box<Handler>>> = HashMap::new();

            loop {

                let mut evts: Vec<EpollEvent> = Vec::with_capacity(max_conn);
                let dst = unsafe { slice::from_raw_parts_mut(evts.as_mut_ptr(), evts.capacity()) };
                let cnt = perror!("epoll_wait", epoll_wait(epfd, dst, loop_ms));
                unsafe { evts.set_len(cnt) }

                for ev in evts {

                    let clifd: c_int = ev.data as i32;


                    let events = ev.events;

                    //get handler
                    if events.contains(EPOLLRDHUP) {
                        shutdown(clifd, Shutdown::Both).unwrap();
                        debug!("successfully closed clifd {}", clifd);
                        handlers.remove(&clifd);

                        perror!("epoll_ctl", epoll_ctl(epfd, EpollOp::EpollCtlDel, clifd, &NO_INTEREST));
                        debug!("unregistered interests for {}", clifd);
                    } else {

                        let mut handler: &mut Rc<Box<Handler>> = match handlers.entry(clifd) {
                            Occupied(entry) => entry.into_mut(),
                            Vacant(entry) =>
                                entry.insert(Rc::new(Box::new(IoHandler::new(epfd, clifd)))),
                        };

                        if events.contains(EPOLLERR) {
                            error!("socket error on {}", clifd);
                        }

                        if events.contains(EPOLLIN) {
                            debug!("socket of {} is readable", clifd);
                            // TODO stop handler and notify client
                            perror!("on_readable()", handler.on_readable());
                        }

                        if events.contains(EPOLLOUT) {
                            debug!("socket of {} is writable", clifd);
                            perror!("on_writable()", handler.on_writable());
                        }
                    }
                }
            }

        });
    }

    debug!("created epoll instances: {:?}", epfds);

    let mut accepted: u64 = 0;

    let cepfd = try!(epoll_create());

    debug!("created connections epoll instance: {:?}", cepfd);

    let io_cpus = cpus - 1;

    let info = ginterest(srvfd);
    try!(epoll_ctl(cepfd, EpollOp::EpollCtlAdd, srvfd, &info));


    loop {
        let mut evts: Vec<EpollEvent> = Vec::with_capacity(max_conn);
        let dst = unsafe { slice::from_raw_parts_mut(evts.as_mut_ptr(), evts.capacity()) };
        // Wait for epoll events for at most timeout_ms milliseconds
        let cnt = perror!("epoll_wait", epoll_wait(cepfd, dst, loop_ms));
        unsafe { evts.set_len(cnt) }

        for _ in evts {

            let clifd = perror!("accept4" ,eagain!(accept4, "accept4", srvfd, sockf));
            debug!("accept4: accpeted new tcp client {}", &clifd);

            let info = ginterest(clifd);

            // round robin
            let next = (accepted % io_cpus as u64) as usize;

            let epfd: RawFd = *epfds.get(next).unwrap();

            debug!("assigned client to next {} epoll instance {}", &next, &epfd);

            perror!("epoll_ctl", epoll_ctl(epfd, EpollOp::EpollCtlAdd, clifd, &info));

            debug!("epoll_ctl: registered interests for {}", clifd);

            accepted += 1;
        }
    }
}

fn main() {

    ssig();

    env_logger::init().unwrap();

    let c = COMMIT.unwrap_or_else(|| "dev");
    info!("starting sonicd cache v.{} ({})", VERSION, c);

    _main().unwrap();

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
