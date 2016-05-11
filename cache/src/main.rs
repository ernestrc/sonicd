#![feature(custom_derive)]
extern crate nix;
extern crate env_logger;
extern crate threadpool;
extern crate ws;
#[macro_use]
extern crate log;
#[macro_use]
extern crate libsonicd;

use libsonicd::*;
use threadpool::ThreadPool;
use std::sync::mpsc::channel;
use std::option::Option;
use nix::sys::epoll::*;
use std::rc::Rc;
use std::cell::Cell;

static VERSION: &'static str = env!("CARGO_PKG_VERSION");
static COMMIT: Option<&'static str> = option_env!("SONICD_COMMIT");

fn main() {

    env_logger::init().unwrap();

    info!("starting sonicd cache v.{} ({})",
           VERSION,
           COMMIT.unwrap_or_else(|| "dev"));

    let n_workers = 4;
    let n_jobs = 8;
    let pool = ThreadPool::new(n_workers);

    let (tx, rx) = channel();

    for i in 0..n_jobs {
        let tx = tx.clone();

        pool.execute(move || {
            tx.send(i).unwrap();
        });
    }

    let count = Rc::new(Cell::new(0));

    assert_eq!(rx.iter().take(n_jobs).fold(0, |a, b| a + b), 28);

    ::ws::listen("127.0.0.1:9111", |out| WsHandler::new(out, count.clone())) .unwrap()


    // let epfd = epoll_create().unwrap();

    // let event = EpollEvent {
    //    events: EPOLLIN | EPOLLOUT,
    //    data: 0,
    // };

    // epoll_ctl(epfd, EpollOp::EpollCtlAdd, 0, &event).unwrap();

    // let mut events = vec!(event);

    // epoll_wait(epfd, &mut events, -1).unwrap();

    // for i in events {
    //    println!("{:?}", i.events);
    //    println!("{:?}", i.data);
    // }
}
