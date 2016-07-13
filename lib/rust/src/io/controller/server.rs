use nix::sys::epoll::*;
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

use {Error, Result};
use io::controller::*;
use io::controller::sync::*;
use io::poll::*;

pub struct Server<F>
    where F: Factory + Send
{
    epfds: Vec<EpollFd>,
    srvfd: RawFd,
    factory: F,
    sockf: SockFlag,
    cepfd: EpollFd,
    signals: SignalFd,
    terminated: bool,
    io_cpus: usize,
    accepted: u64,
}

impl<F> Server<F>
    where F: Factory + Send + 'static
{
    pub fn new<A: ToSocketAddrs>(cepfd: EpollFd,
                             addr: A,
                             factory: F,
                             max_conn: usize,
                             sockf: SockFlag,
                             protocol: i32)
                             -> Result<Server<F>> {

        let srvfd: RawFd =
            try!(socket(AddressFamily::Inet, SockType::Stream, sockf, protocol)) as i32;

        let a = SockAddr::Inet(InetAddr::from_std(&addr.to_socket_addrs()
            .unwrap()
            .next()
            .unwrap()));

        try!(eagain!(bind, "bind", srvfd, &a));
        debug!("bind: success fd {} to {}", srvfd, a);

        try!(eagain!(listen, "listen", srvfd, max_conn));
        debug!("listen: success fd {}: {}", srvfd, a);

        // spawn one thread + epoll instance per
        // cpu, and leave one to take care of
        // connections
        let cpus = ::num_cpus::get();
        let io_cpus = cpus - 1;

        debug!("detected {} (v)cpus", cpus);

        let mut epfds: Vec<EpollFd> = Vec::with_capacity(io_cpus);

        for _ in 0..io_cpus {

            let epfd = EpollFd::new(try!(epoll_create()));

            epfds.push(epfd);

            thread::spawn(move || {
                let controller = SyncController::new(epfd, factory, max_conn);
                let mut epoll = Epoll::from_fd(epfd, controller, -1);
                perror!("loop()", epoll.run());
            });
        }

        debug!("created epoll instances: {:?}", epfds);

        let ceinfo = EpollEvent {
            events: EPOLLET | EPOLLIN | EPOLLOUT | EPOLLERR,
            data: srvfd as u64,
        };

        try!(cepfd.register(srvfd, &ceinfo));

        let mut mask = SigSet::empty();
        mask.add(SIGINT).unwrap();
        mask.add(SIGTERM).unwrap();
        mask.thread_block().unwrap();

        let signals = SignalFd::with_flags(&mask, SFD_NONBLOCK).unwrap();
        let sigfd = signals.as_raw_fd();

        let siginfo = EpollEvent {
            events: EPOLLIN | EPOLLOUT | EPOLLHUP | EPOLLRDHUP | EPOLLERR,
            data: sigfd as u64,
        };

        try!(cepfd.register(sigfd, &siginfo));

        Ok(Server {
            sockf: sockf,
            srvfd: srvfd,
            cepfd: cepfd,
            factory: factory,
            epfds: epfds,
            io_cpus: io_cpus,
            signals: signals,
            accepted: 0,
            terminated: false,
        })
    }
}

impl<F: Factory> Controller for Server<F> {
    fn is_terminated(&self) -> bool {
        self.terminated
    }

    fn ready(&mut self, ev: &EpollEvent) -> Result<()> {

        match ev.data {
            fd if fd == self.srvfd as u64 => {
                // TODO!!! Keep track of what fd is assigned to what epoll instance
                // and register interests for closed connections to do better
                // load balancing
                match eagain!(accept4, "accept4", self.srvfd, self.sockf) {
                    Ok(clifd) => {
                        debug!("accept4: acceted new tcp client {}", &clifd);

                        // round robin
                        let next = (self.accepted % self.io_cpus as u64) as usize;

                        let epfd: EpollFd = *self.epfds.get(next).unwrap();

                        let info = EpollEvent {
                            events: EPOLLONESHOT | EPOLLIN | EPOLLOUT | EPOLLHUP | EPOLLRDHUP,
                            // protocol is 0, as we are bound to one address only
                            data: self.factory.encode(Action::New(0, clifd)),
                        };

                        debug!("assigned client {} to epoll instance {}", &clifd, &epfd);

                        perror!("epoll_ctl", epfd.register(clifd, &info));

                        debug!("epoll_ctl: registered interests for {}", clifd);

                        self.accepted += 1;
                    }
                    Err(e) => error!("accept4: {}", e),
                }

            }
            _ => {
                match self.signals.read_signal() {
                    Ok(Some(sig)) => {
                        warn!("received signal: {:?}", sig.ssi_signo);
                        perror!("unistd::close", unistd::close(self.srvfd));
                    }
                    Ok(None) => (),
                    Err(err) => (), // some error happend
                }
            }
        }

        Ok(())
    }
}
