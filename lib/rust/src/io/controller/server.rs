use std::net::ToSocketAddrs;
use std::{thread, fmt, net, marker, slice};
use std::os::unix::io::{RawFd, AsRawFd};
use std::io::Write;

use nix::sys::epoll::*;
use nix::sys::socket::*;
use nix::sys::signalfd::*;
use nix::sys::signal::{SIGINT, SIGTERM};
use nix::unistd;

use {Error, Result};
use io::controller::*;
use io::controller::sync::*;
use io::poll::*;

/// Server facade
pub struct Server<L>
where L: ServerImpl
{
    im: L,
    sigfd: RawFd,
    signals: SignalFd,
    terminated: bool,
}

impl<L> Server<L>
where L: ServerImpl
{
    pub fn bind<A: ToSocketAddrs>(addr: A, im: L) -> Result<Server<L>> {

        let mask = try!(im.bind(addr));

        // add the set of signals to the signal mask
        // of the main thread
        mask.thread_block().unwrap();

        let signals = SignalFd::with_flags(&mask, SFD_NONBLOCK).unwrap();
        let sigfd = signals.as_raw_fd();

        let siginfo = EpollEvent {
            events: EPOLLIN | EPOLLOUT | EPOLLHUP | EPOLLRDHUP | EPOLLERR,
            data: sigfd as u64,
        };

        try!(im.cepfd.register(sigfd, &siginfo));

        debug!("registered to signalfd {}", sigfd);

        Ok(Server {
            im: im,
            sigfd: sigfd,
            signals: signals,
            terminated: false,
        })
    }
}

impl<I> Drop for Server<I>
where I: ServerImpl
{
    fn drop(&mut self) {
        let _ = unistd::close(self.sigfd).unwrap();
    }
}

impl<I> Controller for Server<I>
where I: ServerImpl
{
    fn is_terminated(&self) -> bool {
        self.terminated
    }

    fn ready(&mut self, ev: &EpollEvent) -> Result<()> {

        match ev.data {
            fd if fd == self.srvfd as u64 => {
                match eintr!(accept4, "accept4", self.srvfd, self.sockf) {
                    Ok(Some(clifd)) => try!(self.im.on_accept(srvfd, clifd)),
                    Ok(None) => debug!("accept4: socket not ready"),
                    Err(e) => error!("accept4: {}", e),
                }
            }
            _ => {
                match self.signals.read_signal() {
                    Ok(Some(sig)) => {
                        self.terminated = try!(self.im.on_signal(sig));
                    }
                    Ok(None) => debug!("read_signal(): not ready to read"),
                    Err(err) => error!("read_signal(): {}", err),
                }
            }
        }

        Ok(())
    }
}


pub trait ServerImpl 
//FIXME
where Self: Sized {

//FIXME not sure how to design API
//FIXME
//FIXME
//FIXME
    /// In order for `Server` to perform its duties and
    /// start spinning, we need to supply it an epoll instance.
    fn get_epoll(&self) -> Epoll<Server<Self>>;

    /// Bind to the specified address and return a set of signals
    /// to be monitored
    fn bind<A: ToSocketAddrs>(&self, addr: A) -> Result<SigSet>;

    /// handle new accepted connection from socket `srvfd`
    fn on_accept(&self, srvfd: RawFd, clifd: RawFd) -> Result<()>;

    /// handle signal described by `sig`
    fn on_signal(&self, sig: siginfo) -> Result<bool>;
}

/// Simple server implementation that creates one AF_INET/SOCK_STREAM socket and uses it to bind/listen
/// at the specified address.
///
/// It creates one epoll instance to accept new connections and one instance per cpu left to perform I/O.
///
/// New connections are load balanced from the connections epoll to the rest in a round-robin fashion.
pub struct SimpleMux<P: EpollProtocol> {
    srvfd: RawFd,
    cepfd: EpollFd,
    eproto: P,
    max_conn: usize,
    io_threads: usize,
    accepted: u64,
    epfds: Vec<EpollFd>,
}

impl<P: EpollProtocol> SimpleMux<P> {
    fn new(eproto: P, max_conn: usize) -> Result<SimpleMux<P>> {
        let cpus = ::num_cpus::get();
        debug!("machine has {} (v)cpus", cpus);

        let io_threads = cpus - 1;

        // connections epoll
        let fd = try!(epoll_create());

        let cepfd = EpollFd { fd: fd };

        // create 1 server socket
        let srvfd = try!(socket(AddressFamily::Inet, SockType::Stream, SOCK_NONBLOCK | SOCK_CLOEXEC, 0)) as i32;

        SimpleMux {
            eproto: eproto,
            cepfd: cepfd,
            srvfd: srvfd,
            max_conn: max_conn,
            io_threads: io_threads,
            accepted: 0,
            epfds: Vec::with_capacity(io_threads),
        }
    }
}

impl<P> ServerImpl for SimpleMux<P>
where P: EpollProtocol + Send
{
    fn on_accept(&self, srvfd: RawFd, clifd: RawFd) -> Result<()> {

        debug!("accept4: acceted new tcp client {}", &clifd);

        // round robin
        let next = (self.accepted % self.io_threads as u64) as usize;

        let epfd: EpollFd = *self.epfds.get(next).unwrap();

        let info = EpollEvent {
            events: EPOLLONESHOT | EPOLLIN | EPOLLOUT | EPOLLHUP | EPOLLRDHUP,
            data: self.eproto.encode(Action::New(0, clifd)),
        };

        debug!("assigned client {} to epoll instance {}", &clifd, &epfd);

        perror!("epoll_ctl", epfd.register(clifd, &info));

        debug!("epoll_ctl: registered interests for {}", clifd);

        self.accepted += 1;
    }

    fn bind<A: ToSocketAddrs>(&self, addr: A) -> Result<SigSet> {

        let a =
            SockAddr::Inet(InetAddr::from_std(&addr.to_socket_addrs().unwrap().next().unwrap()));

        try!(eagain!(bind, "bind", self.srvfd, &a));
        info!("bind: success fd {} to {}", self.srvfd, a);

        try!(eagain!(listen, "listen", self.srvfd, self.max_conn));
        info!("listen: success fd {}: {}", self.srvfd, a);

        let ceinfo = EpollEvent {
            events: EPOLLET | EPOLLIN | EPOLLOUT | EPOLLERR,
            data: self.srvfd as u64,
        };

        try!(self.cepfd.register(self.srvfd, &ceinfo));

        // signal mask to share across threads
        let mut mask = SigSet::empty();
        mask.add(SIGINT).unwrap();
        mask.add(SIGTERM).unwrap();
        try!(mask.thread_set_mask());

        // max number of handlers (connections)
        // per controller
        let maxh = self.max_conn / self.io_threads;

        for _ in 0..self.io_threads {

            let epfd = EpollFd::new(try!(epoll_create()));

            self.epfds.push(epfd);

            thread::spawn(move || {
                // add the set of signals to the signal mask for all threads
                // otherwise signalfd will not work properlys
                mask.thread_block().unwrap();
                let controller = SyncController::new(epfd, self.eproto, maxh);
                let mut epoll = Epoll::from_fd(epfd, controller, -1);
                perror!("loop()", epoll.run());
            });
        }
        debug!("created {} I/O epoll instances", self.io_threads);
        Ok(mask)
    }

    fn on_signal(&self, sig: siginfo) -> Result<bool> {
        // close server socket as the mask registered
        // contains only SIGINT and SIGTERM
        warn!("received signal {:?}. Shutting down..", sig.ssi_signo);
        Ok(true)
    }
}


impl<P> Drop for SimpleMux<P>
where P: EpollProtocol
{
    fn drop(&mut self) {
        let _ = unistd::close(self.srvfd).unwrap();
    }
}
