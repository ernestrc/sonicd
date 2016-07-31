use std::net::ToSocketAddrs;
use std::thread;
use std::os::unix::io::{RawFd, AsRawFd};

use nix::sys::socket::*;
use nix::sys::signalfd::*;
use nix::sys::signal::{SIGINT, SIGTERM};
use nix::unistd;

use {Error, Result};
use io::controller::*;
use io::controller::sync::*;
use io::poll::*;
use super::logging::LoggingBackend;

/// Server facade.
///
/// Takes care of signals and logging, and delegates bind/listen/accept
/// logic to `ServerImpl`.
pub struct Server<L: LoggingBackend> {
    epfd: EpollFd,
    sigfd: SignalFd,
    // to speed up `ready()`
    _sigfd: u64,
    lb: L,
    terminated: bool,
}

unsafe impl <L: LoggingBackend + Send> Send for Server<L> {}

impl <L> Server<L>
where L: LoggingBackend + Send {

    /// Instantiates new Server with the given implementation
    /// and logging backend
    pub fn bind<I: ServerImpl + Send + 'static>(im: I, lb: L) -> Result<()> {

        trace!("bind()");

        // signal mask to share across threads
        let mut mask = SigSet::empty();
        mask.add(SIGINT).unwrap();
        mask.add(SIGTERM).unwrap();

        let loop_ms = im.get_loop_ms();

        thread::spawn(move || {
            try!(mask.thread_block());
            //run impl's I/O event loop(s)
            im.bind(mask)
        });

        // add the set of signals to the signal mask
        // of the main thread
        try!(mask.thread_block());

        let sigfd = try!(SignalFd::with_flags(&mask, SFD_NONBLOCK));
        let fd = sigfd.as_raw_fd();

        let mut epoll = try!(Epoll::new_with(loop_ms, |epfd| {

            //delegate logging registering to logging backend
            let log = lb.setup(&epfd).unwrap();

            ::log::set_logger(|max_log_level| {
                max_log_level.set(lb.level().to_log_level_filter());
                log
            }).unwrap();

            Server {
                epfd: epfd,
                sigfd: sigfd,
                _sigfd: fd as u64,
                lb: lb,
                terminated: false,
            }
        }));

        let siginfo = EpollEvent {
            events: EPOLLIN | EPOLLOUT | EPOLLHUP | EPOLLRDHUP | EPOLLERR,
            data: fd as u64,
        };

        //register signalfd with epfd
        try!(epoll.epfd.register(fd, &siginfo));

        //run aux event loop
        epoll.run()
    }
}

impl <L: LoggingBackend> Drop for Server<L> {
    fn drop(&mut self) {
        // signalfd is closed by the SignalFd struct
        // and epfd is closed by EpollFd
        // so nothing to do here
    }
}

impl <L: LoggingBackend> Controller for Server<L> {
    fn is_terminated(&self) -> bool {
        self.terminated
    }

    fn ready(&mut self, ev: &EpollEvent) -> Result<()> {
        trace!("ready(): {:?}: {:?}", ev.data, ev.events);
        if ev.data == self._sigfd {
            match self.sigfd.read_signal() {
                Ok(Some(sig)) => {
                    // stop server's event loop, as the signal mask
                    // contains SIGINT and SIGTERM
                    warn!("received signal {:?}. Shutting down..", sig.ssi_signo);
                    //terminate server aux loop
                    self.terminated = true;
                }
                Ok(None) => debug!("read_signal(): not ready to read"),
                Err(err) => error!("read_signal(): {}", err),
            }
            Ok(())
        } else {
            // delegate events to logging backend
            self.lb.ready(ev)
        }
    }
}


pub trait ServerImpl {

    fn get_loop_ms(&self) -> isize;

    fn bind(self, mask: SigSet) -> Result<()>;
}

/// Simple server implementation that creates one AF_INET/SOCK_STREAM socket and uses it to bind/listen
/// at the specified address. It instantiates one epoll to accept new connections
/// and one instance per cpu left to perform I/O.
///
/// Events are handled synchronously TODO explain
///
/// New connections are load balanced from the connections epoll to the rest in a round-robin fashion.
pub struct SimpleMux<P: EpollProtocol> {
    srvfd: RawFd,
    cepfd: EpollFd,
    sockaddr: SockAddr,
    eproto: P,
    max_conn: usize,
    io_threads: usize,
    loop_ms: isize,
    accepted: u64,
    terminated: bool,
    epfds: Vec<EpollFd>,
}

pub struct SimpleMuxConfig {
    max_conn: usize,
    io_threads: usize,
    sockaddr: SockAddr,
    loop_ms: isize,
}

impl SimpleMuxConfig {

    pub fn new<A: ToSocketAddrs>(addr: A) -> Result<SimpleMuxConfig> {
        let inet = try!(addr.to_socket_addrs().unwrap().next().ok_or("could not parse sockaddr"));
        let sockaddr = SockAddr::Inet(InetAddr::from_std(&inet));

        // TODO provide good default depending on the number of I/O threads
        let max_conn = 50_000;
        let cpus = ::num_cpus::get();
        let io_threads = cpus - 2; //1 for logging/signals + 1 for connection accepting + n

        Ok(SimpleMuxConfig {
            sockaddr: sockaddr,
            max_conn: max_conn,
            io_threads: io_threads,
            loop_ms: -1,
        })
    }

    pub fn max_conn(self, max_conn: usize) -> SimpleMuxConfig {
        SimpleMuxConfig { max_conn: max_conn, ..self }
    }

    pub fn io_threads(self, io_threads: usize) -> SimpleMuxConfig {
        SimpleMuxConfig { io_threads: io_threads, ..self }
    }

    pub fn loop_ms(self, loop_ms: isize) -> SimpleMuxConfig {
        SimpleMuxConfig { loop_ms: loop_ms, ..self }
    }
}

impl<P: EpollProtocol> SimpleMux<P> {
    pub fn new(eproto: P, config: SimpleMuxConfig) -> Result<SimpleMux<P>> {

        let SimpleMuxConfig { io_threads, max_conn, sockaddr, loop_ms } = config;

        // create connections epoll
        let fd = try!(epoll_create());

        let cepfd = EpollFd { fd: fd };

        // create socket
        let srvfd = try!(socket(AddressFamily::Inet, SockType::Stream, SOCK_NONBLOCK | SOCK_CLOEXEC, 0)) as i32;

        Ok(SimpleMux {
            eproto: eproto,
            sockaddr: sockaddr,
            cepfd: cepfd,
            srvfd: srvfd,
            max_conn: max_conn,
            io_threads: io_threads,
            loop_ms: loop_ms,
            terminated: false,
            accepted: 0,
            epfds: Vec::with_capacity(io_threads),
        })
    }
}

impl <P: EpollProtocol> Controller for SimpleMux<P> {

    fn is_terminated(&self) -> bool {
        self.terminated
    }

    fn ready(&mut self, _: &EpollEvent) -> Result<()> {
        trace!("new()");

        //only monitoring events from srvfd
        match eintr!(accept4, "accept4", self.srvfd, SOCK_NONBLOCK | SOCK_CLOEXEC) {
            Ok(Some(clifd)) => {

                trace!("accept4: acceted new tcp client {}", &clifd);

                // round robin
                let next = (self.accepted % self.io_threads as u64) as usize;

                let epfd: EpollFd = *self.epfds.get(next).unwrap();

                let info = EpollEvent {
                    events: EPOLLONESHOT | EPOLLIN | EPOLLOUT | EPOLLHUP | EPOLLRDHUP,
                    // start with EpollProtocol handler 0
                    data: self.eproto.encode(Action::New(0.into(), clifd)),
                };

                debug!("assigned accepted client {} to epoll instance {}", &clifd, &epfd);

                perror!("epoll_ctl", epfd.register(clifd, &info));

                trace!("epoll_ctl: registered interests for {}", clifd);

                self.accepted += 1;
            }
            Ok(None) => debug!("accept4: socket not ready"),
            Err(e) => error!("accept4: {}", e),
        }
        Ok(())
    }
}

impl<P> ServerImpl for SimpleMux<P>
where P: EpollProtocol + Sync + 'static
{
    fn get_loop_ms(&self) -> isize {
        self.loop_ms
    }

    fn bind(mut self, mask: SigSet) -> Result<()> {
        trace!("bind()");

        try!(eagain!(bind, "bind", self.srvfd, &self.sockaddr));
        info!("bind: fd {} to {}", self.srvfd, self.sockaddr);

        try!(eagain!(listen, "listen", self.srvfd, self.max_conn));
        info!("listen: fd {} with max connections: {}", self.srvfd, self.max_conn);

        let ceinfo = EpollEvent {
            events: EPOLLIN | EPOLLOUT | EPOLLERR,
            data: self.srvfd as u64,
        };

        try!(self.cepfd.register(self.srvfd, &ceinfo));

        let max_conn = self.max_conn;
        let io_threads = self.io_threads;
        let eproto = self.eproto;
        let loop_ms = self.loop_ms;
        let cepfd = self.cepfd;

        for _ in 0..io_threads {

            let epfd = EpollFd::new(try!(epoll_create()));

            self.epfds.push(epfd);

            thread::spawn(move || {
                // add the set of signals to the signal mask for all threads
                // otherwise signalfd will not work properlys
                mask.thread_block().unwrap();
                // max number of handlers (connections)
                // per controller
                let maxh = max_conn / io_threads;
                let controller = SyncController::new(epfd, eproto, maxh);

                let mut epoll = Epoll::from_fd(epfd, controller, -1);

                perror!("epoll.run()", epoll.run());
            });
        }

        debug!("created {} I/O epoll instances", self.io_threads);


        //consume itself
        let mut epoll = Epoll::from_fd(cepfd, self, loop_ms);

        //run accept event loop
        epoll.run()
    }

}


impl<P> Drop for SimpleMux<P>
where P: EpollProtocol
{
    fn drop(&mut self) {
        let _ = unistd::close(self.srvfd).unwrap();
    }
}
