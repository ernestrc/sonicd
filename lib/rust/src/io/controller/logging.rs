use std::os::unix::io::RawFd;
use std::cell::RefCell;
use std::slice;
use std::sync::mpsc::{channel, Sender, Receiver};
use std::sync::Arc;

use pad::{PadStr, Alignment};
use log::{LogLevel, LogMetadata, LogRecord, Log};
use nix::unistd;

use error::Result;
use io::controller::Controller;
use io::poll::*;
use io;

const MEGABYTE: usize = 1024 * 1024;

/// Logging Controller
pub trait LoggingBackend
    where Self: Controller + 'static
{

    fn level(&self) -> LogLevel;

    fn setup(&self, epfd: &EpollFd) -> Result<Box<Log>>;
}

pub struct SimpleLogging {
    level: LogLevel,
    // TODO with proper buffering bufcap: usize,
    //buf: Vec<u8>,
    //pos: usize,
    //limit: usize,
    //fd: RawFd,
}

impl SimpleLogging {
    pub fn new(level: LogLevel) -> SimpleLogging {
        SimpleLogging {
            level: level
        }
    }
    //pub fn stdout(level: LogLevel) -> SimpleLogging {
    //    let cap = 8 * MEGABYTE;
    //    Self::new(level, cap, 1)
    //}

    //pub fn stderr(level: LogLevel) -> SimpleLogging {
    //    let cap = 8 * MEGABYTE;
    //    Self::new(level, cap, 2)
    //}

    //pub fn new(level: LogLevel, bufcap: usize, fd: RawFd) -> SimpleLogging {
    //    // let logger = SimpleLogging {
    //    //    level: level,
    //    //    // bufcap: bufcap,
    //    //    fd: fd,
    //    //    pos: 0,
    //    //    limit: 0,
    //    //    buf: Vec::with_capacity(bufcap),
    //    // };
    //    // let _ = ::log::set_logger(|max_log_level| {
    //    //    // FIXME
    //    //    //max_log_level.set(LogLevelFilter::Trace);
    //    //    Box::new(logger)
    //    // });
    //    unimplemented!()
    //}
}

// struct SimpleLoggingSender<'a> {
//    //TODO Arc + Sender ?
//    tx: Arc<Sender<&'a LogRecord<'a>>>,
//    level: LogLevel,
// }
//
// unsafe impl <'a> Sync for SimpleLoggingSender<'a> {}
// unsafe impl <'a> Send for SimpleLoggingSender<'a> {}

// impl <'a> Log for SimpleLoggingSender<'a> {
//    fn enabled(&self, metadata: &LogMetadata) -> bool {
//        metadata.level() <= self.level
//    }
//
//    fn log(&self, record: &LogRecord) {
//        self.tx.deref()
//        if self.enabled(record.metadata()) {
//            let ms = format!("{:.*}",
//                             3,
//                             ((::time::precise_time_ns() % 1_000_000_000) / 1_000_000));
//            println!("{}.{} {:<5} [{}] {}",
//                     ::time::strftime("%Y-%m-%d %H:%M:%S", &::time::now()).unwrap(),
//                     ms.pad(3, '0', Alignment::Right, true),
//                     record.level().to_string(),
//                     "rpc-perf",
//                     record.args());
//        }
//    }
// }
struct TracingLogger {
    level: LogLevel
}

impl Log for TracingLogger {
    fn enabled(&self, metadata: &LogMetadata) -> bool {
        metadata.level() <= self.level
    }

    fn log(&self, record: &LogRecord) {
        if self.enabled(record.metadata()) {
            let location = record.location();
            let ms = format!("{:.*}",
                             3,
                             ((::time::precise_time_ns() % 1_000_000_000) / 1_000_000));
            println!("{}.{} {:<5} {:<20}:{} - {}",
                     ::time::strftime("%Y-%m-%d %H:%M:%S", &::time::now()).unwrap(),
                     ms.pad(3, '0', Alignment::Right, true),
                     record.level().to_string(),
                     location.file(),
                     location.line(),
                     record.args());
        }
    }
}


impl LoggingBackend for SimpleLogging {
    fn level(&self) -> LogLevel {
        self.level
    }
    fn setup(&self, _: &EpollFd) -> Result<Box<Log>> {
        //let interest = EpollEvent {
        //    events: EPOLLOUT | EPOLLET,
        //    data: self.fd as u64,
        //};
        //try!(epfd.register(self.fd, &interest));
        //let (tx, rx) = channel();
        Ok(Box::new(TracingLogger {
            level: self.level,
        }))
    }
}

impl Controller for SimpleLogging {
    fn is_terminated(&self) -> bool {
        //never terminates
        false
    }

    fn ready(&mut self, _: &EpollEvent) -> Result<()> {
        //let dst = &mut self.buf[self.pos..self.limit];

        //if !dst.is_empty() {
        //    if let Some(cnt) = try!(io::write(self.fd, &dst)) {
        //        self.pos += cnt;
        //        // naive buffering
        //        if self.pos == self.limit {
        //            self.pos = 0;
        //            self.limit = 0;
        //        }
        //    }
        //}
        Ok(())
    }
}
