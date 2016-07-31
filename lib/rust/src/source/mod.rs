use std::fmt::Debug;
use std::os::unix::io::RawFd;

//use nix::fcntl::{self, O_CLOEXEC, O_NONBLOCK};
use serde_json::Value;

use error::Result;
use io::poll::EpollFd;
use model::{Query, SonicMessage};

pub trait Source
    where Self: Debug,
{
    fn next(&mut self) -> Result<StreamOut>;
}

pub enum StreamOut {
    Idle,
    Message(SonicMessage),
    Completed
}

impl Source {
    pub fn new(query: Query, epfd: EpollFd) -> Result<Box<Source>> {

        let class: String = {
                try!(query.get_config("class")
                    .and_then(|s| {
                        s.as_str()
                            .ok_or("'class' field should be a string".into())
                    }))
            }
            .to_owned();

        let s = match class.as_ref() {
            "SyntheticSource" => try!(SyntheticSource::new(query, epfd)),
            e => return Err(format!("unrecorgnized source: {}", e).into()),
        };

        Ok(Box::new(s))
    }
}

#[derive(Debug)]
pub struct SyntheticSource {
    size: i64,
    indexed: bool,
    streamed: i64, // gen: RawFd,
}

impl SyntheticSource {
    pub fn new(query: Query, epfd: EpollFd) -> Result<SyntheticSource> {

        let size = match query.get_raw().parse::<i64>() {
            Ok(s) => s,
            Err(_) => {
                try!(query.get_config("size")
                    .and_then(|s| {
                        s.as_i64()
                            .ok_or("'size' field should be a string".into())
                    }))
            }
        };

        let indexed = query.get_opt("indexed")
            .and_then(|i| i.as_bool())
            .unwrap_or_else(|| true);

        // let gen = try!(fcntl::open("/dev/urandom", O_NONBLOCK, O_CLOEXEC));

        Ok(SyntheticSource {
            size: size,
            indexed: indexed,
            streamed: 0, //   gen: gen
        })
    }
}

impl Source for SyntheticSource {
    fn next(&mut self) -> Result<StreamOut> {
        if self.streamed == self.size {
            Ok(StreamOut::Completed)
        } else {
            let msg = vec!(Value::I64(self.streamed));
            self.streamed += 1;
            Ok(StreamOut::Message(SonicMessage::OutputChunk(msg)))
        }
    }
}