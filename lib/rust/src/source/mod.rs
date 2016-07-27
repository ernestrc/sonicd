use std::fmt::Debug;

use error::Result;
use io::poll::EpollFd;
use model::{SonicMessage, Query};

pub trait Source where Self: Debug {
    fn next(&mut self) -> Result<Option<Vec<SonicMessage>>>;
}

impl Source {
    pub fn new(query: Query, epfd: EpollFd) -> Result<Box<Source>> {

        let class: String = {
            try!(query.get_config("class")
            .and_then(|s| s.as_str().ok_or("'class' field should be a string".into())))
        }.to_owned();

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
}

impl SyntheticSource {
    pub fn new(query: Query, epfd: EpollFd) -> Result<SyntheticSource> {

        let size = try!(query.get_config("size")
            .and_then(|s| s.as_i64().ok_or("'size' field should be a string".into())));

        let indexed = query.get_opt("indexed")
            .and_then(|i| i.as_bool())
            .unwrap_or_else(|| true);

        Ok(SyntheticSource {
            size: size,
            indexed: indexed,
        })
    }
}

impl Source for SyntheticSource {

    fn next(&mut self) -> Result<Option<Vec<SonicMessage>>> {
        unimplemented!()
    }
}
