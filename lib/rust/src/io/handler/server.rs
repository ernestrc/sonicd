use std::os::unix::io::RawFd;

/*
use super::Handler;
use {Error, Result};

struct Server {
    id: Option<usize>,
    epfds: Vec<RawFd>,
    sepfd: RawFd,
}

impl Handler for Server {

    fn with_id(self, id: usize) -> Self {
        self.id = Some(id);
        self
    }

    fn id(&self) -> usize {
        self.id.unwrap()
    }

    fn on_error(&mut self) -> Result<()> {
        unimplemented!()
    }

    fn on_close(&mut self) -> Result<()> {
        unimplemented!()
    }

    fn on_readable(&mut self) -> Result<()> { 
        unimplemented!()
    }

    fn on_writable(&mut self) -> Result<()> {
        unimplemented!()
    }
}*/
