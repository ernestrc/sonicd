use error::Result;

pub mod tcp;
pub mod echo;

pub trait Handler {

    fn on_error(&mut self) -> Result<()>;

    fn on_close(&mut self) -> Result<()>;

    fn on_readable(&mut self) -> Result<()>;

    fn on_writable(&mut self) -> Result<()>;

}
