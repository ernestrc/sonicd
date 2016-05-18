#![feature(custom_derive, plugin, custom_attribute, box_syntax, lookup_host)]
#![plugin(serde_macros)]
extern crate serde;
extern crate serde_json;
extern crate curl;
extern crate nix;
extern crate ws as libws;
extern crate byteorder;
#[macro_use] extern crate log;

mod api;
mod tcp;
mod model;
mod ws;
mod io;

pub use api::{run, version, stream};
pub use model::{Query, SonicMessage, Receipt, Error, ClientConfig, Result};
pub use ws::WsHandler;
