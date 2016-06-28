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
#[macro_use] mod io;

pub use api::{run, version, stream, authenticate};
pub use tcp::{read_message, frame};
pub use model::{Query, SonicMessage, Error, ClientConfig, Result};
pub use ws::WsHandler;
