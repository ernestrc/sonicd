#![feature(custom_derive, plugin, custom_attribute, box_syntax, lookup_host)]
#![plugin(serde_macros)]
extern crate serde;
extern crate serde_json;
extern crate curl;
extern crate nix;
extern crate byteorder;
#[macro_use] extern crate log;

#[cfg(feature="websocket")]
extern crate ws as libws;

mod api;
mod model;
#[macro_use] mod io;

pub mod tcp;
#[cfg(feature="websocket")]
pub mod ws;

pub use api::{run, version, stream, authenticate};
pub use model::{Query, SonicMessage, Error, Result};
