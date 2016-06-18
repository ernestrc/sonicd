#![feature(custom_derive, plugin, custom_attribute, box_syntax, lookup_host)]
#![plugin(serde_macros)]
extern crate serde;
extern crate serde_json;
extern crate curl;
extern crate nix;
extern crate byteorder;
#[macro_use] extern crate log;

// TODO client
mod api;
mod tcp;
mod model;

pub use api::{run, version};
pub use tcp::stream;
pub use model::{Query, SonicMessage, Receipt, Error, ClientConfig};
