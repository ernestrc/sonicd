#![cfg_attr(feature = "serde_macros", feature(custom_derive, plugin))]
#![cfg_attr(feature = "serde_macros", plugin(serde_macros))]
extern crate serde;
extern crate serde_json;
extern crate nix;
extern crate byteorder;
extern crate bytes;
extern crate slab;
#[macro_use] extern crate lazy_static;
#[macro_use] extern crate error_chain;
#[macro_use] extern crate log;

mod error;
mod model;
mod source;
#[macro_use] pub mod io;
//pub mod client;

//#[cfg(feature="websocket")]
//pub mod ws;

//#[cfg(any(target_os = "linux", target_os = "android"))]
//mod linuxapi;
//
//#[cfg(any(target_os = "linux", target_os = "android"))]
//mod api;

//#[cfg(feature="websocket")]
//extern crate ws as libws;


pub use model::{Authenticate, Acknowledge, Query, TypeMetadata, Done, OutputChunk, QueryProgress};
pub use model::protocol::SonicMessage;
pub use error::{Result, Error, ErrorKind};

static VERSION: &'static str = env!("CARGO_PKG_VERSION");
