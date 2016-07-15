#![cfg_attr(feature = "serde_macros", feature(custom_derive, plugin))]
#![cfg_attr(feature = "serde_macros", plugin(serde_macros))]
extern crate serde;
extern crate serde_json;
extern crate nix;
extern crate byteorder;
extern crate bytes;
extern crate slab;
extern crate num_cpus;
extern crate pad;
extern crate time;
#[macro_use] extern crate lazy_static;
#[macro_use] extern crate error_chain;
#[macro_use] extern crate log;

mod error;
mod model;
mod api;
mod source;
#[macro_use] pub mod io;


pub use io::handler::Handler;
pub use io::controller::{Controller};
//pub use io::controller::sync::SyncController;

pub use api::{run, stream, authenticate};
pub use model::{Authenticate, Acknowledge, Query, TypeMetadata, Done, OutputChunk, QueryProgress};
pub use model::protocol::SonicMessage;
pub use error::{Result, Error, ErrorKind};

static VERSION: &'static str = env!("CARGO_PKG_VERSION");
