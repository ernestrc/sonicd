use model::Result;
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::thread;
use std::fmt;
use std::sync::mpsc::channel;

pub struct Handler {
    listener: TcpListener,
    connections: Vec<usize>,
}

/// Utility function for setting up a tcp socket server
pub fn listen<A, F, H>(addr: A, factory: F) -> Result<()>
    where A: ToSocketAddrs + fmt::Debug,
          F: FnMut(usize) -> Handler
{
    let listener = TcpListener::bind(addr).unwrap();

    let (tx, rx) = channel();

    // accept connections and process them
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(move || {
                    // connection succeeded
                    factory(stream.as_raw_fd())
                });
            }
            Err(e) => {
                // connection failed
            }
        }
    }

    // close the socket server
    drop(listener);
    Ok(())
}
