use model::{Query, SonicMessage, Result};
use std::rc::Rc;
use std::cell::Cell;
use libws::{Sender, Message, Handler, Handshake, CloseCode};

pub struct WsHandler {
    out: Sender,
    count: Rc<Cell<u32>>,
}

// TODO this is WIP for cache
impl WsHandler {
    pub fn new(out: Sender, count: Rc<Cell<u32>>) -> WsHandler {
        WsHandler {
            out: out,
            count: count,
        }
    }
}

fn convert_msg(msg: Message) -> Result<SonicMessage> {
    match msg {
        Message::Text(string) => SonicMessage::from_bytes(string.into_bytes()),
        Message::Binary(bytes) => SonicMessage::from_bytes(bytes),
    }
}

impl Handler for WsHandler {
    fn on_open(&mut self, h: Handshake) -> ::libws::Result<()> {
        // We have a new connection, so we increment the connection counter
        let count = self.count.get() + 1;
        debug!("connection count: {}", &count);
        debug!("handshake: {:?}", &h);
        Ok(self.count.set(count))
    }

    fn on_error(&mut self, err: ::libws::Error) {
        error!("The server encountered an error: {:?}", err);

        // The connection is going down, so we need to decrement the count
        self.count.set(self.count.get() - 1)
    }

    fn on_message(&mut self, msg: Message) -> ::libws::Result<()> {
        match convert_msg(msg).and_then(|sm| Query::from_msg(sm)) {
            Ok(q) => {
                debug!("successfully deserialized query {:?}", &q);
                Ok(())
            }
            r@ Err(_) => {
                error!("protocol error: unable to deserialize query");
                let done = SonicMessage::done(r);
                self.out.send(Message::Text(::serde_json::to_string(&done).unwrap())).unwrap();
                self.out.close(CloseCode::Error)
            }
        }
    }

    fn on_close(&mut self, code: CloseCode, reason: &str) {
        // The WebSocket protocol allows for a utf8 reason for the closing state after the
        // close code. WS-RS will attempt to interpret this data as a utf8 description of the
        // reason for closing the connection. I many cases, `reason` will be an empty string.
        // So, you may not normally want to display `reason` to the user,
        // but let's assume that we know that `reason` is human-readable.
        match code {
            CloseCode::Normal => println!("The client is done with the connection."),
            CloseCode::Away => println!("The client is leaving the site."),
            _ => println!("The client encountered an error: {}", reason),
        }
    }
}
