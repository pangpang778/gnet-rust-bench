//! Echo server example for the gnet framework.
//!
//! This example demonstrates the `EventHandler` trait by implementing
//! a simple TCP echo server. Run with:
//!
//! ```bash
//! cargo run --example echo
//! ```
//!
//! Then connect with `nc` or `telnet`:
//!
//! ```bash
//! nc 127.0.0.1 9999
//! ```

use gnet::{engine, Action, Conn, Engine, EventHandler};
use std::net::SocketAddr;
use std::time::Duration;

/// Echo handler: sends back whatever bytes it receives.
struct EchoHandler {
    connections: usize,
}

impl EchoHandler {
    fn new() -> Self {
        Self { connections: 0 }
    }
}

impl EventHandler for EchoHandler {
    fn on_boot(&mut self, engine: &Engine) -> Action {
        println!("[echo] engine booted: {engine:?}");
        Action::None
    }

    fn on_shutdown(&mut self, _engine: &Engine) {
        println!("[echo] engine shutting down");
    }

    fn on_open(&mut self, conn: &mut Conn) -> (Vec<u8>, Action) {
        self.connections += 1;
        println!(
            "[echo] connection opened: {} -> {}",
            conn.remote_addr(),
            conn.local_addr()
        );
        // Send a welcome message on connect.
        (b"Welcome to gnet echo server!\n".to_vec(), Action::None)
    }

    fn on_close(&mut self, conn: &mut Conn, err: Option<&gnet::Error>) -> Action {
        self.connections -= 1;
        match err {
            Some(e) => println!("[echo] {} closed with error: {e}", conn.remote_addr()),
            None => println!("[echo] {} closed cleanly", conn.remote_addr()),
        }
        Action::None
    }

    fn on_traffic(&mut self, conn: &mut Conn) -> Action {
        // Echo back everything currently buffered.
        let buffered = conn.inbound_buffered();
        if buffered == 0 {
            return Action::None;
        }
        match conn.peek(0) {
            Ok(data) => {
                let _ = conn.write(&data);
                conn.discard(buffered);
            }
            Err(e) => println!("[echo] peek failed: {e}"),
        }
        Action::None
    }

    fn on_tick(&mut self) -> (Option<Duration>, Action) {
        // Heartbeat every 30 seconds.
        (Some(Duration::from_secs(30)), Action::None)
    }
}

fn main() -> Result<(), gnet::Error> {
    let addr: SocketAddr = "127.0.0.1:9999"
        .parse()
        .map_err(|e| gnet::Error::InvalidAddress(format!("failed to parse listen address: {e}")))?;

    println!("[echo] starting echo server on {addr}");
    println!("[echo] press Ctrl+C to stop");

    engine::run_with(EchoHandler::new(), &format!("tcp://{addr}"), |o| {
        o.multicore = true;
    })
}
