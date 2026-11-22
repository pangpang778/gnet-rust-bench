//! EventHandler trait, Action enum, and public Engine API.
//!
//! Mirrors `gnet-go/gnet.go`'s EventHandler but idiomatic Rust: handlers take
//! `&self` (use interior mutability for state) and receive `&mut Conn`.

use crate::conn::{Conn, Proto};
use crate::loadbalancer::LoadBalancing;
use std::io;
use std::sync::Arc;
use std::time::Duration;

/// Action returned from event callbacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Action {
    /// No action; keep the connection open.
    #[default]
    None,
    /// Close this connection after the callback returns.
    Close,
    /// Shut down the whole engine.
    Shutdown,
}

/// Engine facade handed to `OnBoot`/`OnShutdown`.
#[derive(Clone)]
pub struct Engine {
    pub(crate) inner: Arc<crate::engine::EngineInner>,
}

impl Engine {
    /// Number of currently active connections across all loops.
    pub fn count_connections(&self) -> usize {
        self.inner.count_connections()
    }

    /// Gracefully stop the engine: stop accepting, close listeners, wait for
    /// loops and connections to drain, then return. Blocks the caller.
    pub fn stop(&self) {
        self.inner.shutdown();
    }

    /// Non-blocking check: has shutdown been initiated?
    pub fn is_shutdown(&self) -> bool {
        self.inner.is_shutdown()
    }

    /// Register a connected `TcpStream` into the engine (client-side use).
    /// The stream is moved onto an event loop chosen by the balancer.
    pub fn register_tcp(&self, stream: std::net::TcpStream) -> io::Result<()> {
        self.inner.register_tcp(stream)
    }

    /// Register a connected `UdpSocket`.
    pub fn register_udp(&self, sock: std::net::UdpSocket) -> io::Result<()> {
        self.inner.register_udp(sock)
    }
}

/// Event callbacks. All methods take `&self`; keep handler state behind
/// `Mutex`/`RwLock`/atomics as needed. Methods run on the connection's event
/// loop thread — do NOT block.
///
/// Provide defaults by implementing only what you need, or embed
/// [`NoopHandler`] for the rest.
pub trait EventHandler: Send + Sync + 'static {
    /// Fires once when the engine is ready to accept connections.
    fn on_boot(&self, _engine: Engine) -> Action {
        Action::None
    }

    /// Fires when the engine is shutting down, after loops/connections close.
    fn on_shutdown(&self, _engine: Engine) {}

    /// Fires when a new connection is opened. Return `Some(bytes)` to send
    /// immediately, plus an Action.
    fn on_open(&self, _conn: &mut Conn) -> (Option<Vec<u8>>, Action) {
        (None, Action::None)
    }

    /// Fires when a connection closes. `err` is the last known error, if any.
    fn on_close(&self, _conn: &mut Conn, _err: Option<&io::Error>) -> Action {
        Action::None
    }

    /// Fires when a socket has inbound data. Read via `conn.inbound` and write
    /// replies via `conn.send(...)`.
    fn on_traffic(&self, _conn: &mut Conn) -> Action {
        Action::None
    }

    /// Fires periodically. Return `(delay_until_next, action)`. Returning
    /// `Duration::ZERO` disables further ticks after this one? No — the loop
    /// re-arms with the returned delay; use a large delay to effectively stop.
    /// Default: disabled.
    fn on_tick(&self) -> (Duration, Action) {
        (Duration::from_secs(3600 * 24 * 365), Action::None)
    }
}

/// No-op handler for embedding.
#[derive(Default)]
pub struct NoopHandler;

impl EventHandler for NoopHandler {}

/// Options for `Engine::run`.
#[derive(Debug, Clone)]
pub struct Options {
    /// Number of event loops. 0 = number of logical CPUs.
    pub num_event_loop: usize,
    /// Load balancing algorithm for incoming connections.
    pub load_balancing: LoadBalancing,
    /// Set SO_REUSEADDR on listeners.
    pub reuse_addr: bool,
    /// Set SO_REUSEPORT on listeners (each loop binds its own socket when true
    /// and num_event_loop > 1; kernel load-balances).
    pub reuse_port: bool,
    /// TCP_NODELAY on accepted sockets.
    pub tcp_no_delay: bool,
    /// SO_KEEPALIVE idle time on accepted sockets.
    pub tcp_keepalive: Option<Duration>,
    /// Max bytes the inbound ring grows to per conn before codec must consume.
    pub read_buffer_cap: usize,
    /// Static outbound buffer size before spilling to the linked list.
    pub write_buffer_cap: usize,
    /// Enable the periodic `on_tick` callback (only meaningful if handler
    /// overrides it; the ticker runs on loop 0).
    pub ticker: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            num_event_loop: 0,
            load_balancing: LoadBalancing::RoundRobin,
            reuse_addr: true,
            reuse_port: false,
            tcp_no_delay: true,
            tcp_keepalive: None,
            read_buffer_cap: 64 * 1024,
            write_buffer_cap: 64 * 1024,
            ticker: false,
        }
    }
}

/// Builder-style option setters.
impl Options {
    pub fn num_event_loop(mut self, n: usize) -> Self {
        self.num_event_loop = n;
        self
    }
    pub fn load_balancing(mut self, lb: LoadBalancing) -> Self {
        self.load_balancing = lb;
        self
    }
    pub fn reuse_addr(mut self, v: bool) -> Self {
        self.reuse_addr = v;
        self
    }
    pub fn reuse_port(mut self, v: bool) -> Self {
        self.reuse_port = v;
        self
    }
    pub fn tcp_no_delay(mut self, v: bool) -> Self {
        self.tcp_no_delay = v;
        self
    }
    pub fn tcp_keepalive(mut self, d: Duration) -> Self {
        self.tcp_keepalive = Some(d);
        self
    }
    pub fn read_buffer_cap(mut self, n: usize) -> Self {
        self.read_buffer_cap = n;
        self
    }
    pub fn write_buffer_cap(mut self, n: usize) -> Self {
        self.write_buffer_cap = n;
        self
    }
    pub fn ticker(mut self, v: bool) -> Self {
        self.ticker = v;
        self
    }
}

/// Parse `tcp://host:port`, `udp://`, `unix://` address strings.
pub fn parse_proto_addr(s: &str) -> io::Result<(Proto, String)> {
    if let Some(rest) = s.strip_prefix("tcp://") {
        return Ok((Proto::Tcp, rest.to_string()));
    }
    if let Some(rest) = s.strip_prefix("tcp4://") {
        return Ok((Proto::Tcp, rest.to_string()));
    }
    if let Some(rest) = s.strip_prefix("tcp6://") {
        return Ok((Proto::Tcp, rest.to_string()));
    }
    if let Some(rest) = s.strip_prefix("udp://") {
        return Ok((Proto::Udp, rest.to_string()));
    }
    if let Some(rest) = s.strip_prefix("udp4://") {
        return Ok((Proto::Udp, rest.to_string()));
    }
    if let Some(rest) = s.strip_prefix("udp6://") {
        return Ok((Proto::Udp, rest.to_string()));
    }
    if let Some(rest) = s.strip_prefix("unix://") {
        return Ok((Proto::Unix, rest.to_string()));
    }
    // Bare address => tcp
    Ok((Proto::Tcp, s.to_string()))
}

/// Run the engine on `addr` (e.g. "tcp://127.0.0.1:8080") with `handler`.
/// Blocks until `Engine::stop()` is called or `on_traffic`/`on_tick` returns
/// `Action::Shutdown`. Returns after graceful shutdown completes.
pub fn run<H: EventHandler>(handler: H, addr: &str, opts: Options) -> io::Result<()> {
    let (proto, address) = parse_proto_addr(addr)?;
    let inner = crate::engine::EngineInner::new(handler, proto, address, opts)?;
    let eng = Engine {
        inner: inner.clone(),
    };
    // Fire on_boot; Shutdown aborts before serving.
    if eng_action_shutdown(inner.shared_handler().on_boot(eng.clone())) {
        return Ok(());
    }
    inner.run(eng)
}

fn eng_action_shutdown(a: Action) -> bool {
    a == Action::Shutdown
}
