//! Event handler trait and supporting types.
//!
//! This is the user-facing surface of the framework: implement
//! [`EventHandler`] and pass it to [`crate::engine::run`].

use std::any::Any;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use tokio::sync::watch;

/// An action that occurs after the completion of an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Action {
    /// No action should occur following an event.
    #[default]
    None,
    /// Close the connection.
    Close,
    /// Shutdown the engine.
    Shutdown,
}

/// Shared state behind every [`Engine`] handle.
///
/// Created once per [`crate::engine::run`] call; clones of `Engine`
/// all point at the same state.
pub(crate) struct EngineState {
    /// Set to `true` once shutdown has been requested.
    pub(crate) shutdown_flag: AtomicBool,
    /// Broadcast channel used to wake every loop and actor.
    pub(crate) shutdown_tx: watch::Sender<bool>,
    /// Receivers are cloned from this by engine internals.
    pub(crate) shutdown_rx: watch::Receiver<bool>,
    /// Number of currently open connections across all loops.
    pub(crate) conn_count: AtomicUsize,
    /// Next connection id.
    pub(crate) next_conn_id: AtomicUsize,
    /// Set when the engine has fully stopped; pairs with `done_cv`.
    pub(crate) done: Mutex<bool>,
    pub(crate) done_cv: Condvar,
}

impl EngineState {
    pub(crate) fn new() -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Self {
            shutdown_flag: AtomicBool::new(false),
            shutdown_tx,
            shutdown_rx,
            conn_count: AtomicUsize::new(0),
            next_conn_id: AtomicUsize::new(1),
            done: Mutex::new(false),
            done_cv: Condvar::new(),
        }
    }

    /// State for a default [`Engine`] that was never started: already
    /// "done" so [`Engine::stop`] returns immediately.
    fn inert() -> Self {
        let s = Self::new();
        *s.done.lock().unwrap() = true;
        s
    }

    /// Request shutdown: flip the flag and notify every watcher.
    pub(crate) fn request_shutdown(&self) {
        self.shutdown_flag.store(true, Ordering::SeqCst);
        let _ = self.shutdown_tx.send(true);
    }

    /// Mark the engine fully stopped and wake any thread in
    /// [`Engine::stop`].
    pub(crate) fn mark_done(&self) {
        let mut done = self.done.lock().unwrap();
        *done = true;
        self.done_cv.notify_all();
    }
}

/// Handle to a running engine, passed to lifecycle callbacks.
///
/// Cloneable; every clone refers to the same engine. Obtain one via
/// [`crate::engine::run`] (it is handed to `on_boot`) — a default
/// `Engine` is inert and only useful for tests.
#[derive(Clone)]
pub struct Engine {
    pub(crate) inner: Arc<EngineState>,
}

impl Default for Engine {
    fn default() -> Self {
        Self {
            inner: Arc::new(EngineState::inert()),
        }
    }
}

impl Engine {
    /// Request the engine to shut down and block until every event
    /// loop and connection has exited.
    ///
    /// # Warning
    ///
    /// This blocks the calling thread. Do not call it from inside an
    /// [`EventHandler`] callback running on the engine's runtime —
    /// with a single event loop that deadlocks. Spawn a thread or call
    /// it from a signal handler instead. [`Engine::is_shutdown`] is
    /// the non-blocking check.
    pub fn stop(&self) {
        self.inner.request_shutdown();
        let mut done = self.inner.done.lock().unwrap();
        while !*done {
            done = self.inner.done_cv.wait(done).unwrap();
        }
    }

    /// Whether shutdown has been requested or completed.
    pub fn is_shutdown(&self) -> bool {
        self.inner.shutdown_flag.load(Ordering::SeqCst)
    }

    /// Number of currently open connections across all event loops.
    pub fn connection_count(&self) -> usize {
        self.inner.conn_count.load(Ordering::SeqCst)
    }
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("shutdown", &self.is_shutdown())
            .field("connections", &self.connection_count())
            .finish()
    }
}

/// Per-connection context passed to [`EventHandler`] callbacks.
///
/// For live connections `inner` carries the socket and buffers; for
/// connections fabricated in tests it is `None` and the IO methods
/// degrade to no-ops (reads return 0 bytes, writes are accepted and
/// dropped).
pub struct Conn {
    /// Connection id, unique within the engine run.
    pub(crate) id: usize,
    /// Local socket address of the connection.
    pub(crate) local_addr: SocketAddr,
    /// Remote socket address of the peer.
    pub(crate) remote_addr: SocketAddr,
    /// User-defined per-connection context.
    pub(crate) ctx: Option<Box<dyn Any + Send>>,
    /// IO plumbing; `None` for test-constructed connections.
    pub(crate) inner: Option<Arc<crate::eventloop::ConnInner>>,
}

impl Conn {
    /// Local socket address of this connection.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Remote socket address of the peer.
    ///
    /// For UDP sockets this is the sender of the most recently received
    /// datagram (`0.0.0.0:0` before the first packet arrives).
    pub fn remote_addr(&self) -> SocketAddr {
        if let Some(inner) = &self.inner {
            if let Some(peer) = *inner.udp_peer.lock().unwrap() {
                return peer;
            }
        }
        self.remote_addr
    }

    /// Borrow the user-defined context stored on this connection.
    pub fn context<T: 'static>(&self) -> Option<&T> {
        self.ctx.as_ref()?.downcast_ref()
    }

    /// Store a user-defined context on this connection.
    pub fn set_context<T: Any + Send>(&mut self, ctx: T) {
        self.ctx = Some(Box::new(ctx));
    }

    /// Take the user-defined context out of this connection.
    pub fn take_context(&mut self) -> Option<Box<dyn Any + Send>> {
        self.ctx.take()
    }

    /// Number of received bytes buffered but not yet consumed.
    pub fn inbound_buffered(&self) -> usize {
        self.inner
            .as_ref()
            .map_or(0, |i| i.inbound.lock().unwrap().buffered())
    }

    /// Number of bytes queued for transmission but not yet written
    /// to the socket.
    pub fn outbound_buffered(&self) -> usize {
        self.inner.as_ref().map_or(0, |i| {
            if i.is_udp {
                i.udp_out
                    .lock()
                    .unwrap()
                    .iter()
                    .map(Vec::len)
                    .sum::<usize>()
            } else {
                i.outbound.lock().unwrap().buffered()
            }
        })
    }

    /// Copy up to `buf.len()` received bytes out of the inbound
    /// buffer, consuming them. Returns the number of bytes copied;
    /// `0` means nothing is buffered.
    ///
    /// Only meaningful inside [`EventHandler`] callbacks — that is
    /// where inbound data is guaranteed to be present.
    pub fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let Some(inner) = &self.inner else {
            return Ok(0);
        };
        let mut inbound = inner.inbound.lock().unwrap();
        match inbound.read(buf) {
            Ok(n) => Ok(n),
            Err(crate::buffer::EmptyError) => Ok(0),
        }
    }

    /// Copy up to `n` received bytes without consuming them
    /// (`n == 0` or `n > buffered` peeks at everything buffered).
    pub fn peek(&self, n: usize) -> io::Result<Vec<u8>> {
        let Some(inner) = &self.inner else {
            return Ok(Vec::new());
        };
        let inbound = inner.inbound.lock().unwrap();
        let want = if n == 0 {
            inbound.buffered()
        } else {
            n.min(inbound.buffered())
        };
        Ok(inbound
            .peek(want)
            .chunks()
            .iter()
            .flat_map(|c| c.iter().copied())
            .collect())
    }

    /// Discard up to `n` received bytes (`n <= 0` discards all).
    /// Returns the number of bytes actually discarded.
    pub fn discard(&mut self, n: usize) -> usize {
        let Some(inner) = &self.inner else {
            return 0;
        };
        inner.inbound.lock().unwrap().discard(n)
    }

    /// Queue `data` for transmission to the peer.
    ///
    /// For stream connections the bytes are appended to the outbound
    /// elastic buffer and flushed by the connection's IO task; for UDP
    /// each call becomes one datagram sent to the most recent peer.
    /// Returns the number of bytes accepted — always `data.len()` on
    /// success since writes are buffered.
    pub fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        let Some(inner) = &self.inner else {
            return Ok(data.len());
        };
        if inner.is_udp {
            inner.udp_out.lock().unwrap().push_back(data.to_vec());
        } else {
            inner.outbound.lock().unwrap().write(data);
        }
        inner.outbound_notify.notify_one();
        Ok(data.len())
    }

    /// Queue multiple byte slices for transmission, preserving order.
    pub fn write_vectored(&mut self, bufs: &[&[u8]]) -> io::Result<usize> {
        let Some(inner) = &self.inner else {
            return Ok(bufs.iter().map(|b| b.len()).sum());
        };
        if inner.is_udp {
            let mut q = inner.udp_out.lock().unwrap();
            for b in bufs {
                q.push_back(b.to_vec());
            }
        } else {
            inner.outbound.lock().unwrap().writev(bufs);
        }
        inner.outbound_notify.notify_one();
        Ok(bufs.iter().map(|b| b.len()).sum())
    }

    /// Nudge the IO task to flush pending outbound data now.
    ///
    /// Writes already trigger a flush automatically; this is only
    /// useful after manipulating buffers directly.
    pub fn flush(&self) -> io::Result<()> {
        if let Some(inner) = &self.inner {
            inner.outbound_notify.notify_one();
        }
        Ok(())
    }

    /// Close the connection. The actual socket close and the
    /// [`EventHandler::on_close`] callback happen asynchronously on
    /// the owning event loop.
    pub fn close(&self) {
        if let Some(inner) = &self.inner {
            let _ = inner.cmd_tx.send(crate::eventloop::ConnCmd::Close);
        }
    }

    /// Trigger another [`EventHandler::on_traffic`] callback for this
    /// connection without new socket data.
    pub fn wake(&self) {
        if let Some(inner) = &self.inner {
            let _ = inner.cmd_tx.send(crate::eventloop::ConnCmd::Wake);
        }
    }
}

impl std::fmt::Debug for Conn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Conn")
            .field("id", &self.id)
            .field("local_addr", &self.local_addr)
            .field("remote_addr", &self.remote_addr)
            .finish_non_exhaustive()
    }
}

/// Callbacks fired by the engine for connection and lifecycle events.
///
/// All methods take `&mut self` so handlers can keep per-engine state.
/// The trait is object-safe: use `Box<dyn EventHandler>`. Handlers
/// shared across event loops must be `Send`; `run` requires it.
pub trait EventHandler {
    /// Fires when the engine is ready for accepting connections.
    fn on_boot(&mut self, _engine: &Engine) -> Action {
        Action::None
    }

    /// Fires when the engine is being shut down, after all event loops
    /// and connections are closed.
    fn on_shutdown(&mut self, _engine: &Engine) {}

    /// Fires when a new connection has been opened.
    ///
    /// Returns bytes to send back to the peer immediately, plus the
    /// action to take. Sending large amounts of data in `on_open` is
    /// usually not recommended.
    fn on_open(&mut self, _conn: &mut Conn) -> (Vec<u8>, Action) {
        (Vec::new(), Action::None)
    }

    /// Fires when a connection has been closed.
    ///
    /// `err` is the last known connection error, if any.
    fn on_close(&mut self, _conn: &mut Conn, _err: Option<&crate::Error>) -> Action {
        Action::None
    }

    /// Fires when a socket receives data from the remote.
    fn on_traffic(&mut self, _conn: &mut Conn) -> Action {
        Action::None
    }

    /// Fires immediately after the engine starts and again after the
    /// returned delay elapses. Return `None` for `delay` to stop ticking.
    fn on_tick(&mut self) -> (Option<Duration>, Action) {
        (None, Action::None)
    }
}

/// A no-op handler for users who only need a subset of callbacks.
///
/// Embed it (via `#[derive(Default)]` composition is not possible for
/// trait objects, so implement `EventHandler` and delegate) or use it
/// directly when no callbacks are needed.
#[derive(Debug, Default)]
pub struct NoopHandler;

impl EventHandler for NoopHandler {}

#[cfg(test)]
mod tests {
    use super::*;

    struct Counting {
        opened: usize,
        closed: usize,
        ticked: bool,
    }

    impl EventHandler for Counting {
        fn on_open(&mut self, _conn: &mut Conn) -> (Vec<u8>, Action) {
            self.opened += 1;
            (b"hi".to_vec(), Action::None)
        }

        fn on_close(&mut self, _conn: &mut Conn, _err: Option<&crate::Error>) -> Action {
            self.closed += 1;
            Action::None
        }

        fn on_tick(&mut self) -> (Option<Duration>, Action) {
            self.ticked = true;
            (Some(Duration::from_secs(1)), Action::None)
        }
    }

    fn make_conn() -> Conn {
        Conn {
            id: 0,
            local_addr: "127.0.0.1:8080".parse().unwrap(),
            remote_addr: "127.0.0.1:12345".parse().unwrap(),
            ctx: None,
            inner: None,
        }
    }

    #[test]
    fn action_default_is_none() {
        assert_eq!(Action::default(), Action::None);
    }

    #[test]
    fn action_variants_distinct() {
        assert_ne!(Action::None, Action::Close);
        assert_ne!(Action::Close, Action::Shutdown);
    }

    #[test]
    fn conn_exposes_addresses() {
        let conn = make_conn();
        assert_eq!(conn.local_addr().port(), 8080);
        assert_eq!(conn.remote_addr().port(), 12345);
    }

    #[test]
    fn conn_context_roundtrip() {
        let mut conn = make_conn();
        conn.set_context(String::from("session-1"));
        assert_eq!(
            conn.context::<String>().map(String::as_str),
            Some("session-1")
        );
        assert!(conn.context::<u32>().is_none());
        let taken = conn.take_context().unwrap();
        assert_eq!(taken.downcast::<String>().unwrap().as_str(), "session-1");
        assert!(conn.take_context().is_none());
    }

    #[test]
    fn noop_handler_satisfies_trait_object() {
        let mut h: Box<dyn EventHandler> = Box::new(NoopHandler);
        let engine = Engine::default();
        assert_eq!(h.on_boot(&engine), Action::None);
        h.on_shutdown(&engine);
        let mut conn = make_conn();
        assert_eq!(h.on_open(&mut conn).1, Action::None);
        assert_eq!(h.on_close(&mut conn, None), Action::None);
        assert_eq!(h.on_traffic(&mut conn), Action::None);
        assert_eq!(h.on_tick().0, None);
    }

    #[test]
    fn handler_state_mutates_across_callbacks() {
        let mut h = Counting {
            opened: 0,
            closed: 0,
            ticked: false,
        };
        let mut conn = make_conn();
        let (out, action) = h.on_open(&mut conn);
        assert_eq!(out, b"hi");
        assert_eq!(action, Action::None);
        assert_eq!(h.on_close(&mut conn, None), Action::None);
        let (delay, _) = h.on_tick();
        assert_eq!(delay, Some(Duration::from_secs(1)));
        assert_eq!(h.opened, 1);
        assert_eq!(h.closed, 1);
        assert!(h.ticked);
    }

    #[test]
    fn trait_is_object_safe_for_stateful_handler() {
        let mut h: Box<dyn EventHandler> = Box::new(Counting {
            opened: 0,
            closed: 0,
            ticked: false,
        });
        let mut conn = make_conn();
        let _ = h.on_open(&mut conn);
        let _ = h.on_traffic(&mut conn);
        let _ = h.on_close(&mut conn, None);
    }

    #[test]
    fn test_conn_io_methods_are_safe_noops() {
        let mut conn = make_conn();
        let mut buf = [0u8; 8];
        assert_eq!(conn.read(&mut buf).unwrap(), 0);
        assert!(conn.peek(4).unwrap().is_empty());
        assert_eq!(conn.discard(4), 0);
        assert_eq!(conn.write(b"x").unwrap(), 1);
        assert_eq!(conn.write_vectored(&[b"a", b"b"]).unwrap(), 2);
        assert_eq!(conn.inbound_buffered(), 0);
        assert_eq!(conn.outbound_buffered(), 0);
        conn.flush().unwrap();
        conn.close();
        conn.wake();
    }

    #[test]
    fn engine_default_is_inert() {
        let e = Engine::default();
        assert!(!e.is_shutdown());
        assert_eq!(e.connection_count(), 0);
        e.stop(); // returns immediately: nothing is running
        assert!(e.is_shutdown());
    }
}
