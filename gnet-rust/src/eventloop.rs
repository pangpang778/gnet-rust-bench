//! Event loop: per-loop connection actors and the dispatch task.
//!
//! Architecture (the Rust analogue of gnet's `eventloop_unix.go`):
//!
//! - One event-loop task per worker, owning the [`EventHandler`]
//!   callbacks for every connection assigned to it. Callbacks are
//!   serialized per loop through a shared `Mutex`, matching the Go
//!   model where one handler object serves all loops.
//! - One actor task per connection owning the socket. The actor does
//!   all non-blocking IO (`try_read`/`try_write` until `WouldBlock`,
//!   i.e. edge-triggered draining) and forwards "socket has data" /
//!   "socket closed" notices to its loop over an mpsc channel.
//! - The loop calls `on_open`/`on_traffic`/`on_close`/`on_tick` and
//!   maps the returned [`Action`] onto connection close or engine
//!   shutdown.
//!
//! Tokio's reactor supplies epoll on Linux, kqueue on BSD/macOS and
//! IOCP on Windows underneath `TcpStream`/`UdpSocket`, so the same
//! code covers all three.

use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::{mpsc, watch, Notify};
use tokio::time::{sleep_until, Instant, Sleep};

use crate::balancer::Balancer;
use crate::buffer::{ElasticBuffer, RingBuffer};
use crate::error::Error;
use crate::handler::{Action, Conn, EngineState, EventHandler};
use crate::options::Options;

/// Scratch buffer size for socket reads/writes.
const IO_BUF_SIZE: usize = 16 * 1024;

/// Commands a [`Conn`] can send to its owning actor.
#[derive(Debug)]
pub(crate) enum ConnCmd {
    /// Close the socket and report `Closed` to the loop.
    Close,
    /// Ask the loop to fire `on_traffic` again.
    Wake,
}

/// Shared IO state of one live connection.
///
/// The actor task owns the socket; the loop owns the [`Conn`] handle.
/// Both reach the buffers through this struct, so `Conn::write` called
/// inside a handler callback and the actor's flush never race.
pub(crate) struct ConnInner {
    /// Received bytes not yet consumed by the handler.
    pub inbound: Mutex<RingBuffer>,
    /// Stream outbound buffer (ring + overflow list).
    pub outbound: Mutex<ElasticBuffer>,
    /// UDP outbound datagram queue (one entry per datagram).
    pub udp_out: Mutex<VecDeque<Vec<u8>>>,
    /// Wakes the actor when outbound data arrives.
    pub outbound_notify: Notify,
    /// Commands from `Conn::{close,wake}` to the actor.
    pub cmd_tx: mpsc::UnboundedSender<ConnCmd>,
    /// Whether this is a UDP socket.
    pub is_udp: bool,
    /// Latest UDP peer address, updated per datagram.
    pub udp_peer: Mutex<Option<SocketAddr>>,
}

impl ConnInner {
    pub(crate) fn new(
        read_cap: usize,
        write_cap: usize,
        is_udp: bool,
        cmd_tx: mpsc::UnboundedSender<ConnCmd>,
    ) -> Self {
        Self {
            inbound: Mutex::new(RingBuffer::new(read_cap)),
            outbound: Mutex::new(
                ElasticBuffer::new(write_cap)
                    .unwrap_or_else(|_| ElasticBuffer::new(64 * 1024).expect("nonzero cap")),
            ),
            udp_out: Mutex::new(VecDeque::new()),
            outbound_notify: Notify::new(),
            cmd_tx,
            is_udp,
            udp_peer: Mutex::new(None),
        }
    }
}

/// Socket kind carried by [`LoopMsg::NewConn`].
pub(crate) enum StreamKind {
    Tcp(TcpStream),
    Udp(UdpSocket),
}

/// Messages sent to an event-loop task.
pub(crate) enum LoopMsg {
    /// Register a freshly accepted (or bound, for UDP) connection.
    NewConn {
        id: usize,
        kind: StreamKind,
        local: SocketAddr,
        remote: SocketAddr,
    },
    /// The actor saw inbound data; fire `on_traffic`.
    Traffic(usize),
    /// The actor exited; fire `on_close`.
    Closed(usize, Option<Error>),
}

/// Handle used by the acceptor/engine to reach one loop.
#[derive(Clone)]
pub(crate) struct LoopHandle {
    pub id: usize,
    pub tx: mpsc::UnboundedSender<LoopMsg>,
}

/// Everything a loop task needs, bundled for `tokio::spawn`.
pub(crate) struct LoopParts {
    pub id: usize,
    pub handler: Arc<Mutex<Box<dyn EventHandler + Send>>>,
    pub state: Arc<EngineState>,
    pub options: Arc<Options>,
    pub balancer: Arc<Mutex<Box<dyn Balancer<usize> + Send>>>,
    pub is_tick_loop: bool,
}

/// Spawn one event-loop task. Returns its handle (for the acceptor)
/// and join handle (for engine teardown).
pub(crate) fn spawn_loop(parts: LoopParts) -> (LoopHandle, tokio::task::JoinHandle<()>) {
    let (tx, rx) = mpsc::unbounded_channel::<LoopMsg>();
    let handle = LoopHandle {
        id: parts.id,
        tx: tx.clone(),
    };
    let join = tokio::spawn(run_loop(parts, tx, rx));
    (handle, join)
}

/// Register a new connection: build shared state, spawn the actor,
/// store the [`Conn`], fire `on_open`, then apply its action.
#[allow(clippy::too_many_arguments)]
fn open_conn(
    loop_id: usize,
    conn_id: usize,
    kind: StreamKind,
    local: SocketAddr,
    remote: SocketAddr,
    conns: &mut HashMap<usize, Conn>,
    msg_tx: &mpsc::UnboundedSender<LoopMsg>,
    shutdown_rx: &watch::Receiver<bool>,
    handler: &Arc<Mutex<Box<dyn EventHandler + Send>>>,
    state: &Arc<EngineState>,
    options: &Arc<Options>,
    balancer: &Arc<Mutex<Box<dyn Balancer<usize> + Send>>>,
) -> Action {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let is_udp = matches!(kind, StreamKind::Udp(_));
    let inner = Arc::new(ConnInner::new(
        options.read_buffer_cap,
        options.write_buffer_cap,
        is_udp,
        cmd_tx,
    ));

    let mut conn = Conn {
        id: conn_id,
        local_addr: local,
        remote_addr: remote,
        ctx: None,
        inner: Some(inner.clone()),
    };

    // Spawn the actor before on_open so callback writes flush at once.
    match kind {
        StreamKind::Tcp(stream) => {
            // TCP_NODELAY on every accepted connection (Go applies it
            // as a listener sockopt inherited by accept(); tokio does
            // not inherit it, so set it per-socket).
            if matches!(options.tcp_no_delay, crate::options::TcpNoDelay::Enabled) {
                let _ = stream.set_nodelay(true);
            }
            tokio::spawn(tcp_actor(
                conn_id,
                stream,
                inner,
                cmd_rx,
                msg_tx.clone(),
                shutdown_rx.clone(),
            ));
        }
        StreamKind::Udp(sock) => {
            tokio::spawn(udp_actor(
                conn_id,
                sock,
                inner,
                cmd_rx,
                msg_tx.clone(),
                shutdown_rx.clone(),
            ));
        }
    }

    state.conn_count.fetch_add(1, Ordering::SeqCst);
    balancer.lock().unwrap().conn_opened(loop_id);

    let (out, action) = {
        let mut h = handler.lock().unwrap();
        h.on_open(&mut conn)
    };
    if !out.is_empty() {
        let _ = conn.write(&out);
    }
    conns.insert(conn_id, conn);
    action
}

/// Apply an `Action` returned from `on_open`/`on_traffic`.
fn handle_action(
    conns: &HashMap<usize, Conn>,
    conn_id: usize,
    action: Action,
    state: &Arc<EngineState>,
) {
    match action {
        Action::None => {}
        Action::Close => {
            if let Some(conn) = conns.get(&conn_id) {
                conn.close();
            }
        }
        Action::Shutdown => state.request_shutdown(),
    }
}

/// Fire `on_tick` and return the next sleep deadline.
fn perform_tick(
    handler: &Arc<Mutex<Box<dyn EventHandler + Send>>>,
    state: &Arc<EngineState>,
) -> Option<Pin<Box<Sleep>>> {
    let (delay, action) = {
        let mut h = handler.lock().unwrap();
        h.on_tick()
    };
    if matches!(action, Action::Shutdown) {
        state.request_shutdown();
    }
    delay.map(|d| Box::pin(sleep_until(Instant::now() + d)) as Pin<Box<Sleep>>)
}

/// The event-loop task: owns all connections assigned to it and is
/// the only place `EventHandler` callbacks run for those conns.
async fn run_loop(
    parts: LoopParts,
    msg_tx: mpsc::UnboundedSender<LoopMsg>,
    mut msg_rx: mpsc::UnboundedReceiver<LoopMsg>,
) {
    let LoopParts {
        id: loop_id,
        handler,
        state,
        options,
        balancer,
        is_tick_loop,
    } = parts;

    let mut conns: HashMap<usize, Conn> = HashMap::new();
    let mut shutdown_rx = state.shutdown_rx.clone();
    let mut shutting_down = state.shutdown_flag.load(Ordering::SeqCst);

    // Ticker state (loop 0 only, mirroring Go's el0): fire on_tick
    // once up front, then reschedule from its returned delay.
    let mut tick_sleep: Option<Pin<Box<Sleep>>> = is_tick_loop
        .then(|| perform_tick(&handler, &state))
        .flatten();

    loop {
        if shutting_down && conns.is_empty() {
            break;
        }

        tokio::select! {
            biased;
            _ = shutdown_rx.changed() => {
                shutting_down = true;
            }
            msg = msg_rx.recv() => {
                match msg {
                    Some(LoopMsg::NewConn { id: conn_id, kind, local, remote }) => {
                        let action = open_conn(
                            loop_id,
                            conn_id,
                            kind,
                            local,
                            remote,
                            &mut conns,
                            &msg_tx,
                            &shutdown_rx,
                            &handler,
                            &state,
                            &options,
                            &balancer,
                        );
                        handle_action(&conns, conn_id, action, &state);
                    }
                    Some(LoopMsg::Traffic(conn_id)) => {
                        if let Some(conn) = conns.get_mut(&conn_id) {
                            let action = {
                                let mut h = handler.lock().unwrap();
                                h.on_traffic(conn)
                            };
                            handle_action(&conns, conn_id, action, &state);
                        }
                    }
                    Some(LoopMsg::Closed(conn_id, err)) => {
                        if let Some(mut conn) = conns.remove(&conn_id) {
                            state.conn_count.fetch_sub(1, Ordering::SeqCst);
                            balancer.lock().unwrap().conn_closed(loop_id);
                            let action = {
                                let mut h = handler.lock().unwrap();
                                h.on_close(&mut conn, err.as_ref())
                            };
                            if matches!(action, Action::Shutdown) {
                                state.request_shutdown();
                            }
                        }
                    }
                    None => {
                        // Every sender dropped: the engine is gone.
                        shutting_down = true;
                    }
                }
            }
            _ = async {
                match tick_sleep.as_mut() {
                    Some(s) => s.await,
                    None => std::future::pending::<()>().await,
                }
            }, if is_tick_loop => {
                tick_sleep = perform_tick(&handler, &state);
            }
        }
    }
}

// ------------------------------------------------------------------
// Connection actors
// ------------------------------------------------------------------

/// Drain the socket until `WouldBlock`; returns `false` on EOF/error.
/// Inbound bytes are appended to `inner.inbound` and the loop is
/// notified once per drain.
async fn drain_reads(
    stream: &TcpStream,
    inner: &Arc<ConnInner>,
    scratch: &mut [u8],
    msg_tx: &mpsc::UnboundedSender<LoopMsg>,
    conn_id: usize,
) -> Result<bool, Error> {
    let mut got = false;
    loop {
        match stream.try_read(scratch) {
            Ok(0) => return Ok(false), // EOF
            Ok(n) => {
                inner.inbound.lock().unwrap().write(&scratch[..n]);
                got = true;
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) => return Err(Error::Io(e)),
        }
    }
    if got {
        let _ = msg_tx.send(LoopMsg::Traffic(conn_id));
    }
    Ok(true)
}

/// Flush pending outbound bytes; returns `Err` on a real socket error.
async fn drain_writes(
    stream: &TcpStream,
    inner: &Arc<ConnInner>,
    scratch: &mut [u8],
) -> Result<(), Error> {
    // Peek rather than read so a partial write keeps the remainder
    // buffered; discard only what the kernel accepted.
    let chunks = {
        let ob = inner.outbound.lock().unwrap();
        ob.peek(0).unwrap_or_default()
    };
    if chunks.is_empty() {
        return Ok(());
    }
    let mut sent = 0usize;
    'outer: for chunk in chunks {
        let mut off = 0usize;
        while off < chunk.len() {
            match stream.try_write(&chunk[off..]) {
                Ok(0) => break 'outer,
                Ok(n) => {
                    off += n;
                    sent += n;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break 'outer,
                Err(e) => return Err(Error::Io(e)),
            }
        }
    }
    if sent > 0 {
        inner.outbound.lock().unwrap().discard(sent);
    }
    let _ = scratch; // kept for symmetry with drain_reads
    Ok(())
}

/// Best-effort flush used during shutdown: one non-blocking pass,
/// then close regardless of what remains.
async fn flush_once(stream: &TcpStream, inner: &Arc<ConnInner>) {
    let mut scratch = vec![0u8; IO_BUF_SIZE];
    let _ = drain_writes(stream, inner, &mut scratch).await;
}

/// TCP connection actor.
async fn tcp_actor(
    conn_id: usize,
    stream: TcpStream,
    inner: Arc<ConnInner>,
    mut cmd_rx: mpsc::UnboundedReceiver<ConnCmd>,
    msg_tx: mpsc::UnboundedSender<LoopMsg>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut scratch = vec![0u8; IO_BUF_SIZE];
    let mut last_err: Option<Error> = None;

    // A receiver cloned after shutdown was requested starts at the
    // current version and would never observe `changed()`; check the
    // value directly.
    if *shutdown.borrow() {
        flush_once(&stream, &inner).await;
        let _ = msg_tx.send(LoopMsg::Closed(conn_id, last_err));
        return;
    }

    loop {
        let write_pending = inner.outbound.lock().unwrap().buffered() > 0;
        tokio::select! {
            biased;
            _ = shutdown.changed() => {
                // Shutdown requested (or sender dropped): best-effort
                // flush, then close.
                flush_once(&stream, &inner).await;
                break;
            }
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(ConnCmd::Close) | None => break,
                    Some(ConnCmd::Wake) => {
                        let _ = msg_tx.send(LoopMsg::Traffic(conn_id));
                    }
                }
            }
            _ = inner.outbound_notify.notified() => {
                if let Err(e) = drain_writes(&stream, &inner, &mut scratch).await {
                    last_err = Some(e);
                    break;
                }
            }
            res = stream.readable() => {
                match res {
                    Ok(()) => {
                        match drain_reads(&stream, &inner, &mut scratch, &msg_tx, conn_id).await {
                            Ok(true) => {}
                            Ok(false) => break, // EOF
                            Err(e) => {
                                last_err = Some(e);
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        last_err = Some(Error::Io(e));
                        break;
                    }
                }
            }
            _ = stream.writable(), if write_pending => {
                if let Err(e) = drain_writes(&stream, &inner, &mut scratch).await {
                    last_err = Some(e);
                    break;
                }
            }
        }
    }

    let _ = msg_tx.send(LoopMsg::Closed(conn_id, last_err));
}

/// UDP socket actor: one per bound socket. Each datagram becomes one
/// inbound write + one `Traffic` event; `remote_addr` on the `Conn`
/// reflects the latest sender.
async fn udp_actor(
    conn_id: usize,
    sock: UdpSocket,
    inner: Arc<ConnInner>,
    mut cmd_rx: mpsc::UnboundedReceiver<ConnCmd>,
    msg_tx: mpsc::UnboundedSender<LoopMsg>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut scratch = vec![0u8; 64 * 1024];
    let mut last_err: Option<Error> = None;

    // See tcp_actor: a post-shutdown receiver never sees changed().
    if *shutdown.borrow() {
        drain_udp_out(&sock, &inner).await;
        let _ = msg_tx.send(LoopMsg::Closed(conn_id, last_err));
        return;
    }

    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => {
                drain_udp_out(&sock, &inner).await;
                break;
            }
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(ConnCmd::Close) | None => break,
                    Some(ConnCmd::Wake) => {
                        let _ = msg_tx.send(LoopMsg::Traffic(conn_id));
                    }
                }
            }
            _ = inner.outbound_notify.notified() => {
                drain_udp_out(&sock, &inner).await;
            }
            res = sock.recv_from(&mut scratch) => {
                match res {
                    Ok((n, peer)) => {
                        *inner.udp_peer.lock().unwrap() = Some(peer);
                        {
                            let mut inbound = inner.inbound.lock().unwrap();
                            inbound.reset();
                            inbound.write(&scratch[..n]);
                        }
                        let _ = msg_tx.send(LoopMsg::Traffic(conn_id));
                    }
                    Err(e) => {
                        last_err = Some(Error::Io(e));
                        break;
                    }
                }
            }
        }
    }

    let _ = msg_tx.send(LoopMsg::Closed(conn_id, last_err));
}

/// Send all queued UDP datagrams to the latest known peer.
async fn drain_udp_out(sock: &UdpSocket, inner: &Arc<ConnInner>) {
    loop {
        let datagram = inner.udp_out.lock().unwrap().pop_front();
        let Some(data) = datagram else { break };
        let peer = *inner.udp_peer.lock().unwrap();
        let Some(peer) = peer else {
            // No peer seen yet: drop rather than block the actor.
            continue;
        };
        match sock.send_to(&data, peer).await {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                // Put it back and wait for the next notify.
                inner.udp_out.lock().unwrap().push_front(data);
                break;
            }
            Err(_) => break,
        }
    }
}
