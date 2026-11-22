//! A single event loop: owns a `mio::Poll`, a set of connections, and runs
//! the accept/read/write dispatch for them on its own thread.
//!
//! Loop 0 is the *ingress*: it owns the listeners, accepts TCP connections
//! and reads UDP datagrams, then hands them to worker loops through per-loop
//! mailboxes (mpsc + mio Waker). Worker loops register the handed-off sockets
//! and run `on_open`/`on_traffic`/`on_close`.

use crate::conn::{Conn, Proto};
use crate::handler::{Action, Engine, EventHandler};
use crate::loadbalancer::ConnCounts;
use mio::net::{TcpListener as MioTcpListener, TcpStream as MioTcpStream, UdpSocket as MioUdpSocket};
use mio::{Events, Interest, Poll, Registry, Token, Waker};
use std::collections::HashMap;
use std::io;
use std::net::{SocketAddr, UdpSocket};
#[cfg(unix)]
use std::os::unix::io::AsRawFd;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// Token reserved for "drain mailbox" wakeups and listener events.
/// Listener tokens are `LISTENER_BASE + slot`; conn tokens are conn ids.
pub const WAKER_TOKEN: usize = usize::MAX;
pub const LISTENER_BASE: usize = usize::MAX - 1024; // up to 1024 listeners

/// A unit of work handed from ingress to a worker loop.
pub enum Incoming {
    /// A freshly accepted TCP connection.
    Tcp {
        stream: MioTcpStream,
        local: SocketAddr,
        remote: SocketAddr,
    },
    /// A freshly accepted unix-domain connection (unix only).
    #[cfg(unix)]
    Unix {
        stream: mio::net::UnixStream,
        local: SocketAddr,
        remote: SocketAddr,
    },
}

/// Per-worker-loop mailbox: ingress pushes `Incoming`, the loop's Waker
/// interrupts `poll()` so it drains promptly. The receiving end lives in
/// `rx_slots` inside EngineInner (one per loop).
pub struct LoopMailbox {
    pub tx: mpsc::Sender<Incoming>,
    /// Shared with the owning loop's `EventLoop.waker`; `wake()` needs `&self`
    /// but poll registration needs `&mut`, hence the Mutex.
    pub waker: Arc<Waker>,
}

/// State shared by all loops and the engine.
pub struct LoopShared {
    pub handler: Arc<dyn EventHandler>,
    pub read_cap: usize,
    pub write_cap: usize,
    pub tcp_no_delay: bool,
    pub tcp_keepalive: Option<Duration>,
    pub ticker: bool,
    pub conn_counts: Arc<ConnCounts>,
    pub shutdown: Arc<AtomicBool>,
    pub balancer: Arc<dyn crate::loadbalancer::Balancer>,
    pub num_workers: usize,
    pub mailboxes: Vec<Arc<LoopMailbox>>,
    pub next_conn_id: AtomicUsize,
    /// Set by EngineInner once constructed (Engine facade needs it for
    /// count_connections/stop inside callbacks).
    pub engine_inner: OnceLock<Arc<crate::engine::EngineInner>>,
}

impl LoopShared {
    pub fn engine(&self) -> Engine {
        Engine {
            inner: self
                .engine_inner
                .get()
                .expect("engine_inner not set")
                .clone(),
        }
    }
}

/// Listener registered on the ingress loop.
pub struct Listener {
    pub token: usize,
    pub kind: ListenerKind,
    pub local_addr: SocketAddr,
    pub unix_path: Option<String>,
    /// For UDP: maps peer addr -> conn id on whichever loop owns it.
    /// Only used on ingress; workers keep their own map.
    pub udp_peers: Mutex<HashMap<SocketAddr, usize>>,
}

impl Listener {
    /// For UDP listeners, returns the std socket clone used for per-peer conns.
    fn udp_std_sock(&self) -> &UdpSocket {
        match &self.kind {
            ListenerKind::Udp { std_sock, .. } => std_sock,
            _ => panic!("not a udp listener"),
        }
    }
}

pub enum ListenerKind {
    Tcp(MioTcpListener),
    /// `mio` is registered with the Poll; `std_sock` is a clone used to hand
    /// out per-peer conn sockets (send_to) without registering them.
    Udp {
        mio: MioUdpSocket,
        std_sock: UdpSocket,
    },
    /// Unix domain socket listener (unix only).
    #[cfg(unix)]
    Unix(mio::net::UnixListener),
}

/// One event-loop thread.
pub struct EventLoop {
    pub idx: usize,
    /// True for loop 0 (owns listeners, accepts, dispatches).
    pub is_ingress: bool,
    shared: Arc<LoopShared>,
    poll: Poll,
    /// The waker registered with `poll` for mailbox wakeups. Shared (via Arc)
    /// with `LoopMailbox.waker` so ingress can wake us.
    waker: Arc<Waker>,
    listeners: Vec<Listener>,
    mailbox_rx: Option<Arc<Mutex<mpsc::Receiver<Incoming>>>>,
    /// conn id -> Conn
    conns: HashMap<usize, Conn>,
    /// For UDP workers: (listener_token, peer) -> conn id
    udp_conns: HashMap<(usize, SocketAddr), usize>,
    read_buf: Vec<u8>,
    next_tick: Instant,
    tick_interval: Duration,
    tick_armed: bool,
}

impl EventLoop {
    /// Create a loop. `poll`/`waker` are created by the engine so the mailbox
    /// waker can be shared with the ingress dispatcher before the thread starts.
    pub fn new(
        idx: usize,
        is_ingress: bool,
        shared: Arc<LoopShared>,
        poll: Poll,
        waker: Arc<Waker>,
        listeners: Vec<Listener>,
        mailbox_rx: Option<Arc<Mutex<mpsc::Receiver<Incoming>>>>,
    ) -> io::Result<Self> {
        Ok(Self {
            idx,
            is_ingress,
            shared,
            poll,
            waker,
            listeners,
            mailbox_rx,
            conns: HashMap::new(),
            udp_conns: HashMap::new(),
            read_buf: vec![0u8; 64 * 1024],
            next_tick: Instant::now(),
            tick_interval: Duration::from_secs(3600 * 24 * 365),
            tick_armed: false,
        })
    }

    pub fn run(mut self) -> io::Result<()> {
        let registry = self.poll.registry().try_clone()?;
        let mut events = Events::with_capacity(1024);

        // The mailbox waker self-registers with the poll at construction
        // (mio::Waker::new). Nothing to register here.

        // Register listeners (one-time, mutable borrows).
        for l in self.listeners.iter_mut() {
            match &mut l.kind {
                ListenerKind::Tcp(t) => {
                    self.poll.registry().register(t, Token(l.token), Interest::READABLE)?;
                }
                ListenerKind::Udp { mio, .. } => {
                    self.poll.registry().register(mio, Token(l.token), Interest::READABLE)?;
                }
                #[cfg(unix)]
                ListenerKind::Unix(u) => {
                    self.poll.registry().register(u, Token(l.token), Interest::READABLE)?;
                }
            }
        }

        // Arm ticker on ingress if requested.
        if self.is_ingress && self.shared.ticker {
            let (d, _) = self.shared.handler.on_tick();
            self.tick_interval = d;
            self.next_tick = Instant::now() + d;
            self.tick_armed = true;
        }

        loop {
            if self.shared.shutdown.load(Ordering::Relaxed) {
                break;
            }

            let timeout = if self.tick_armed {
                self.next_tick
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(500))
            } else {
                Duration::from_millis(500)
            };

            self.poll.poll(&mut events, Some(timeout))?;

            // Drain mailbox.
            self.drain_mailbox(&mut events)?;

            for event in events.iter() {
                let token = event.token().0;
                if token == WAKER_TOKEN {
                    continue; // already drained
                }
                if self.listeners.iter().any(|l| l.token == token) {
                    if let Err(e) = self.handle_listener(token, &registry) {
                        if e.kind() != io::ErrorKind::WouldBlock {
                            // Log and continue; listener errors are usually fatal.
                            eprintln!("gnet-rs: listener error on loop {}: {}", self.idx, e);
                        }
                    }
                    continue;
                }
                if let Err(e) = self.handle_conn(token, event.is_readable(), event.is_writable(), &registry) {
                    eprintln!("gnet-rs: conn error on loop {}: {}", self.idx, e);
                }
            }

            if self.tick_armed && Instant::now() >= self.next_tick {
                let (_, action) = self.shared.handler.on_tick();
                self.next_tick = Instant::now() + self.tick_interval;
                if action == Action::Shutdown {
                    self.shared.shutdown.store(true, Ordering::Relaxed);
                    break;
                }
            }
        }

        self.shutdown_all_conns();
        Ok(())
    }

    /// Pull all pending `Incoming` from our mailbox and register them.
    fn drain_mailbox(&mut self, _events: &mut Events) -> io::Result<()> {
        let Some(rx_arc) = self.mailbox_rx.clone() else { return Ok(()) };
        // Drain the channel while holding the lock, collect items, release,
        // then process (accept_incoming needs &mut self).
        let mut pending = Vec::new();
        {
            let rx = rx_arc.lock().unwrap();
            loop {
                match rx.try_recv() {
                    Ok(incoming) => pending.push(incoming),
                    Err(_) => break,
                }
            }
        }
        for incoming in pending {
            self.accept_incoming(incoming)?;
        }
        Ok(())
    }

    fn accept_incoming(&mut self, incoming: Incoming) -> io::Result<()> {
        match incoming {
            Incoming::Tcp { stream, local, remote } => {
                let id = self.shared.next_conn_id.fetch_add(1, Ordering::Relaxed);
                let mut conn = Conn::new_tcp(
                    id,
                    stream.into(),
                    local,
                    remote,
                    self.shared.write_cap,
                    self.shared.read_cap,
                )?;
                // Re-wrap as mio TcpStream for registration? Conn stores std
                // TcpStream. mio::Poll::register needs a mio Source. std
                // TcpStream does NOT implement mio::event::Source on unix?
                // Actually mio implements Source for std::net::TcpStream since
                // mio 0.8 via `impl Source for TcpStream`? No — mio provides
                // `mio::net::TcpStream`. We should keep the mio stream.
                // Conn::new_tcp takes std TcpStream and sets nonblocking.
                // For registration we need mio::net::TcpStream. Convert back:
                let mut mio_stream = MioTcpStream::from_std(
                    conn.try_clone_stream()?,
                );
                self.apply_tcp_opts(&mut conn);
                let interests = Interest::READABLE;
                self.poll.registry().register(&mut mio_stream, Token(id), interests)?;
                conn.set_mio_tcp(mio_stream);
                let (out, action) = self.shared.handler.on_open(&mut conn);
                if let Some(b) = out {
                    let _ = conn.send(&b);
                }
                if !conn.outbound.is_empty() {
                    let _ = self.poll.registry().reregister(
                        conn.mio_tcp_mut().unwrap(),
                        Token(id),
                        Interest::READABLE.add(Interest::WRITABLE),
                    );
                }
                self.shared.conn_counts.inc(self.idx);
                self.conns.insert(id, conn);
                self.post_open_action(id, action);
            }
            #[cfg(unix)]
            Incoming::Unix { stream, local, remote } => {
                let id = self.shared.next_conn_id.fetch_add(1, Ordering::Relaxed);
                let mut conn = Conn::new_unix(
                    id, stream, local, remote,
                    self.shared.write_cap, self.shared.read_cap,
                )?;
                self.poll.registry().register(conn.unix_mut().unwrap(), Token(id), Interest::READABLE)?;
                let (out, action) = self.shared.handler.on_open(&mut conn);
                if let Some(b) = out { let _ = conn.send(&b); }
                if !conn.outbound.is_empty() {
                    let _ = self.poll.registry().reregister(
                        conn.unix_mut().unwrap(),
                        Token(id),
                        Interest::READABLE.add(Interest::WRITABLE),
                    );
                }
                self.shared.conn_counts.inc(self.idx);
                self.conns.insert(id, conn);
                self.post_open_action(id, action);
            }
        }
        Ok(())
    }

    fn post_open_action(&mut self, id: usize, action: Action) {
        match action {
            Action::None => {}
            Action::Shutdown => {
                self.shared.shutdown.store(true, Ordering::Relaxed);
            }
            Action::Close => {
                if let Some(mut c) = self.conns.remove(&id) {
                    self.do_close(&mut c, None);
                    self.shared.conn_counts.dec(self.idx);
                }
            }
        }
    }

    /// UDP datagram on the ingress loop: maintain per-peer conn that borrows
    /// a try_clone of the listener's socket for sending replies.
    fn handle_udp_datagram(
        &mut self,
        sock_token: usize,
        std_sock: &UdpSocket,
        local: SocketAddr,
        peer: SocketAddr,
        data: Vec<u8>,
    ) -> io::Result<()> {
        let key = (sock_token, peer);
        let id = match self.udp_conns.get(&key) {
            Some(&id) => id,
            None => {
                let id = self.shared.next_conn_id.fetch_add(1, Ordering::Relaxed);
                let sock2 = std_sock.try_clone()?;
                sock2.set_nonblocking(true)?;
                let mut conn = Conn::new_udp_socket(
                    id, sock2, local,
                    self.shared.write_cap, self.shared.read_cap,
                )?;
                conn.udp_peer = Some(peer);
                conn.remote_addr = peer;
                conn.opened = true;
                // UDP conns are NOT registered with poll individually; the
                // shared listener socket already is. on_open fires on first
                // packet (matches gnet behavior of creating conn on traffic).
                let (out, action) = self.shared.handler.on_open(&mut conn);
                if let Some(b) = out { let _ = conn.send(&b); }
                self.shared.conn_counts.inc(self.idx);
                self.conns.insert(id, conn);
                self.udp_conns.insert(key, id);
                self.post_open_action(id, action);
                if self.shared.shutdown.load(Ordering::Relaxed) {
                    return Ok(());
                }
                *self.udp_conns.get(&key).unwrap()
            }
        };
        let conn = match self.conns.get_mut(&id) {
            Some(c) => c,
            None => {
                self.udp_conns.remove(&key);
                return Ok(());
            }
        };
        conn.udp_peer = Some(peer);
        conn.remote_addr = peer;
        conn.inbound.set_current(&data);
        let action = self.shared.handler.on_traffic(conn);
        conn.inbound.compact();
        match action {
            Action::None => {}
            Action::Close | Action::Shutdown => {
                if let Some(mut c) = self.conns.remove(&id) {
                    self.do_close(&mut c, None);
                    self.shared.conn_counts.dec(self.idx);
                }
                self.udp_conns.remove(&key);
                if action == Action::Shutdown {
                    self.shared.shutdown.store(true, Ordering::Relaxed);
                }
            }
        }
        Ok(())
    }

    fn apply_tcp_opts(&self, conn: &mut Conn) {
        if self.shared.tcp_no_delay {
            if let Some(s) = conn.stream_mut() {
                let _ = s.set_nodelay(true);
            }
        }
        if let Some(d) = self.shared.tcp_keepalive {
            set_keepalive(conn.fd(), d);
        }
    }

    fn handle_listener(&mut self, token: usize, registry: &Registry) -> io::Result<()> {
        let idx = self
            .listeners
            .iter()
            .position(|l| l.token == token)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "listener token"))?;
        // We need to borrow listener and mutate self.conns — split borrows by
        // taking the listener kind out temporarily is messy. Instead match on
        // a cloned reference: ListenerKind isn't Clone (mio types are). Use
        // raw approach: get raw fd and accept via std? mio TcpListener::accept
        // needs &self. We can borrow self.listeners immutably while mutating
        // self.conns? No — both are fields of self; disjoint field borrows are
        // allowed within a method if we access via self.field directly, but
        // calling &mut self methods conflicts. We'll inline logic using
        // field-level borrows.
        // Collect listener metadata up front so the match borrow on
        // self.listeners doesn't conflict with &mut self calls inside.
        let local_addr = self.listeners[idx].local_addr;
        #[cfg(unix)]
        let unix_path = self.listeners[idx].unix_path.clone();
        let listener_token = self.listeners[idx].token;

        // Phase 1 (immutable borrow of self.listeners): accept/read raw events
        // into owned values.
        enum RawEvent {
            Tcp(MioTcpStream, SocketAddr),
            #[cfg(unix)]
            Unix(mio::net::UnixStream),
            Udp(Vec<u8>, SocketAddr),
        }
        let mut raw_events = Vec::new();
        match &self.listeners[idx].kind {
            ListenerKind::Tcp(tcp) => {
                loop {
                    match tcp.accept() {
                        Ok((s, remote)) => raw_events.push(RawEvent::Tcp(s, remote)),
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                        Err(e) => return Err(e),
                    }
                }
            }
            #[cfg(unix)]
            ListenerKind::Unix(unix_l) => {
                loop {
                    match unix_l.accept() {
                        Ok((s, _)) => raw_events.push(RawEvent::Unix(s)),
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                        Err(e) => return Err(e),
                    }
                }
            }
            ListenerKind::Udp { mio, .. } => {
                let mut buf = vec![0u8; self.shared.read_cap.max(2048)];
                match mio.recv_from(&mut buf) {
                    Ok((n, peer)) => raw_events.push(RawEvent::Udp(buf[..n].to_vec(), peer)),
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                    Err(e) => return Err(e),
                }
            }
        }

        // Phase 2 (mutable self): dispatch collected events.
        for ev in raw_events {
            match ev {
                RawEvent::Tcp(mio_stream, remote) => {
                    if self.is_ingress {
                        let counts = self.shared.conn_counts.snapshot();
                        let widx = self.shared.balancer.pick(&remote.to_string(), &counts);
                        let mb = &self.shared.mailboxes[widx];
                        let std_stream: std::net::TcpStream = mio_stream.into();
                        std_stream.set_nonblocking(true)?;
                        let _ = mb.tx.send(Incoming::Tcp {
                            stream: MioTcpStream::from_std(std_stream),
                            local: local_addr,
                            remote,
                        });
                        let _ = mb.waker.wake();
                    } else {
                        self.accept_tcp_local(mio_stream, local_addr, remote)?;
                    }
                }
                #[cfg(unix)]
                RawEvent::Unix(stream) => {
                    let remote = local_addr;
                    if self.is_ingress {
                        let counts = self.shared.conn_counts.snapshot();
                        let key = format!("unix://{}", unix_path.clone().unwrap_or_default());
                        let widx = self.shared.balancer.pick(&key, &counts);
                        let mb = &self.shared.mailboxes[widx];
                        let _ = mb.tx.send(Incoming::Unix {
                            stream,
                            local: local_addr,
                            remote,
                        });
                        let _ = mb.waker.wake();
                    } else {
                        self.accept_unix_local(stream, local_addr, remote)?;
                    }
                }
                RawEvent::Udp(data, peer) => {
                    // Clone the std socket handle out first to end the borrow.
                    let sock = match &self.listeners[idx].kind {
                        ListenerKind::Udp { std_sock, .. } => std_sock.try_clone()?,
                        _ => unreachable!(),
                    };
                    self.handle_udp_datagram(listener_token, &sock, local_addr, peer, data)?;
                }
            }
        }
        let _ = registry;
        Ok(())
    }

    fn accept_tcp_local(
        &mut self,
        mio_stream: MioTcpStream,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> io::Result<()> {
        let id = self.shared.next_conn_id.fetch_add(1, Ordering::Relaxed);
        let std_stream: std::net::TcpStream = mio_stream.into();
        std_stream.set_nonblocking(true)?;
        let mut conn = Conn::new_tcp(
            id, std_stream, local, remote,
            self.shared.write_cap, self.shared.read_cap,
        )?;
        self.apply_tcp_opts(&mut conn);
        let mut mio = MioTcpStream::from_std(conn.try_clone_stream()?);
        self.poll.registry().register(&mut mio, Token(id), Interest::READABLE)?;
        conn.set_mio_tcp(mio);
        let (out, action) = self.shared.handler.on_open(&mut conn);
        if let Some(b) = out { let _ = conn.send(&b); }
        self.shared.conn_counts.inc(self.idx);
        self.conns.insert(id, conn);
        self.post_open_action(id, action);
        Ok(())
    }

    #[cfg(unix)]
    fn accept_unix_local(
        &mut self,
        stream: mio::net::UnixStream,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> io::Result<()> {
        let id = self.shared.next_conn_id.fetch_add(1, Ordering::Relaxed);
        let mut conn = Conn::new_unix(
            id, stream, local, remote,
            self.shared.write_cap, self.shared.read_cap,
        )?;
        self.poll.registry().register(conn.unix_mut().unwrap(), Token(id), Interest::READABLE)?;
        let (out, action) = self.shared.handler.on_open(&mut conn);
        if let Some(b) = out { let _ = conn.send(&b); }
        self.shared.conn_counts.inc(self.idx);
        self.conns.insert(id, conn);
        self.post_open_action(id, action);
        Ok(())
    }

    fn handle_conn(
        &mut self,
        token: usize,
        readable: bool,
        writable: bool,
        registry: &Registry,
    ) -> io::Result<()> {
        if !self.conns.contains_key(&token) {
            return Ok(()); // stale event
        }
        let mut close = false;
        let mut shutdown = false;
        let mut fatal: Option<io::Error> = None;

        if readable {
            // Take conn out to satisfy borrow checker across handler calls.
            let mut conn = self.conns.remove(&token).unwrap();
            // Move read_buf out to avoid clone-per-read.
            let mut buf = std::mem::take(&mut self.read_buf);
            match conn.proto {
                Proto::Tcp => {
                    loop {
                        match conn.read_once(&mut buf) {
                            Ok((n, eof)) if n > 0 => {
                                let data = buf[..n].to_vec();
                                conn.inbound.set_current(&data);
                                let action = self.shared.handler.on_traffic(&mut conn);
                                conn.inbound.compact();
                                match action {
                                    Action::None => {}
                                    Action::Close => { close = true; break; }
                                    Action::Shutdown => { shutdown = true; break; }
                                }
                                if eof {
                                    conn.is_eof = true;
                                    break;
                                }
                            }
                            Ok((_, true)) => { conn.is_eof = true; break; }
                            Ok((_, false)) => break,
                            Err(e) => { fatal = Some(e); break; }
                        }
                    }
                }
                Proto::Udp => {
                    match conn.read_udp_once(&mut buf) {
                        Ok((n, Some(peer))) if n > 0 => {
                            conn.udp_peer = Some(peer);
                            conn.remote_addr = peer;
                            let data = buf[..n].to_vec();
                            conn.inbound.set_current(&data);
                            let action = self.shared.handler.on_traffic(&mut conn);
                            conn.inbound.compact();
                            match action {
                                Action::None => {}
                                Action::Close => close = true,
                                Action::Shutdown => shutdown = true,
                            }
                        }
                        Ok(_) => {}
                        Err(e) => fatal = Some(e),
                    }
                }
                Proto::Unix => {
                    #[cfg(unix)]
                    {
                        match conn.read_once(&mut buf) {
                            Ok((n, eof)) if n > 0 => {
                                let data = buf[..n].to_vec();
                                conn.inbound.set_current(&data);
                                let action = self.shared.handler.on_traffic(&mut conn);
                                conn.inbound.compact();
                                match action {
                                    Action::None => {}
                                    Action::Close => { close = true; break; }
                                    Action::Shutdown => { shutdown = true; break; }
                                }
                                if eof {
                                    conn.is_eof = true;
                                    break;
                                }
                            }
                            Ok((_, true)) => { conn.is_eof = true; break; }
                            Ok((_, false)) => break,
                            Err(e) => { fatal = Some(e); break; }
                        }
                    }
                    #[cfg(not(unix))]
                    {
                        let _ = (&mut conn, &mut buf);
                    }
                }
            }
            self.read_buf = buf;
            self.conns.insert(token, conn);
        }

        if writable && !close && !shutdown && fatal.is_none() {
            let conn = self.conns.get_mut(&token).unwrap();
            if let Err(e) = conn.flush_outbound() {
                fatal = Some(e);
            } else if conn.outbound.is_empty() {
                // drop WRITABLE
                match conn.proto {
                    Proto::Tcp => {
                        if let Some(s) = conn.mio_tcp_mut() {
                            let _ = self.poll.registry().reregister(s, Token(token), Interest::READABLE);
                        }
                    }
                    #[cfg(unix)]
                    Proto::Unix => {
                        if let Some(s) = conn.unix_mut() {
                            let _ = self.poll.registry().reregister(s, Token(token), Interest::READABLE);
                        }
                    }
                    #[cfg(not(unix))]
                    Proto::Unix => {}
                    _ => {}
                }
            }
        }

        if fatal.is_some() || close {
            let mut conn = self.conns.remove(&token).unwrap();
            let err = fatal.as_ref().map(|e| e as &io::Error);
            self.do_close(&mut conn, err);
            self.shared.conn_counts.dec(self.idx);
        }
        if shutdown {
            self.shared.shutdown.store(true, Ordering::Relaxed);
        }
        let _ = registry;
        Ok(())
    }

    fn do_close(&self, conn: &mut Conn, err: Option<&io::Error>) {
        let _ = conn.flush_outbound();
        let action = self.shared.handler.on_close(conn, err);
        match conn.proto {
            Proto::Tcp => {
                if let Some(s) = conn.mio_tcp_mut() {
                    let _ = self.poll.registry().deregister(s);
                }
            }
            #[cfg(unix)]
            Proto::Unix => {
                if let Some(s) = conn.unix_mut() {
                    let _ = self.poll.registry().deregister(s);
                }
            }
            #[cfg(not(unix))]
            Proto::Unix => {}
            Proto::Udp => {
                if let Some(s) = conn.mio_udp_mut() {
                    let _ = self.poll.registry().deregister(s);
                }
            }
        }
        conn.opened = false;
        if action == Action::Shutdown {
            self.shared.shutdown.store(true, Ordering::Relaxed);
        }
    }

    fn shutdown_all_conns(&mut self) {
        let ids: Vec<usize> = self.conns.keys().cloned().collect();
        for id in ids {
            if let Some(mut c) = self.conns.remove(&id) {
                let _ = self.shared.handler.on_close(&mut c, None);
                self.shared.conn_counts.dec(self.idx);
            }
        }
        // udp_conns reference conns already closed; clear map.
        self.udp_conns.clear();
    }
}

#[cfg(unix)]
fn set_keepalive(fd: crate::conn::RawFd, idle: Duration) {
    unsafe {
        let one: libc::c_int = 1;
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_KEEPALIVE,
            &one as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
        let secs = idle.as_secs().max(1) as libc::c_int;
        #[cfg(target_os = "linux")]
        {
            libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_KEEPIDLE,
                &secs as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
        #[cfg(any(target_os = "macos", target_os = "freebsd", target_os = "dragonfly", target_os = "openbsd", target_os = "netbsd"))]
        {
            let mut tv = libc::timeval {
                tv_sec: secs as libc::time_t,
                tv_usec: 0,
            };
            libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_KEEPALIVE,
                &mut tv as *mut _ as *mut libc::c_void,
                std::mem::size_of::<libc::timeval>() as libc::socklen_t,
            );
        }
    }
}

#[cfg(not(unix))]
fn set_keepalive(_fd: crate::conn::RawFd, _idle: Duration) {
    // Windows keepalive via socket options is out of scope for this port;
    // TCP keepalive is rarely used on Windows servers anyway.
}
