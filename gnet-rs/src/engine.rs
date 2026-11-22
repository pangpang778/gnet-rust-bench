//! Engine: binds listeners, spawns event loops, coordinates shutdown.

use crate::conn::Proto;
use crate::eventloop::{EventLoop, Incoming, Listener, ListenerKind, LoopMailbox, LoopShared, WAKER_TOKEN};
use crate::handler::{Engine, EventHandler, Options};
use crate::loadbalancer::{make_balancer, ConnCounts};
use mio::net::{TcpListener as MioTcpListener, UdpSocket as MioUdpSocket};
use mio::{Poll, Waker};
use std::io;
use std::net::{SocketAddr, TcpListener, UdpSocket};
#[cfg(unix)]
#[cfg(unix)]
use std::os::unix::io::AsRawFd;
#[cfg(windows)]
use std::os::windows::io::{AsRawSocket, RawSocket};
#[cfg(windows)]
type RawFd = RawSocket;

#[inline]
fn sock_raw<T: SockHandle>(t: &T) -> RawFd {
    t.raw()
}
trait SockHandle {
    fn raw(&self) -> RawFd;
}
#[cfg(unix)]
impl<T: AsRawFd> SockHandle for T {
    fn raw(&self) -> RawFd { self.as_raw_fd() }
}
#[cfg(windows)]
impl<T: AsRawSocket> SockHandle for T {
    fn raw(&self) -> RawFd { self.as_raw_socket() }
}
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::Duration;

/// Shared engine state reachable from `Engine` facade and loop callbacks.
pub struct EngineInner {
    shared: Arc<LoopShared>,
    /// Loop threads; joined on shutdown.
    threads: Mutex<Vec<JoinHandle<io::Result<()>>>>,
    /// Address string for diagnostics.
    pub addr: String,
    pub proto: Proto,
    /// Unix socket path to remove on shutdown.
    unix_path: Option<String>,
    /// Set once run() finishes cleanup.
    done: AtomicBool,
}

impl EngineInner {
    pub fn new<H: EventHandler>(
        handler: H,
        proto: Proto,
        address: String,
        opts: Options,
    ) -> io::Result<Arc<Self>> {
        let num_loops = if opts.num_event_loop == 0 {
            num_cpus()
        } else {
            opts.num_event_loop.max(1)
        };
        // For UDP, a single loop is enough unless reuse_port (kernel hashing
        // needs multiple sockets). Keep it simple: UDP always uses the
        // ingress-dispatch model but all datagrams handled on loop 0.
        let effective_loops = if proto == Proto::Udp { num_loops.max(1) } else { num_loops };

        // Bind listeners.
        let listeners = bind_listeners(proto, &address, &opts, effective_loops)?;

        // Shared state.
        let conn_counts = Arc::new(ConnCounts::new(effective_loops));
        let shutdown = Arc::new(AtomicBool::new(false));
        let balancer = Arc::from(make_balancer(opts.load_balancing));
        let handler: Arc<dyn EventHandler> = Arc::new(handler);

        // Decide topology:
        // - reuse_port && num_loops>1 && proto!=udp: each loop binds its own
        //   listener (kernel LB). listeners_per_loop = effective_loops.
        // - else: single listener set on loop 0 (ingress), workers get conns
        //   via mailbox.
        let per_loop_sockets = opts.reuse_port && effective_loops > 1 && proto != Proto::Udp;

        // Build polls + mailboxes.
        // Mailbox i is for worker i; ingress is index 0 in `mailboxes`? We
        // give EVERY loop a mailbox slot so balancer can pick any index;
        // loop 0 also accepts. For per_loop_sockets mode, no mailboxes needed
        // (each loop accepts its own), but keep them for uniformity (unused).
        let mut polls = Vec::with_capacity(effective_loops);
        let mut wakers: Vec<Arc<Waker>> = Vec::with_capacity(effective_loops);
        for _ in 0..effective_loops {
            let poll = Poll::new()?;
            let waker = Arc::new(Waker::new(poll.registry(), mio::Token(WAKER_TOKEN))?);
            polls.push(poll);
            wakers.push(waker);
        }

        // Channels + mailbox structs (tx/waker only; rx lives in rx_slots).
        let mut mailboxes: Vec<Arc<LoopMailbox>> = Vec::with_capacity(effective_loops);
        let mut rx_slots: Vec<Arc<Mutex<mpsc::Receiver<Incoming>>>> = Vec::with_capacity(effective_loops);
        for i in 0..effective_loops {
            let (tx, rx) = mpsc::channel::<Incoming>();
            rx_slots.push(Arc::new(Mutex::new(rx)));
            mailboxes.push(Arc::new(LoopMailbox {
                tx,
                waker: wakers[i].clone(),
            }));
        }

        let shared = Arc::new(LoopShared {
            handler,
            read_cap: opts.read_buffer_cap.max(4096),
            write_cap: opts.write_buffer_cap.max(4096),
            tcp_no_delay: opts.tcp_no_delay,
            tcp_keepalive: opts.tcp_keepalive,
            ticker: opts.ticker,
            conn_counts,
            shutdown: shutdown.clone(),
            balancer,
            num_workers: effective_loops,
            mailboxes,
            next_conn_id: AtomicUsize::new(1),
            engine_inner: OnceLock::new(),
        });

        // Distribute listeners to loops.
        // listeners: Vec<Vec<Listener>> per loop index.
        let mut per_loop_listeners: Vec<Vec<Listener>> =
            (0..effective_loops).map(|_| Vec::new()).collect();
        if per_loop_sockets {
            // listeners[i] belongs to loop i (already one socket per loop).
            for (i, l) in listeners.into_iter().enumerate() {
                per_loop_listeners[i % effective_loops].push(l);
            }
        } else {
            // All listeners on loop 0 (ingress).
            per_loop_listeners[0] = listeners;
        }

        // Spawn threads. polls/wakers were created earlier; waker i belongs
        // to loop i and is also stored in mailboxes[i].waker.
        let mut threads = Vec::with_capacity(effective_loops);
        for i in 0..effective_loops {
            let shared = shared.clone();
            let poll = polls.remove(0);
            let waker = wakers.remove(0);
            let listeners = std::mem::take(&mut per_loop_listeners[i]);
            let rx = if per_loop_sockets {
                None
            } else {
                Some(rx_slots[i].clone())
            };
            let is_ingress = i == 0;
            let handle = std::thread::Builder::new()
                .name(format!("gnet-loop-{i}"))
                .spawn(move || {
                    let loop_ = EventLoop::new(i, is_ingress, shared, poll, waker, listeners, rx)?;
                    loop_.run()
                })?;
            threads.push(handle);
        }

        let unix_path = if proto == Proto::Unix {
            Some(address.clone())
        } else {
            None
        };
        let inner = Arc::new(Self {
            shared: shared.clone(),
            threads: Mutex::new(threads),
            addr: address,
            proto,
            unix_path,
            done: AtomicBool::new(false),
        });
        // Wire engine_inner for callbacks.
        let _ = shared.engine_inner.set(inner.clone());
        Ok(inner)
    }

    /// Access the shared handler (used by `run` to fire `on_boot`).
    pub fn shared_handler(&self) -> Arc<dyn EventHandler> {
        self.shared.handler.clone()
    }

    /// Count live connections across loops.
    pub fn count_connections(&self) -> usize {
        self.shared.conn_counts.snapshot().iter().sum()
    }

    pub fn is_shutdown(&self) -> bool {
        self.shared.shutdown.load(Ordering::Relaxed)
    }

    /// Signal shutdown and wait for all loops to exit.
    pub fn shutdown(&self) {
        self.shared.shutdown.store(true, Ordering::Relaxed);
        // Wake all worker loops so they notice promptly.
        for mb in &self.shared.mailboxes {
            let _ = mb.waker.wake();
        }
        // Wake ingress poll via its waker too (mailboxes[0] is loop 0's).
        // Loop 0 also has a mailbox slot; its waker wakes its poll.
        // Join threads.
        let mut threads = self.threads.lock().unwrap();
        for t in threads.drain(..) {
            let _ = t.join();
        }
        self.done.store(true, Ordering::Relaxed);
        // cleanup unix socket file
        if let Some(p) = &self.unix_path {
            let _ = std::fs::remove_file(p);
        }
        // on_shutdown callback
        let eng = Engine {
            inner: self.shared.engine_inner.get().unwrap().clone(),
        };
        self.shared.handler.on_shutdown(eng);
    }

    /// Blocking run: spawn already done in new(); this just waits for shutdown.
    /// Called by `handler::run`.
    pub fn run(self: &Arc<Self>, _eng: Engine) -> io::Result<()> {
        // Wait until shutdown flag set (by Action::Shutdown in a callback or
        // Engine::stop called from another thread). Poll with sleep.
        while !self.shared.shutdown.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_millis(10));
        }
        self.shutdown();
        Ok(())
    }

    /// Register an externally-connected TcpStream (client side).
    pub fn register_tcp(&self, stream: std::net::TcpStream) -> io::Result<()> {
        stream.set_nonblocking(true)?;
        let remote = stream.peer_addr()?;
        let local = stream.local_addr()?;
        // Hand to worker 0's mailbox (or least-loaded).
        let counts = self.shared.conn_counts.snapshot();
        let idx = self.shared.balancer.pick(&remote.to_string(), &counts);
        let mb = &self.shared.mailboxes[idx];
        let mio_stream = mio::net::TcpStream::from_std(stream);
        mb.tx.send(Incoming::Tcp {
            stream: mio_stream,
            local,
            remote,
        })
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "engine shutting down"))?;
        let _ = mb.waker.wake();
        Ok(())
    }

    /// Register an externally-connected UdpSocket (client side). For simplicity
    /// we hand it to loop 0 which registers it as a dedicated conn socket.
    pub fn register_udp(&self, sock: UdpSocket) -> io::Result<()> {
        // Not fully wired for per-worker UDP client sockets in this port;
        // return unsupported for now to keep scope tight.
        let _ = sock;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "register_udp not yet supported",
        ))
    }
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// Bind listeners. Returns one `Listener` per bound socket.
/// - TCP + reuse_port + loops>1: `loops` sockets, each SO_REUSEPORT.
/// - else: a single socket.
fn bind_listeners(
    proto: Proto,
    address: &str,
    opts: &Options,
    num_loops: usize,
) -> io::Result<Vec<Listener>> {
    match proto {
        Proto::Tcp => bind_tcp(address, opts, num_loops),
        Proto::Udp => bind_udp(address, opts),
        Proto::Unix => bind_unix(address, opts),
    }
}

fn bind_tcp(address: &str, opts: &Options, num_loops: usize) -> io::Result<Vec<Listener>> {
    let count = if opts.reuse_port && num_loops > 1 { num_loops } else { 1 };
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let std_listener = socket2_like_tcp_bind(address, opts.reuse_addr, opts.reuse_port)?;
        std_listener.set_nonblocking(true)?;
        let local = std_listener.local_addr()?;
        let mio = MioTcpListener::from_std(std_listener);
        out.push(Listener {
            token: crate::eventloop::LISTENER_BASE + i,
            kind: ListenerKind::Tcp(mio),
            local_addr: local,
            unix_path: None,
            udp_peers: Mutex::new(std::collections::HashMap::new()),
        });
    }
    Ok(out)
}

fn bind_udp(address: &str, _opts: &Options) -> io::Result<Vec<Listener>> {
    let std_sock = UdpSocket::bind(address)?;
    std_sock.set_nonblocking(true)?;
    // SO_REUSEADDR for UDP (unix only; Windows semantics differ).
    #[cfg(unix)]
    unsafe {
        let one: libc::c_int = 1;
        libc::setsockopt(
            std_sock.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_REUSEADDR,
            &one as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
    let local = std_sock.local_addr()?;
    let clone_for_mio = std_sock.try_clone()?;
    clone_for_mio.set_nonblocking(true)?;
    let mio = MioUdpSocket::from_std(clone_for_mio);
    Ok(vec![Listener {
        token: crate::eventloop::LISTENER_BASE,
        kind: ListenerKind::Udp {
            mio,
            std_sock,
        },
        local_addr: local,
        unix_path: None,
        udp_peers: Mutex::new(std::collections::HashMap::new()),
    }])
}

fn bind_unix(address: &str, _opts: &Options) -> io::Result<Vec<Listener>> {
    // Remove stale socket file.
    let _ = std::fs::remove_file(address);
    // std UnixListener -> mio doesn't have unix listener on all platforms;
    // use std listener + manual accept? mio supports UnixListener on unix.
    // For scope, use std::os::unix::net::UnixListener wrapped manually is
    // not mio-registrable. We'll bind via mio's TcpListener? No.
    // Simplest: use std UnixListener in blocking? Must be nonblocking + poll.
    // mio::net::UnixListener exists on unix. Use it.
    #[cfg(unix)]
    {
        let std_l = std::os::unix::net::UnixListener::bind(address)?;
        std_l.set_nonblocking(true)?;
        let mio = mio::net::UnixListener::from_std(std_l);
        // We need ListenerKind::Unix variant — but we didn't define one.
        // Treat unix conns as TCP-like stream. Add variant via Tcp? Can't.
        // For this port, unix:// binds a UnixListener and we accept via a
        // dedicated code path. To keep ListenerKind small, we add Unix variant.
        Ok(vec![Listener {
            token: crate::eventloop::LISTENER_BASE,
            kind: ListenerKind::Unix(mio),
            local_addr: SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), 0),
            unix_path: Some(address.to_string()),
            udp_peers: Mutex::new(std::collections::HashMap::new()),
        }])
    }
    #[cfg(not(unix))]
    {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "unix sockets not supported on this platform",
        ))
    }
}

/// Minimal TCP bind with SO_REUSEADDR / SO_REUSEPORT. We avoid the socket2
/// dependency by using libc setsockopt on a manually created socket? Easier:
/// use std TcpListener::bind then set reuseport is too late (must be before
/// bind). So we create via socket(2)? That's a lot of unsafe. Alternative:
/// add socket2 dependency — it's the standard, tiny, and exactly for this.
/// We'll add it to Cargo.toml.
fn socket2_like_tcp_bind(
    address: &str,
    reuse_addr: bool,
    _reuse_port: bool,
) -> io::Result<TcpListener> {
    use socket2::{Domain, Protocol, Socket, Type};
    let addr = address
        .to_socket_addrs_first()?;
    let domain = if addr.is_ipv6() {
        Domain::IPV6
    } else {
        Domain::IPV4
    };
    let sock = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    if reuse_addr {
        sock.set_reuse_address(true)?;
    }
    // SO_REUSEPORT exists on unix; on Windows socket2 doesn't expose it and
    // the semantics differ anyway — skip.
    #[cfg(unix)]
    if reuse_port {
        sock.set_reuse_port(true)?;
    }
    sock.set_nonblocking(true)?;
    sock.bind(&addr.into())?;
    sock.listen(1024)?;
    let std_listener: TcpListener = sock.into();
    std_listener.set_nonblocking(true)?;
    Ok(std_listener)
}

trait ToSocketAddrsFirst {
    fn to_socket_addrs_first(&self) -> io::Result<SocketAddr>;
}

impl ToSocketAddrsFirst for str {
    fn to_socket_addrs_first(&self) -> io::Result<SocketAddr> {
        use std::net::ToSocketAddrs;
        self.to_socket_addrs()?
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "cannot resolve address"))
    }
}
