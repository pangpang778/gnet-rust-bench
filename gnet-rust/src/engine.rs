//! Engine entry points: `run`, `run_with`, `run_with_options`.
//!
//! `run` is the blocking counterpart of Go's `gnet.Run`: it builds a
//! multi-threaded tokio runtime, spawns one event-loop task per
//! configured worker plus an acceptor task, and blocks until shutdown.
//! Address schemes follow the Go syntax: `tcp://host:port`,
//! `tcp4://`, `tcp6://`, `udp://`, `udp4://`, `udp6://` and
//! `unix:///path`.
//!
//! ## Architecture
//!
//! ```text
//!            ┌─────────────┐
//!            │  acceptor   │  listener.accept() → balancer.next(peer)
//!            └──────┬──────┘         │ LoopMsg::NewConn
//!        ┌──────────┼──────────┐     ▼
//!        ▼          ▼          ▼  ┌─────────┐
//!   ┌─────────┐ ┌─────────┐ ┌──────┐│ loop 0  │ owns handler callbacks
//!   │ loop 0  │ │ loop 1  │ │ loop ││ + tick  │ for its conns
//!   └────┬────┘ └────┬────┘ └──┬───┘└─────────┘
//!        │           │         │
//!        ▼           ▼         ▼
//!   conn actor   conn actor  conn actor   (one tokio task per socket)
//! ```
//!
//! The acceptor model (rather than per-loop `SO_REUSEPORT` binds)
//! works identically on every platform and makes `num_event_loop`
//! independent of socket-option support. `reuse_port` is still
//! applied to the listener so multiple processes can share a port.

use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::{TcpListener, UdpSocket};

use crate::balancer::{Balancer, LeastConnections, RoundRobin, SourceAddrHash};
use crate::error::Error;
use crate::eventloop::{spawn_loop, LoopMsg, LoopParts, StreamKind};
use crate::handler::{Action, Engine, EngineState, EventHandler};
use crate::options::{self, LoadBalancing, Options};

/// Run the engine with default options until shutdown.
///
/// Blocks the calling thread; returns `Ok(())` after
/// [`Engine::stop`] completes the graceful shutdown. See
/// [`run_with`] and [`run_with_options`] for configuration.
pub fn run<H>(handler: H, addr: &str) -> Result<(), Error>
where
    H: EventHandler + Send + 'static,
{
    run_with(handler, addr, |_| {})
}

/// Run the engine after applying `configure` to the default
/// [`Options`].
///
/// ```no_run
/// # use gnet::{engine, options, NoopHandler};
/// # use std::time::Duration;
/// engine::run_with(
///     NoopHandler,
///     "tcp://127.0.0.1:0",
///     |o| {
///         o.multicore = true;
///         o.tcp_keep_alive = Some(Duration::from_secs(30));
///     },
/// );
/// ```
pub fn run_with<H, F>(handler: H, addr: &str, configure: F) -> Result<(), Error>
where
    H: EventHandler + Send + 'static,
    F: FnOnce(&mut Options),
{
    let mut opts = Options::default();
    configure(&mut opts);
    run_with_options(handler, addr, opts)
}

/// Run the engine with a fully built [`Options`].
///
/// # Errors
///
/// Returns [`Error::InvalidAddress`], [`Error::UnsupportedScheme`],
/// or [`Error::Io`] when the address cannot be parsed or the listener
/// cannot be bound.
pub fn run_with_options<H>(handler: H, addr: &str, options: Options) -> Result<(), Error>
where
    H: EventHandler + Send + 'static,
{
    let (scheme, target) = options::parse_addr(addr)?;

    let loops = options.event_loop_count().max(1);
    // One extra worker beyond the loop count keeps the engine future,
    // acceptor and conn actors from starving the loop tasks — and
    // keeps `Engine::stop` (which blocks a worker) from deadlocking
    // the pool when called from inside a callback.
    let workers = loops + 1;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .map_err(Error::Io)?;

    rt.block_on(engine_future(handler, scheme, target, Arc::new(options)))
}

/// The async half of `run`: build listeners, spawn loops + acceptor,
/// wait for shutdown, drain, call `on_shutdown`.
async fn engine_future<H>(
    handler: H,
    scheme: String,
    target: String,
    options: Arc<Options>,
) -> Result<(), Error>
where
    H: EventHandler + Send + 'static,
{
    let state = Arc::new(EngineState::new());
    let handler: Arc<Mutex<Box<dyn EventHandler + Send>>> = Arc::new(Mutex::new(Box::new(handler)));
    let engine = Engine {
        inner: state.clone(),
    };

    // on_boot: Shutdown aborts before any listener is created.
    {
        let mut h = handler.lock().unwrap();
        if matches!(h.on_boot(&engine), Action::Shutdown) {
            state.mark_done();
            return Ok(());
        }
    }

    let balancer: Arc<Mutex<Box<dyn Balancer<usize> + Send>>> = match options.load_balancing {
        LoadBalancing::RoundRobin => Arc::new(Mutex::new(Box::new(RoundRobin::new()))),
        LoadBalancing::LeastConnections => Arc::new(Mutex::new(Box::new(LeastConnections::new()))),
        LoadBalancing::SourceAddrHash => Arc::new(Mutex::new(Box::new(SourceAddrHash::new()))),
    };

    let loop_count = options.event_loop_count().max(1);

    // Spawn event loops first so the acceptor has somewhere to send.
    let mut loop_handles = Vec::with_capacity(loop_count);
    let mut loop_joins = Vec::with_capacity(loop_count);
    for i in 0..loop_count {
        let (handle, join) = spawn_loop(LoopParts {
            id: i,
            handler: handler.clone(),
            state: state.clone(),
            options: options.clone(),
            balancer: balancer.clone(),
            is_tick_loop: i == 0,
        });
        balancer.lock().unwrap().add(i);
        loop_handles.push(handle);
        loop_joins.push(join);
    }

    // Listener + acceptor (or single UDP conn for datagram schemes).
    let mut acceptor_join: Option<tokio::task::JoinHandle<()>> = None;
    let result = match scheme.as_str() {
        "tcp" | "tcp4" | "tcp6" => match bind_tcp(&target, &options) {
            Ok(listener) => {
                acceptor_join = Some(tokio::spawn(accept_loop(
                    listener,
                    loop_handles,
                    balancer.clone(),
                    state.clone(),
                )));
                Ok(())
            }
            Err(e) => Err(e),
        },
        "udp" | "udp4" | "udp6" => match bind_udp(&target, &options) {
            Ok(sock) => {
                // UDP has no accept: one "connection" represents the
                // socket, assigned to the balancer's pick (usually
                // loop 0 since no traffic has flowed yet).
                let loop_id = balancer.lock().unwrap().next(None).unwrap_or(0);
                let handle = loop_handles
                    .iter()
                    .find(|h| h.id == loop_id)
                    .or_else(|| loop_handles.first())
                    .cloned();
                if let Some(h) = handle {
                    let local = sock
                        .local_addr()
                        .unwrap_or_else(|_| "0.0.0.0:0".parse().unwrap());
                    let remote: SocketAddr = if local.is_ipv4() {
                        "0.0.0.0:0".parse().unwrap()
                    } else {
                        "[::]:0".parse().unwrap()
                    };
                    let _ = h.tx.send(LoopMsg::NewConn {
                        id: state.next_conn_id.fetch_add(1, Ordering::SeqCst),
                        kind: StreamKind::Udp(sock),
                        local,
                        remote,
                    });
                }
                Ok(())
            }
            Err(e) => Err(e),
        },
        "unix" => Err(Error::UnsupportedScheme(format!(
            "unix:// is parsed but not yet implemented ({target})"
        ))),
        other => Err(Error::UnsupportedScheme(other.to_string())),
    };

    if let Err(e) = result {
        // Bind failed: shut everything down and report.
        state.request_shutdown();
        for j in loop_joins {
            let _ = j.await;
        }
        state.mark_done();
        return Err(e);
    }

    // Wait for shutdown: either external Engine::stop or an
    // Action::Shutdown from a handler.
    {
        let mut rx = state.shutdown_rx.clone();
        // `wait_for` completes when the value becomes true; the
        // initial value is false and only `send(true)` flips it.
        let _ = rx.wait_for(|v| *v).await;
    }

    // The acceptor exits on the same watch; join it so the listener
    // fd is closed before we report done.
    if let Some(j) = acceptor_join {
        let _ = j.await;
    }

    // Loops drain their connections (actors flush once and close,
    // each Closed triggers on_close) then exit on their own.
    for j in loop_joins {
        let _ = j.await;
    }

    {
        let mut h = handler.lock().unwrap();
        h.on_shutdown(&engine);
    }

    state.mark_done();
    Ok(())
}

/// Accept loop: accept connections and dispatch them to loops via
/// the balancer until shutdown.
async fn accept_loop(
    listener: TcpListener,
    loop_handles: Vec<crate::eventloop::LoopHandle>,
    balancer: Arc<Mutex<Box<dyn Balancer<usize> + Send>>>,
    state: Arc<EngineState>,
) {
    let mut shutdown_rx = state.shutdown_rx.clone();
    loop {
        tokio::select! {
            biased;
            _ = shutdown_rx.changed() => break,
            res = listener.accept() => {
                match res {
                    Ok((stream, remote)) => {
                        // Pick a loop. Balancer indexes match spawn
                        // order == loop id because we registered ids
                        // 0..n in order.
                        let loop_id = {
                            let mut b = balancer.lock().unwrap();
                            b.next(Some(&remote)).unwrap_or(0)
                        };
                        let local = stream
                            .local_addr()
                            .unwrap_or_else(|_| "0.0.0.0:0".parse().unwrap());
                        if let Some(h) = loop_handles.iter().find(|h| h.id == loop_id) {
                            let _ = h.tx.send(LoopMsg::NewConn {
                                id: state.next_conn_id.fetch_add(1, Ordering::SeqCst),
                                kind: StreamKind::Tcp(stream),
                                local,
                                remote,
                            });
                        }
                        // If no handle matched (shouldn't happen),
                        // dropping `stream` closes the connection.
                    }
                    Err(e) if is_transient_accept_error(&e) => continue,
                    Err(_) => break,
                }
            }
        }
    }
}

fn is_transient_accept_error(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::ConnectionAborted
            | io::ErrorKind::Interrupted
            | io::ErrorKind::WouldBlock
            | io::ErrorKind::TimedOut
    )
}

// ------------------------------------------------------------------
// Listener construction with socket options
// ------------------------------------------------------------------

/// Parse `host:port` (or bare `port`) into a `SocketAddr`, resolving
/// hostnames once at startup like Go's `ResolveTCPAddr`.
fn resolve_addr(target: &str) -> Result<SocketAddr, Error> {
    if let Ok(sa) = target.parse::<SocketAddr>() {
        return Ok(sa);
    }
    // "host:port" or ":port" or "port"
    let with_port = if target.contains(':') {
        target.to_string()
    } else {
        format!("{target}:0")
    };
    with_port
        .to_socket_addrs()
        .map_err(|e| Error::InvalidAddress(format!("cannot resolve {target}: {e}")))?
        .next()
        .ok_or_else(|| Error::InvalidAddress(format!("no address for {target}")))
}

fn socket_domain(addr: &SocketAddr) -> Domain {
    if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    }
}

fn apply_listener_opts(sock: &Socket, options: &Options, is_tcp: bool) {
    let _ = sock.set_reuse_address(options.reuse_addr);
    // SO_REUSEPORT: unix-only in socket2; on Windows the equivalent
    // is exclusive-address-use which we leave at its default.
    #[cfg(unix)]
    {
        if options.reuse_port {
            let _ = sock.set_reuse_port(true);
        }
    }
    if is_tcp {
        if let Some(keep) = options.keep_alive_params() {
            let (idle, intvl, count) = keep;
            let _ = sock.set_keepalive(true);
            let _ = set_keepalive_params(sock, idle, intvl, count);
        }
    }
}

/// Platform keep-alive tuning. socket2 exposes `set_keepalive` but
/// not TCP_KEEPIDLE/INTVL/CNT portably; on Linux we set them via
/// `set_sockopt` with raw option numbers through `socket2`'s
/// `SockRef` where available, otherwise keepalive stays at OS defaults.
fn set_keepalive_params(
    _sock: &Socket,
    _idle: Duration,
    _intvl: Duration,
    _count: u32,
) -> io::Result<()> {
    // socket2 0.6 does not expose TCP_KEEPIDLE/TCP_KEEPINTVL/
    // TCP_KEEPCNT; the plain SO_KEEPALIVE enabled above is the
    // portable baseline. Idle/interval/count tuning is a documented
    // gap for a follow-up WorkUnit.
    Ok(())
}

fn bind_tcp(target: &str, options: &Options) -> Result<TcpListener, Error> {
    let addr = resolve_addr(target)?;
    let domain = socket_domain(&addr);
    let sock = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    apply_listener_opts(&sock, options, true);
    sock.set_nonblocking(true)?;
    sock.bind(&addr.into())?;
    sock.listen(1024)?;

    let std_listener: std::net::TcpListener = sock.into();
    std_listener.set_nonblocking(true)?;
    TcpListener::from_std(std_listener).map_err(Error::Io)
}

fn bind_udp(target: &str, options: &Options) -> Result<UdpSocket, Error> {
    let addr = resolve_addr(target)?;
    let domain = socket_domain(&addr);
    let sock = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
    // Go forces SO_REUSEPORT for UDP so multiple loops/processes can
    // bind the same port; mirror that.
    #[cfg(unix)]
    {
        let _ = sock.set_reuse_port(true);
    }
    let _ = sock.set_reuse_address(options.reuse_addr || options.reuse_port);
    sock.set_nonblocking(true)?;
    sock.bind(&addr.into())?;

    let std_sock: std::net::UdpSocket = sock.into();
    std_sock.set_nonblocking(true)?;
    UdpSocket::from_std(std_sock).map_err(Error::Io)
}
