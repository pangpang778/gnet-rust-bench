//! Server options for the gnet engine.
//!
//! Mirrors the relevant subset of the Go `Options`/`Option` surface in
//! `gnet-go/options.go`: multicore, event-loop count, TCP_NODELAY,
//! SO_REUSEADDR/SO_REUSEPORT, keepalive, read/write buffer caps and
//! load balancing. UDP/unix-specific options that have no Rust
//! counterpart yet are intentionally omitted.

use std::num::NonZeroUsize;
use std::time::Duration;

use crate::error::Error;

/// Default size of the per-connection read buffer, in bytes (64 KiB).
pub const DEFAULT_READ_BUFFER_CAP: usize = 64 * 1024;

/// Default size of the per-connection outbound static buffer, in bytes
/// (64 KiB). Overflow goes to the elastic linked list.
pub const DEFAULT_WRITE_BUFFER_CAP: usize = 64 * 1024;

/// Load-balancing algorithm used to assign accepted connections to
/// event loops.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LoadBalancing {
    /// Hand out event loops in order, cycling. This is the Go default.
    #[default]
    RoundRobin,
    /// Pick the event loop serving the fewest active connections.
    LeastConnections,
    /// Hash the peer address so a source sticks to one event loop.
    SourceAddrHash,
}

/// TCP_NODELAY socket option state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TcpNoDelay {
    /// Enable TCP_NODELAY (Nagle disabled). Default in Go gnet.
    #[default]
    Enabled,
    /// Disable TCP_NODELAY (Nagle enabled).
    Disabled,
}

/// Configuration for [`crate::engine::run`].
///
/// All fields default to the same values the Go engine uses: single
/// event loop, TCP_NODELAY on, no SO_REUSEADDR/SO_REUSEPORT, 64 KiB
/// read/write buffers, RoundRobin balancing.
#[derive(Debug)]
pub struct Options {
    /// Run one event loop per usable CPU core. Ignored when
    /// `num_event_loop` is set to a positive value.
    pub multicore: bool,

    /// Number of event loops to start. `0` means "derive from
    /// `multicore`" (1 or CPU count).
    pub num_event_loop: usize,

    /// Load balancer used to pick the event loop for each accepted
    /// connection.
    pub load_balancing: LoadBalancing,

    /// Set SO_REUSEADDR on listeners.
    pub reuse_addr: bool,

    /// Set SO_REUSEPORT on listeners (Linux/DragonFly/FreeBSD only;
    /// silently ignored elsewhere, matching the Go behaviour).
    pub reuse_port: bool,

    /// Maximum bytes read from the socket per readable event.
    pub read_buffer_cap: usize,

    /// Size of the static outbound ring before spilling to the
    /// linked list.
    pub write_buffer_cap: usize,

    /// TCP_NODELAY on accepted connections.
    pub tcp_no_delay: TcpNoDelay,

    /// TCP keep-alive idle time. `None` disables keep-alive.
    pub tcp_keep_alive: Option<Duration>,

    /// Interval between TCP keep-alive probes. Defaults to
    /// `keep_alive / 5` when unset.
    pub tcp_keep_interval: Option<Duration>,

    /// Number of unanswered probes before the connection drops.
    /// Defaults to 5 when unset.
    pub tcp_keep_count: Option<u32>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            multicore: false,
            num_event_loop: 0,
            load_balancing: LoadBalancing::RoundRobin,
            reuse_addr: false,
            reuse_port: false,
            read_buffer_cap: DEFAULT_READ_BUFFER_CAP,
            write_buffer_cap: DEFAULT_WRITE_BUFFER_CAP,
            tcp_no_delay: TcpNoDelay::Enabled,
            tcp_keep_alive: None,
            tcp_keep_interval: None,
            tcp_keep_count: None,
        }
    }
}

impl Options {
    /// Number of event loops to spawn.
    ///
    /// Mirrors Go's `determineEventLoops`: `num_event_loop` wins when
    /// positive, otherwise `multicore` picks `available_parallelism()`
    /// or 1.
    pub(crate) fn event_loop_count(&self) -> usize {
        if self.num_event_loop > 0 {
            self.num_event_loop
        } else if self.multicore {
            std::thread::available_parallelism()
                .map(NonZeroUsize::get)
                .unwrap_or(1)
        } else {
            1
        }
    }

    /// Effective TCP keep-alive parameters: `(idle, interval, count)`.
    pub(crate) fn keep_alive_params(&self) -> Option<(Duration, Duration, u32)> {
        let idle = self.tcp_keep_alive?;
        let interval = self
            .tcp_keep_interval
            .unwrap_or_else(|| idle / 5)
            .max(Duration::from_secs(1));
        let count = self.tcp_keep_count.unwrap_or(5).max(1);
        Some((idle, interval, count))
    }
}

// Builder-style option functions. Each returns a closure that can be
// passed to `engine::run_with`'s configure callback.
//
// Example:
//
// ```no_run
// # use gnet::{engine, options, NoopHandler, LoadBalancing};
// # use std::time::Duration;
// let _ = engine::run_with(
//     NoopHandler,
//     "tcp://127.0.0.1:0",
//     |o| {
//         options::multicore(true)(o);
//         options::load_balancing(LoadBalancing::LeastConnections)(o);
//         options::tcp_keep_alive(Duration::from_secs(30))(o);
//     },
// );
// ```

/// Set `Options::multicore`.
pub fn multicore(v: bool) -> impl Fn(&mut Options) {
    move |o| o.multicore = v
}

/// Set `Options::num_event_loop`.
pub fn num_event_loop(v: usize) -> impl Fn(&mut Options) {
    move |o| o.num_event_loop = v
}

/// Set `Options::load_balancing`.
pub fn load_balancing(lb: LoadBalancing) -> impl Fn(&mut Options) {
    move |o| o.load_balancing = lb
}

/// Set `Options::reuse_addr`.
pub fn reuse_addr(v: bool) -> impl Fn(&mut Options) {
    move |o| o.reuse_addr = v
}

/// Set `Options::reuse_port`.
pub fn reuse_port(v: bool) -> impl Fn(&mut Options) {
    move |o| o.reuse_port = v
}

/// Set `Options::read_buffer_cap`.
pub fn read_buffer_cap(v: usize) -> impl Fn(&mut Options) {
    move |o| o.read_buffer_cap = v
}

/// Set `Options::write_buffer_cap`.
pub fn write_buffer_cap(v: usize) -> impl Fn(&mut Options) {
    move |o| o.write_buffer_cap = v
}

/// Set `Options::tcp_no_delay`.
pub fn tcp_no_delay(v: bool) -> impl Fn(&mut Options) {
    move |o| {
        o.tcp_no_delay = if v {
            TcpNoDelay::Enabled
        } else {
            TcpNoDelay::Disabled
        };
    }
}

/// Set `Options::tcp_keep_alive`.
pub fn tcp_keep_alive(d: Duration) -> impl Fn(&mut Options) {
    move |o| o.tcp_keep_alive = Some(d)
}

/// Set `Options::tcp_keep_interval`.
pub fn tcp_keep_interval(d: Duration) -> impl Fn(&mut Options) {
    move |o| o.tcp_keep_interval = Some(d)
}

/// Set `Options::tcp_keep_count`.
pub fn tcp_keep_count(v: u32) -> impl Fn(&mut Options) {
    move |o| o.tcp_keep_count = Some(v)
}
/// Errors that can come from parsing an address string.
pub(crate) fn parse_addr(addr: &str) -> Result<(String, String), Error> {
    let (scheme, rest) = addr
        .split_once("://")
        .ok_or_else(|| Error::InvalidAddress(format!("missing scheme in address: {addr}")))?;
    let scheme = scheme.to_ascii_lowercase();
    match scheme.as_str() {
        "tcp" | "tcp4" | "tcp6" | "udp" | "udp4" | "udp6" => {
            if rest.is_empty() {
                return Err(Error::InvalidAddress(format!(
                    "empty address for scheme {scheme}"
                )));
            }
            Ok((scheme, rest.to_string()))
        }
        "unix" => {
            if rest.is_empty() {
                return Err(Error::InvalidAddress("empty unix socket path".into()));
            }
            Ok((scheme, rest.to_string()))
        }
        other => Err(Error::UnsupportedScheme(other.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_options_match_go() {
        let o = Options::default();
        assert!(!o.multicore);
        assert_eq!(o.num_event_loop, 0);
        assert_eq!(o.load_balancing, LoadBalancing::RoundRobin);
        assert!(!o.reuse_addr);
        assert!(!o.reuse_port);
        assert_eq!(o.read_buffer_cap, 64 * 1024);
        assert_eq!(o.write_buffer_cap, 64 * 1024);
        assert_eq!(o.tcp_no_delay, TcpNoDelay::Enabled);
        assert!(o.tcp_keep_alive.is_none());
    }

    #[test]
    fn event_loop_count_prefers_explicit() {
        let o = Options {
            multicore: true,
            num_event_loop: 4,
            ..Default::default()
        };
        assert_eq!(o.event_loop_count(), 4);
    }

    #[test]
    fn event_loop_count_single_when_not_multicore() {
        let o = Options::default();
        assert_eq!(o.event_loop_count(), 1);
    }

    #[test]
    fn keep_alive_params_defaults() {
        let o = Options {
            tcp_keep_alive: Some(Duration::from_secs(60)),
            ..Default::default()
        };
        let (idle, intvl, cnt) = o.keep_alive_params().unwrap();
        assert_eq!(idle, Duration::from_secs(60));
        assert_eq!(intvl, Duration::from_secs(12)); // 60/5
        assert_eq!(cnt, 5);
    }

    #[test]
    fn parse_addr_schemes() {
        assert_eq!(
            parse_addr("tcp://127.0.0.1:8080").unwrap(),
            ("tcp".into(), "127.0.0.1:8080".into())
        );
        assert_eq!(
            parse_addr("udp://0.0.0.0:53").unwrap(),
            ("udp".into(), "0.0.0.0:53".into())
        );
        assert_eq!(
            parse_addr("unix:///tmp/sock").unwrap(),
            ("unix".into(), "/tmp/sock".into())
        );
        assert!(matches!(
            parse_addr("http://x"),
            Err(Error::UnsupportedScheme(_))
        ));
        assert!(matches!(
            parse_addr("no-scheme"),
            Err(Error::InvalidAddress(_))
        ));
    }
}
