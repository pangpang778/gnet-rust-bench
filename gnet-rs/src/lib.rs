//! gnet-rs — a high-performance, event-driven networking framework for Rust.
//!
//! This is a Rust rewrite of the core of [gnet](https://github.com/panjf2000/gnet)
//! v2 (Apache-2.0). It provides:
//!
//! - [`EventHandler`] trait with `on_boot`/`on_shutdown`/`on_open`/`on_close`/
//!   `on_traffic`/`on_tick` callbacks and an [`Action`] enum.
//! - [`run`] blocking entry point supporting `tcp://`, `udp://`, `unix://`.
//! - Multiple event loops (default = CPU count) driven by `mio`
//!   (epoll on Linux, kqueue on BSD/macOS, wepoll/IOCP on Windows).
//! - Load balancing: [`LoadBalancing::RoundRobin`], `LeastConnections`,
//!   `SourceAddrHash`.
//! - Inbound ring buffer ([`buffer::ring::RingBuffer`]) and outbound elastic
//!   buffer ([`buffer::elastic::Buffer`]).
//! - TCP options: TCP_NODELAY, SO_REUSEADDR, SO_REUSEPORT, keepalive.
//! - Graceful shutdown via [`Engine::stop`].

pub mod buffer;
pub mod conn;
pub mod engine;
pub mod eventloop;
pub mod handler;
pub mod loadbalancer;
pub mod protocol;

pub use conn::Conn;
pub use handler::{run, Action, Engine, EventHandler, NoopHandler, Options};
pub use loadbalancer::LoadBalancing;
