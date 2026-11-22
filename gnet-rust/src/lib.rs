//! gnet — a high-performance, lightweight, non-blocking,
//! event-driven network framework written in Rust.
//!
//! Port of the Go [`gnet`](https://gnet.host/) v2 core.

#![warn(missing_docs)]
#![forbid(unsafe_code)]

pub mod balancer;
pub mod buffer;
pub mod engine;
pub mod error;
pub mod handler;
pub mod options;
pub mod proto;

mod eventloop;

pub use balancer::{Balancer, LeastConnections, RoundRobin, SourceAddrHash};
pub use engine::{run, run_with, run_with_options};
pub use error::Error;
pub use handler::{Action, Conn, Engine, EventHandler, NoopHandler};
pub use options::{LoadBalancing, Options, TcpNoDelay};
pub use proto::{Codec, CodecError, DelimiterCodec, FixedLengthCodec, LengthPrefixCodec};
