//! Crate-wide error type.

use std::io;

/// Errors produced by the gnet engine and supporting types.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An I/O error from the underlying socket or file descriptor.
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    /// An address could not be parsed.
    #[error("invalid address: {0}")]
    InvalidAddress(String),

    /// The protocol scheme is not supported.
    #[error("unsupported protocol scheme: {0}")]
    UnsupportedScheme(String),

    /// The engine was already shut down.
    #[error("engine already shut down")]
    EngineShutdown,
}
