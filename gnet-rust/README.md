# gnet-rust

A high-performance, lightweight, non-blocking, event-driven network framework written in Rust. This is a port of the Go [`gnet`](https://github.com/panjf2000/gnet) v2 core.

## Architecture

The framework is built around three pillars:

### Event Loops

Each listener runs one or more event loops (one per CPU core by default). Accepted connections are distributed across loops by a **balancer**:

- `RoundRobin` — cycles through backends in order
- `LeastConnections` — picks the backend with the fewest active connections
- `SourceAddrHash` — FNV-1a hash of the remote address for sticky sessions

### Buffers

Non-blocking I/O uses a hybrid buffer strategy:

- `RingBuffer` — fixed-capacity circular buffer for inbound data
- `LinkedListBuffer` — FIFO of chunks for outbound overflow
- `ElasticBuffer` — ring first, spills to list when the ring fills

### Protocol Codecs

Wire framing is handled by pluggable codecs:

- `LengthPrefixCodec` — 4-byte big-endian length prefix
- `DelimiterCodec` — packets separated by a delimiter byte (e.g. `\n`)
- `FixedLengthCodec` — every packet is exactly N bytes

## Usage

```rust
use gnet::{Action, Conn, Engine, EventHandler};

struct EchoHandler;

impl EventHandler for EchoHandler {
    fn on_open(&mut self, conn: &mut Conn) -> (Vec<u8>, Action) {
        (b"Welcome!\n".to_vec(), Action::None)
    }

    fn on_traffic(&mut self, conn: &mut Conn) -> Action {
        // Read buffered bytes, write them back, discard input.
        // (Conn IO methods land with the engine WorkUnit.)
        Action::None
    }
}

fn main() -> Result<(), gnet::Error> {
    gnet::run(EchoHandler, "127.0.0.1:9999")
}
```

Run the bundled example:

```bash
cargo run --example echo
```

## API Reference

### Core Types

| Type | Description |
|------|-------------|
| `EventHandler` | Trait for connection and lifecycle callbacks |
| `Conn` | Per-connection context (addresses, user data, IO) |
| `Engine` | Opaque handle to the running engine |
| `Action` | Post-event action: `None`, `Close`, `Shutdown` |
| `Error` | Crate-wide error type |

### Buffers

| Type | Description |
|------|-------------|
| `RingBuffer` | Circular buffer with `peek`/`read`/`write`/`discard` |
| `LinkedListBuffer` | Chunked FIFO for large outbound data |
| `ElasticBuffer` | Ring + list hybrid for outbound writes |

### Codecs

| Type | Description |
|------|-------------|
| `Codec` | Trait: `encode` / `feed` / `decode` / `clear` |
| `LengthPrefixCodec` | 4-byte BE length prefix framing |
| `DelimiterCodec` | Delimiter-byte packet splitting |
| `FixedLengthCodec` | Fixed-size packet framing |

### Balancers

| Type | Description |
|------|-------------|
| `Balancer<T>` | Trait for backend selection |
| `RoundRobin<T>` | Cyclic backend selection |
| `LeastConnections<T>` | Least-loaded backend selection |
| `SourceAddrHash<T>` | Source-address hash for sticky routing |

## Comparison with Go gnet

| Aspect | Go gnet | gnet-rust |
|--------|---------|-----------|
| Language | Go | Rust |
| Memory safety | GC + unsafe FFI | Safe Rust, `#![forbid(unsafe_code)]` |
| Concurrency | Goroutines + epoll/kqueue/IOCP | Tokio runtime + event loops |
| Buffers | `pkg/buffer/*` | `src/buffer.rs` |
| Codecs | User-implemented | Built-in `Codec` trait + 3 impls |
| Balancers | `load_balancer.go` | `src/balancer.rs` |
| Handler API | `EventHandler` interface | `EventHandler` trait (object-safe) |

## Status

This is an early port. The following WorkUnits are complete or in progress:

- `wu-core` — error types, handler trait, lib skeleton
- `wu-io` — ring buffer, linked-list buffer, elastic buffer
- `wu-lb` — round-robin, least-connections, source-addr-hash balancers
- `wu-proto` — length-prefix, delimiter, fixed-length codecs
- `wu-engine` — event loop, acceptor, connection management (in progress)
- `wu-example` — echo server example + README (this file)

The echo example compiles now; full server functionality arrives with `wu-engine`.
