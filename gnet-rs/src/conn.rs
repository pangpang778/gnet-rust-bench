//! Connection state owned by an event loop.
//!
//! A `Conn` is `!Send` in spirit — all methods that touch buffers must be
//! called from the owning loop thread. We enforce this by keeping conns in a
//! per-loop slab and only exposing `&mut Conn` inside loop callbacks.

use crate::buffer::elastic;
use crate::buffer::ring::RingBuffer;
use mio::net::{TcpStream as MioTcpStream, UdpSocket as MioUdpSocket};
#[cfg(unix)]
use mio::net::UnixStream as MioUnixStream;
use std::io;
use std::net::{SocketAddr, TcpStream, UdpSocket};

#[cfg(unix)]
use std::os::unix::io::AsRawFd;
#[cfg(unix)]
pub use std::os::unix::io::RawFd;
#[cfg(windows)]
use std::os::windows::prelude::{AsRawSocket, RawSocket};
#[cfg(windows)]
pub type RawFd = RawSocket;

#[inline]
fn raw_of<T: SockHandle>(t: &T) -> RawFd {
    t.raw()
}

trait SockHandle {
    fn raw(&self) -> RawFd;
}

#[cfg(unix)]
impl<T: AsRawFd> SockHandle for T {
    fn raw(&self) -> RawFd {
        self.as_raw_fd()
    }
}

#[cfg(windows)]
impl<T: AsRawSocket> SockHandle for T {
    fn raw(&self) -> RawFd {
        self.as_raw_socket()
    }
}

/// Protocol of a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Proto {
    Tcp,
    Udp,
    Unix,
}

impl Proto {
    pub fn as_str(&self) -> &'static str {
        match self {
            Proto::Tcp => "tcp",
            Proto::Udp => "udp",
            Proto::Unix => "unix",
        }
    }
}

/// Inbound side: leftover bytes that handlers didn't consume plus the
/// current read chunk. Handlers call `next`/`peek`/`discard` here.
pub struct Inbound {
    /// Bytes carried over from previous reads that the codec didn't consume.
    buf: RingBuffer,
    /// Scratch for the current OnTraffic delivery (set during read).
    current: Vec<u8>,
}

impl Inbound {
    pub fn new(cap: usize) -> Self {
        Self {
            buf: RingBuffer::new(cap),
            current: Vec::new(),
        }
    }

    /// Called by the loop after a successful read: appends `data` to `current`
    /// so handlers can consume it via `next`/`peek`/`discard`.
    pub fn set_current(&mut self, data: &[u8]) {
        self.current.clear();
        self.current.extend_from_slice(data);
    }

    /// Total bytes available to the handler right now.
    pub fn buffered(&self) -> usize {
        self.buf.buffered() + self.current.len()
    }

    /// Read up to `p.len()` bytes (current chunk first, then leftover buffer).
    pub fn read_into(&mut self, p: &mut [u8]) -> usize {
        let mut n = 0;
        if !self.current.is_empty() {
            let take = self.current.len().min(p.len());
            p[..take].copy_from_slice(&self.current[..take]);
            self.current.drain(..take);
            n += take;
        }
        if n < p.len() && !self.buf.is_empty() {
            n += self.buf.read_into(&mut p[n..]).unwrap_or(0);
        }
        n
    }

    /// Peek at the next `n` bytes WITHOUT consuming. Returns a Vec copy
    /// (simpler than head/tail borrows across the two backing stores).
    /// `n == 0` returns everything.
    pub fn peek(&self, n: usize) -> Vec<u8> {
        let want = if n == 0 || n > self.buffered() {
            self.buffered()
        } else {
            n
        };
        let mut out = Vec::with_capacity(want);
        // current chunk first
        let cur_take = self.current.len().min(want);
        out.extend_from_slice(&self.current[..cur_take]);
        if out.len() < want && !self.buf.is_empty() {
            let need = want - out.len();
            let (h, t) = self.buf.peek(need);
            out.extend_from_slice(h);
            if out.len() < want {
                out.extend_from_slice(t);
            }
        }
        out.truncate(want);
        out
    }

    /// Peek into the leftover ring buffer only (for codecs that operate on
    /// the persistent buffer). For codec use prefer `CodecBuffer` below.
    pub fn ring(&mut self) -> &mut RingBuffer {
        &mut self.buf
    }

    /// Discard `n` bytes: consumes from current chunk first, then ring.
    pub fn discard(&mut self, n: usize) -> usize {
        let mut remaining = n;
        let mut discarded = 0;
        if !self.current.is_empty() {
            let take = self.current.len().min(remaining);
            self.current.drain(..take);
            remaining -= take;
            discarded += take;
        }
        if remaining > 0 && !self.buf.is_empty() {
            discarded += self.buf.discard(remaining);
        }
        discarded
    }

    /// Move any unconsumed bytes in `current` into the persistent ring so they
    /// survive the next `set_current` call. Called by the loop after each
    /// `on_traffic`.
    pub fn compact(&mut self) {
        if !self.current.is_empty() {
            self.buf.write(&self.current);
            self.current.clear();
        }
    }
}

/// A connection managed by an event loop.
pub struct Conn {
    pub id: usize,
    pub proto: Proto,
    /// Raw fd for mio registration. We keep a `TcpStream`/`UdpSocket` for the
    /// actual IO but register the raw fd.
    pub fd: RawFd,
    stream: Option<TcpStream>,
    /// The mio wrapper around `stream`'s fd, kept alive so the Poll
    /// registration stays valid.
    mio_tcp: Option<MioTcpStream>,
    /// For unix-domain conns: the mio UnixStream (doubles as IO handle).
    #[cfg(unix)]
    unix: Option<MioUnixStream>,
    udp: Option<UdpSocket>,
    /// For UDP conns we keep the mio socket that is registered with the Poll.
    mio_udp: Option<MioUdpSocket>,
    pub local_addr: SocketAddr,
    pub remote_addr: SocketAddr,
    /// For UDP: the last peer we heard from (used by SendTo default).
    pub udp_peer: Option<SocketAddr>,
    pub opened: bool,
    pub is_eof: bool,
    /// Outbound elastic buffer (ring + list).
    pub outbound: elastic::Buffer,
    /// Inbound buffer for codec leftovers + current chunk.
    pub inbound: Inbound,
    /// User context (opaque).
    pub ctx: Option<Box<dyn std::any::Any>>,
}

impl Conn {
    pub fn new_tcp(
        id: usize,
        stream: TcpStream,
        local: SocketAddr,
        remote: SocketAddr,
        write_cap: usize,
        read_cap: usize,
    ) -> io::Result<Self> {
        stream.set_nonblocking(true)?;
        let fd = stream.raw();
        Ok(Self {
            id,
            proto: Proto::Tcp,
            fd,
            stream: Some(stream),
            mio_tcp: None,
            #[cfg(unix)]
            unix: None,
            udp: None,
            mio_udp: None,
            local_addr: local,
            remote_addr: remote,
            udp_peer: None,
            opened: false,
            is_eof: false,
            outbound: elastic::Buffer::new(write_cap),
            inbound: Inbound::new(read_cap),
            ctx: None,
        })
    }

    /// Create a UDP conn around a *connected* std UdpSocket (client side) —
    /// or an unconnected one for server per-peer conns. The mio wrapper is
    /// created by the caller via `set_mio_udp`.
    pub fn new_udp_socket(
        id: usize,
        sock: UdpSocket,
        local: SocketAddr,
        write_cap: usize,
        read_cap: usize,
    ) -> io::Result<Self> {
        sock.set_nonblocking(true)?;
        let fd = sock.raw();
        Ok(Self {
            id,
            proto: Proto::Udp,
            fd,
            stream: None,
            mio_tcp: None,
            #[cfg(unix)]
            unix: None,
            udp: Some(sock),
            mio_udp: None,
            local_addr: local,
            remote_addr: local,
            udp_peer: None,
            opened: true,
            is_eof: false,
            outbound: elastic::Buffer::new(write_cap),
            inbound: Inbound::new(read_cap),
            ctx: None,
        })
    }

    /// Unix-domain connection wrapping a mio UnixStream.
    #[cfg(unix)]
    pub fn new_unix(
        id: usize,
        stream: MioUnixStream,
        local: SocketAddr,
        remote: SocketAddr,
        write_cap: usize,
        read_cap: usize,
    ) -> io::Result<Self> {
        stream.set_nonblocking(true)?;
        let fd = stream.raw();
        Ok(Self {
            id,
            proto: Proto::Unix,
            fd,
            stream: None,
            mio_tcp: None,
            unix: Some(stream),
            udp: None,
            mio_udp: None,
            local_addr: local,
            remote_addr: remote,
            udp_peer: None,
            opened: false,
            is_eof: false,
            outbound: elastic::Buffer::new(write_cap),
            inbound: Inbound::new(read_cap),
            ctx: None,
        })
    }

    /// Wrap an existing mio UdpSocket (used when engine hands a bound socket).
    pub fn from_mio_udp(
        id: usize,
        sock: MioUdpSocket,
        local: SocketAddr,
        write_cap: usize,
        read_cap: usize,
    ) -> io::Result<Self> {
        // mio sockets are always nonblocking.
        let fd = sock.raw();
        Ok(Self {
            id,
            proto: Proto::Udp,
            fd,
            stream: None,
            mio_tcp: None,
            #[cfg(unix)]
            unix: None,
            udp: None,
            mio_udp: Some(sock),
            local_addr: local,
            remote_addr: local,
            udp_peer: None,
            opened: true,
            is_eof: false,
            outbound: elastic::Buffer::new(write_cap),
            inbound: Inbound::new(read_cap),
            ctx: None,
        })
    }

    /// Write raw bytes to the socket, draining `outbound` first if non-empty.
    /// Returns Ok(()) on success or EAGAIN (remaining data stays buffered).
    /// Err on real error.
    pub fn flush_outbound(&mut self) -> io::Result<()> {
        if self.outbound.is_empty() {
            return Ok(());
        }
        // Copy iovecs out so we can mutably borrow self.stream while writing.
        // shortcut: copies once per flush; acceptable vs borrow complexity.
        let iov: Vec<Vec<u8>> = self
            .outbound
            .peek(0)
            .iter()
            .map(|s| s.to_vec())
            .collect();
        if iov.is_empty() {
            return Ok(());
        }
        let mut written_total = 0usize;
        let mut blocked = false;
        'outer: for chunk in &iov {
            let mut off = 0usize;
            while off < chunk.len() {
                match self.write_vectored(&chunk[off..]) {
                    Ok(0) => break 'outer,
                    Ok(n) => {
                        off += n;
                        written_total += n;
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        blocked = true;
                        break 'outer;
                    }
                    Err(e) => {
                        // discard what we managed to write, then propagate
                        if written_total > 0 {
                            self.outbound.discard(written_total);
                        }
                        return Err(e);
                    }
                }
            }
        }
        let _ = blocked;
        if written_total > 0 {
            self.outbound.discard(written_total);
        }
        Ok(())
    }

    fn write_vectored(&mut self, buf: &[u8]) -> io::Result<usize> {
        #[cfg(unix)]
        if self.proto == Proto::Unix {
            if let Some(s) = self.unix.as_mut() {
                return s.write(buf);
            }
        }
        match &mut self.stream {
            Some(s) => s.write(buf),
            None => Err(io::Error::new(io::ErrorKind::Unsupported, "not a stream")),
        }
    }

    /// Queue `data` to outbound and try immediate flush.
    /// For stream conns: writes through, buffers remainder on WouldBlock.
    /// For UDP: sends immediately (datagram semantics).
    pub fn send(&mut self, data: &[u8]) -> io::Result<()> {
        if self.proto == Proto::Udp {
            return self.send_udp(data);
        }
        // If outbound has pending data, append to preserve ordering.
        if !self.outbound.is_empty() {
            self.outbound.write(data);
            return Ok(());
        }
        // Try direct write first.
        match self.write_vectored(data) {
            Ok(n) if n == data.len() => Ok(()),
            Ok(n) => {
                self.outbound.write(&data[n..]);
                Ok(())
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                self.outbound.write(data);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    fn send_udp(&mut self, data: &[u8]) -> io::Result<()> {
        if let Some(sock) = self.udp.as_ref() {
            return match self.udp_peer {
                Some(peer) => sock.send_to(data, peer).map(|_| ()),
                None => sock.send(data).map(|_| ()),
            };
        }
        let sock = self.mio_udp.as_ref().ok_or_else(|| {
            io::Error::new(io::ErrorKind::Unsupported, "not udp")
        })?;
        match self.udp_peer {
            Some(peer) => sock.send_to(data, peer).map(|_| ()),
            None => sock.send(data).map(|_| ()),
        }
    }

    /// Read available bytes from the socket into `buf`. Returns (n, eof).
    /// n==0 with eof=false means WouldBlock.
    pub fn read_once(&mut self, buf: &mut [u8]) -> io::Result<(usize, bool)> {
        #[cfg(unix)]
        if self.proto == Proto::Unix {
            if let Some(s) = self.unix.as_mut() {
                return match s.read(buf) {
                    Ok(0) => Ok((0, true)),
                    Ok(n) => Ok((n, false)),
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok((0, false)),
                    Err(e) => Err(e),
                };
            }
        }
        match &mut self.stream {
            Some(s) => match s.read(buf) {
                Ok(0) => Ok((0, true)),
                Ok(n) => Ok((n, false)),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok((0, false)),
                Err(e) => Err(e),
            },
            None => Err(io::Error::new(io::ErrorKind::Unsupported, "not stream")),
        }
    }

    /// UDP recv_from into buf; also captures sender addr. Returns (n, peer).
    /// Only meaningful for conns that own a dedicated socket (client side or
    /// per-peer server conns). The shared ingress UDP socket is read directly
    /// by the loop, not through a Conn.
    pub fn read_udp_once(&mut self, buf: &mut [u8]) -> io::Result<(usize, Option<SocketAddr>)> {
        if let Some(sock) = self.udp.as_ref() {
            return match sock.recv_from(buf) {
                Ok((n, addr)) => Ok((n, Some(addr))),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok((0, None)),
                Err(e) => Err(e),
            };
        }
        let sock = self.mio_udp.as_ref().ok_or_else(|| {
            io::Error::new(io::ErrorKind::Unsupported, "not udp")
        })?;
        match sock.recv_from(buf) {
            Ok((n, addr)) => Ok((n, Some(addr))),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok((0, None)),
            Err(e) => Err(e),
        }
    }

    /// TCP accept helper.
    pub fn try_clone_stream(&self) -> io::Result<TcpStream> {
        self.stream
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Unsupported, "not stream"))?
            .try_clone()
    }

    // ---- accessors used by the event loop ----

    pub fn fd(&self) -> RawFd {
        self.fd
    }

    pub fn stream_ref(&self) -> Option<&TcpStream> {
        self.stream.as_ref()
    }

    pub fn stream_mut(&mut self) -> Option<&mut TcpStream> {
        self.stream.as_mut()
    }

    pub fn mio_udp_ref(&self) -> Option<&MioUdpSocket> {
        self.mio_udp.as_ref()
    }

    pub fn mio_tcp_ref(&self) -> Option<&MioTcpStream> {
        self.mio_tcp.as_ref()
    }

    pub fn mio_tcp_mut(&mut self) -> Option<&mut MioTcpStream> {
        self.mio_tcp.as_mut()
    }

    #[cfg(unix)]
    pub fn unix_mut(&mut self) -> Option<&mut MioUnixStream> {
        self.unix.as_mut()
    }

    pub fn mio_udp_mut(&mut self) -> Option<&mut MioUdpSocket> {
        self.mio_udp.as_mut()
    }

    pub fn set_mio_tcp(&mut self, s: MioTcpStream) {
        self.mio_tcp = Some(s);
    }

    pub fn set_mio_udp(&mut self, s: MioUdpSocket) {
        self.mio_udp = Some(s);
    }

    #[cfg(unix)]
    pub fn unix_ref(&self) -> Option<&MioUnixStream> {
        self.unix.as_ref()
    }
}

use std::io::{Read, Write};
