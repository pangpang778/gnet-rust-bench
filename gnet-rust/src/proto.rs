//! Protocol codec: packet framing for the wire.
//!
//! Port of the Go gnet codec concept (`Encode` / `Decode` / `Unpack` in
//! `gnet_test.go` and downstream users). The Go version is pull-based on a
//! `Conn` with `Peek`/`Discard`; in Rust we keep it simple and synchronous:
//! `encode` turns a payload into wire bytes, `decode` is an incremental
//! decoder that consumes from an internal buffer and yields whole packets.
//!
//! Three common patterns are provided:
//!
//! - [`LengthPrefixCodec`] — 4-byte big-endian length prefix, payload follows.
//!   This matches the Go reference codec exactly.
//! - [`DelimiterCodec`] — packets separated by a byte delimiter (e.g. `\n`).
//! - [`FixedLengthCodec`] — every packet is exactly `N` bytes.
//!
//! Custom codecs implement [`Codec`] directly.

use std::collections::VecDeque;

/// A protocol codec turns application payloads into framed wire bytes and
/// back. Implementations are incremental: feed wire bytes via
/// [`Codec::feed`] and drain complete packets with [`Codec::decode`].
pub trait Codec {
    /// Encode one application payload into wire bytes.
    fn encode(&self, payload: &[u8]) -> Vec<u8>;

    /// Feed raw bytes received from the peer into the decoder.
    fn feed(&mut self, bytes: &[u8]);

    /// Try to extract the next complete packet.
    ///
    /// Returns `Some(payload)` when a full packet is buffered, `None` when
    /// more bytes are needed. Bytes belonging to a partial packet are kept
    /// internally; they are only discarded once a complete packet is emitted.
    fn decode(&mut self) -> Option<Vec<u8>>;

    /// Remove all buffered undecoded bytes (e.g. on connection reset).
    fn clear(&mut self);
}

/// Errors surfaced by codec constructors and validation.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    /// A codec parameter was out of range.
    #[error("invalid codec parameter: {0}")]
    InvalidParameter(String),
}

/// Codec using a 4-byte big-endian length prefix.
///
/// Wire format:
///
/// ```text
/// +--------+--------+----------------+
/// | magic? |  len   |    payload     |
/// +--------+--------+----------------+
/// ```
///
/// This codec has no magic field — pure length-prefix framing. The payload
/// length is `u32` big-endian and counts only the bytes after the prefix.
///
/// # Example
///
/// ```
/// use gnet::proto::LengthPrefixCodec;
/// use gnet::proto::Codec;
///
/// let mut codec = LengthPrefixCodec::new();
/// let wire = codec.encode(b"hello");
/// assert_eq!(wire.len(), 4 + 5);
/// codec.feed(&wire);
/// assert_eq!(codec.decode().as_deref(), Some(&b"hello"[..]));
/// assert!(codec.decode().is_none());
/// ```
#[derive(Debug, Default)]
pub struct LengthPrefixCodec {
    buf: Vec<u8>,
}

impl LengthPrefixCodec {
    /// Create a new codec with empty input buffer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Bytes currently buffered but not yet decoded.
    pub fn buffered(&self) -> usize {
        self.buf.len()
    }
}

impl Codec for LengthPrefixCodec {
    fn encode(&self, payload: &[u8]) -> Vec<u8> {
        let len = payload.len() as u32;
        let mut out = Vec::with_capacity(4 + payload.len());
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(payload);
        out
    }

    fn feed(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    fn decode(&mut self) -> Option<Vec<u8>> {
        if self.buf.len() < 4 {
            return None;
        }
        let len = u32::from_be_bytes([self.buf[0], self.buf[1], self.buf[2], self.buf[3]]) as usize;
        let total = 4usize.checked_add(len)?;
        if self.buf.len() < total {
            return None;
        }
        // Drain exactly one packet.
        let packet: Vec<u8> = self.buf.drain(..total).skip(4).collect();
        Some(packet)
    }

    fn clear(&mut self) {
        self.buf.clear();
    }
}

/// Codec using a single delimiter byte (e.g. `b'\n'` for line protocols).
///
/// The delimiter is not included in the decoded payload.
#[derive(Debug)]
pub struct DelimiterCodec {
    delimiter: u8,
    buf: VecDeque<u8>,
}

impl DelimiterCodec {
    /// Create a codec that splits packets on `delimiter`.
    pub fn new(delimiter: u8) -> Self {
        Self {
            delimiter,
            buf: VecDeque::new(),
        }
    }

    /// Bytes currently buffered but not yet decoded.
    pub fn buffered(&self) -> usize {
        self.buf.len()
    }
}

impl Codec for DelimiterCodec {
    fn encode(&self, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(payload.len() + 1);
        out.extend_from_slice(payload);
        out.push(self.delimiter);
        out
    }

    fn feed(&mut self, bytes: &[u8]) {
        self.buf.extend(bytes.iter().copied());
    }

    fn decode(&mut self) -> Option<Vec<u8>> {
        // Find the first delimiter.
        let pos = self.buf.iter().position(|&b| b == self.delimiter)?;
        let mut packet = Vec::with_capacity(pos);
        for _ in 0..pos {
            // unwrap safe: we just found `pos` elements before the delimiter.
            packet.push(self.buf.pop_front().unwrap());
        }
        // Consume the delimiter itself.
        self.buf.pop_front();
        Some(packet)
    }

    fn clear(&mut self) {
        self.buf.clear();
    }
}

/// Codec where every packet is exactly `packet_size` bytes.
#[derive(Debug)]
pub struct FixedLengthCodec {
    packet_size: usize,
    buf: Vec<u8>,
}

impl FixedLengthCodec {
    /// Create a codec for fixed-size packets.
    ///
    /// # Errors
    /// Returns [`CodecError::InvalidParameter`] when `packet_size` is zero.
    pub fn new(packet_size: usize) -> Result<Self, CodecError> {
        if packet_size == 0 {
            return Err(CodecError::InvalidParameter(
                "packet_size must be > 0".into(),
            ));
        }
        Ok(Self {
            packet_size,
            buf: Vec::new(),
        })
    }

    /// Bytes currently buffered but not yet decoded.
    pub fn buffered(&self) -> usize {
        self.buf.len()
    }
}

impl Codec for FixedLengthCodec {
    fn encode(&self, payload: &[u8]) -> Vec<u8> {
        debug_assert_eq!(
            payload.len(),
            self.packet_size,
            "fixed-length codec payload must be exactly packet_size bytes"
        );
        payload.to_vec()
    }

    fn feed(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    fn decode(&mut self) -> Option<Vec<u8>> {
        if self.buf.len() < self.packet_size {
            return None;
        }
        let packet: Vec<u8> = self.buf.drain(..self.packet_size).collect();
        Some(packet)
    }

    fn clear(&mut self) {
        self.buf.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------- LengthPrefixCodec ----------

    #[test]
    fn length_prefix_roundtrip_single() {
        let mut c = LengthPrefixCodec::new();
        let wire = c.encode(b"hello");
        assert_eq!(wire.len(), 9);
        assert_eq!(&wire[..4], &5u32.to_be_bytes());
        assert_eq!(&wire[4..], b"hello");
        c.feed(&wire);
        assert_eq!(c.decode().as_deref(), Some(&b"hello"[..]));
        assert!(c.decode().is_none());
    }

    #[test]
    fn length_prefix_roundtrip_multiple_in_one_feed() {
        let mut c = LengthPrefixCodec::new();
        let mut wire = c.encode(b"foo");
        wire.extend_from_slice(&c.encode(b"barbaz"));
        wire.extend_from_slice(&c.encode(b"")); // empty payload
        c.feed(&wire);
        assert_eq!(c.decode().as_deref(), Some(&b"foo"[..]));
        assert_eq!(c.decode().as_deref(), Some(&b"barbaz"[..]));
        assert_eq!(c.decode().as_deref(), Some(&b""[..]));
        assert!(c.decode().is_none());
    }

    #[test]
    fn length_prefix_handles_fragmented_feed() {
        let mut c = LengthPrefixCodec::new();
        let wire = c.encode(b"fragmented");
        // Feed byte by byte.
        for &b in &wire[..5] {
            c.feed(&[b]);
            assert!(c.decode().is_none());
        }
        for &b in &wire[5..] {
            c.feed(&[b]);
        }
        assert_eq!(c.decode().as_deref(), Some(&b"fragmented"[..]));
    }

    #[test]
    fn length_prefix_partial_header_then_body() {
        let mut c = LengthPrefixCodec::new();
        let wire = c.encode(b"abc");
        // Feed only 2 bytes of header.
        c.feed(&wire[..2]);
        assert!(c.decode().is_none());
        // Feed remaining header byte but not body.
        c.feed(&wire[2..4]);
        assert!(c.decode().is_none());
        // Feed body.
        c.feed(&wire[4..]);
        assert_eq!(c.decode().as_deref(), Some(&b"abc"[..]));
    }

    #[test]
    fn length_prefix_large_payload() {
        let mut c = LengthPrefixCodec::new();
        let payload: Vec<u8> = (0..100_000u32).map(|i| (i % 256) as u8).collect();
        let wire = c.encode(&payload);
        assert_eq!(wire.len(), 4 + payload.len());
        c.feed(&wire);
        assert_eq!(c.decode(), Some(payload));
    }

    #[test]
    fn length_prefix_clear_resets_state() {
        let mut c = LengthPrefixCodec::new();
        let wire = c.encode(b"data");
        c.feed(&wire[..6]); // partial
        c.clear();
        assert_eq!(c.buffered(), 0);
        assert!(c.decode().is_none());
    }

    #[test]
    fn length_prefix_max_u32_len_no_panic() {
        // Advertised length larger than usize can hold on 32-bit would be an
        // issue; on 64-bit usize it is fine but we will never get that much
        // data. Ensure decode returns None without panicking.
        let mut c = LengthPrefixCodec::new();
        let mut wire = Vec::new();
        wire.extend_from_slice(&u32::MAX.to_be_bytes());
        wire.extend_from_slice(b"short");
        c.feed(&wire);
        assert!(c.decode().is_none());
    }

    // ---------- DelimiterCodec ----------

    #[test]
    fn delimiter_roundtrip() {
        let mut c = DelimiterCodec::new(b'\n');
        let wire = c.encode(b"line1");
        assert_eq!(wire, b"line1\n");
        let wire2 = c.encode(b"line2");
        assert_eq!(wire2, b"line2\n");
        let mut all = wire;
        all.extend_from_slice(&wire2);
        c.feed(&all);
        assert_eq!(c.decode().as_deref(), Some(&b"line1"[..]));
        assert_eq!(c.decode().as_deref(), Some(&b"line2"[..]));
        assert!(c.decode().is_none());
    }

    #[test]
    fn delimiter_empty_payload() {
        let mut c = DelimiterCodec::new(b'\n');
        c.feed(b"\n");
        assert_eq!(c.decode().as_deref(), Some(&b""[..]));
    }

    #[test]
    fn delimiter_fragmented_feed() {
        let mut c = DelimiterCodec::new(b'\n');
        c.feed(b"hel");
        assert!(c.decode().is_none());
        c.feed(b"lo\nwor");
        assert_eq!(c.decode().as_deref(), Some(&b"hello"[..]));
        assert!(c.decode().is_none());
        c.feed(b"ld\n");
        assert_eq!(c.decode().as_deref(), Some(&b"world"[..]));
    }

    #[test]
    fn delimiter_multiple_delimiters_back_to_back() {
        let mut c = DelimiterCodec::new(b'\n');
        c.feed(b"a\n\nb\n");
        assert_eq!(c.decode().as_deref(), Some(&b"a"[..]));
        assert_eq!(c.decode().as_deref(), Some(&b""[..]));
        assert_eq!(c.decode().as_deref(), Some(&b"b"[..]));
        assert!(c.decode().is_none());
    }

    #[test]
    fn delimiter_clear() {
        let mut c = DelimiterCodec::new(b'\n');
        c.feed(b"partial");
        c.clear();
        assert_eq!(c.buffered(), 0);
        assert!(c.decode().is_none());
    }

    // ---------- FixedLengthCodec ----------

    #[test]
    fn fixed_length_roundtrip() {
        let mut c = FixedLengthCodec::new(4).unwrap();
        c.feed(b"abcdefgh");
        assert_eq!(c.decode().as_deref(), Some(&b"abcd"[..]));
        assert_eq!(c.decode().as_deref(), Some(&b"efgh"[..]));
        assert!(c.decode().is_none());
    }

    #[test]
    fn fixed_length_partial() {
        let mut c = FixedLengthCodec::new(4).unwrap();
        c.feed(b"ab");
        assert!(c.decode().is_none());
        c.feed(b"cd");
        assert_eq!(c.decode().as_deref(), Some(&b"abcd"[..]));
    }

    #[test]
    fn fixed_length_zero_size_rejected() {
        assert!(FixedLengthCodec::new(0).is_err());
    }

    #[test]
    fn fixed_length_clear() {
        let mut c = FixedLengthCodec::new(4).unwrap();
        c.feed(b"ab");
        c.clear();
        assert_eq!(c.buffered(), 0);
    }

    // ---------- Cross-codec sanity ----------

    #[test]
    fn length_prefix_decode_keeps_partial_bytes() {
        let mut c = LengthPrefixCodec::new();
        // Two packets, but feed only the first fully and part of second.
        let mut wire = c.encode(b"one");
        let second = c.encode(b"two");
        wire.extend_from_slice(&second);
        c.feed(&wire[..7 + 4 + 1]); // first packet + len of second + 1 byte of payload
        assert_eq!(c.decode().as_deref(), Some(&b"one"[..]));
        // Second packet is incomplete: 1 of 3 payload bytes present.
        assert!(c.decode().is_none());
        // Feed remaining 2 bytes.
        c.feed(&second[4 + 1..]);
        assert_eq!(c.decode().as_deref(), Some(&b"two"[..]));
    }

    #[test]
    fn codec_trait_object_works() {
        let c: Box<dyn Codec> = Box::new(LengthPrefixCodec::new());
        // Just check it can be used as a trait object.
        let _ = c;
    }
}
