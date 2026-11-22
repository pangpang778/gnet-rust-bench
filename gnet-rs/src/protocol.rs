//! Protocol codecs built on the ring buffer.
//!
//! These are *helpers* users call inside `OnTraffic`: peek buffered bytes,
//! decode complete frames, then `discard` what was consumed. They never block
//! — they return `Ok(None)` when a full frame isn't buffered yet.

use crate::buffer::ring::RingBuffer;
use std::io;

/// Errors surfaced by codecs.
#[derive(Debug)]
pub enum CodecError {
    /// A length-prefixed frame declared more bytes than `max_frame`.
    FrameTooLarge { len: usize, max: usize },
    /// Buffer ended mid-frame.
    Incomplete,
    Io(io::Error),
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CodecError::FrameTooLarge { len, max } => {
                write!(f, "frame length {len} exceeds max {max}")
            }
            CodecError::Incomplete => write!(f, "incomplete frame"),
            CodecError::Io(e) => write!(f, "io error: {e}"),
        }
    }
}

impl std::error::Error for CodecError {}

impl From<io::Error> for CodecError {
    fn from(e: io::Error) -> Self {
        CodecError::Io(e)
    }
}

impl From<crate::buffer::ring::RingError> for CodecError {
    fn from(e: crate::buffer::ring::RingError) -> Self {
        CodecError::Io(io::Error::new(io::ErrorKind::Other, e))
    }
}

/// Line codec: frames are `\n`-terminated. `\r\n` is tolerated.
///
/// Usage inside `OnTraffic`:
/// ```ignore
/// while let Some(line) = LineCodec::next(&mut conn.inbound)? {
///     // handle line (without trailing \n)
/// }
/// ```
pub struct LineCodec;

impl LineCodec {
    /// Pop the next complete line. Returns `Ok(None)` if no `\n` is buffered.
    /// Consumes the line including its terminator.
    pub fn next(rb: &mut RingBuffer) -> Result<Option<Vec<u8>>, CodecError> {
        let (h, t) = rb.peek(0);
        // find \n across head+tail
        let mut idx = None;
        for (i, &b) in h.iter().enumerate() {
            if b == b'\n' {
                idx = Some((i, 0usize));
                break;
            }
        }
        if idx.is_none() {
            let off = h.len();
            for (j, &b) in t.iter().enumerate() {
                if b == b'\n' {
                    idx = Some((off + j, 1usize));
                    break;
                }
            }
        }
        let Some((pos, _)) = idx else {
            return Ok(None);
        };
        let total = h.len() + t.len();
        let mut line = Vec::with_capacity(pos);
        // copy bytes [0..pos) across head/tail
        if pos <= h.len() {
            line.extend_from_slice(&h[..pos]);
        } else {
            line.extend_from_slice(h);
            line.extend_from_slice(&t[..pos - h.len()]);
        }
        // strip trailing \r
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        let consume = pos + 1; // include \n
        rb.discard(consume);
        let _ = total;
        Ok(Some(line))
    }
}

/// Length-prefixed codec: `[u32 BE len][payload]`.
///
/// `next` returns the payload bytes of the next complete frame, or `Ok(None)`
/// if fewer than 4 header bytes or the full payload aren't buffered yet.
pub struct LengthPrefixCodec {
    /// Reject frames whose declared length exceeds this.
    pub max_frame: usize,
}

impl Default for LengthPrefixCodec {
    fn default() -> Self {
        Self {
            max_frame: 16 * 1024 * 1024,
        }
    }
}

impl LengthPrefixCodec {
    pub fn new(max_frame: usize) -> Self {
        Self { max_frame }
    }

    /// Encode a payload into a frame (header + body).
    pub fn encode(payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + payload.len());
        out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        out.extend_from_slice(payload);
        out
    }

    /// Try to decode the next frame from `rb`. Returns `Ok(None)` when
    /// incomplete; `Err(FrameTooLarge)` when header declares too much.
    pub fn next(&self, rb: &mut RingBuffer) -> Result<Option<Vec<u8>>, CodecError> {
        let buffered = rb.buffered();
        if buffered < 4 {
            return Ok(None);
        }
        let (h, _t) = rb.peek(4);
        let len = u32::from_be_bytes([h[0], h[1], h[2], h[3]]) as usize;
        if len > self.max_frame {
            return Err(CodecError::FrameTooLarge {
                len,
                max: self.max_frame,
            });
        }
        if buffered < 4 + len {
            return Ok(None);
        }
        rb.discard(4);
        let mut payload = vec![0u8; len];
        let mut off = 0;
        while off < len {
            let n = rb.read_into(&mut payload[off..])?;
            if n == 0 {
                break;
            }
            off += n;
        }
        Ok(Some(payload))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn line_codec_basic() {
        let mut rb = RingBuffer::new(64);
        rb.write(b"hello\nworld\n");
        assert_eq!(LineCodec::next(&mut rb).unwrap().unwrap(), b"hello");
        assert_eq!(LineCodec::next(&mut rb).unwrap().unwrap(), b"world");
        assert!(LineCodec::next(&mut rb).unwrap().is_none());
    }

    #[test]
    fn line_codec_crlf_and_partial() {
        let mut rb = RingBuffer::new(64);
        rb.write(b"a\r\nb");
        assert_eq!(LineCodec::next(&mut rb).unwrap().unwrap(), b"a");
        assert!(LineCodec::next(&mut rb).unwrap().is_none()); // "b" has no \n
        rb.write(b"\n");
        assert_eq!(LineCodec::next(&mut rb).unwrap().unwrap(), b"b");
    }

    #[test]
    fn line_codec_across_wrap() {
        let mut rb = RingBuffer::new(8);
        rb.write(b"one\ntwo\n"); // fills: positions 0..7
        let mut tmp = [0u8; 4];
        rb.read_into(&mut tmp).unwrap(); // consume "one\n" -> r=4
        rb.write(b"three\n"); // wraps
        // remaining buffered: "two\nthree\n"
        assert_eq!(LineCodec::next(&mut rb).unwrap().unwrap(), b"two");
        assert_eq!(LineCodec::next(&mut rb).unwrap().unwrap(), b"three");
        assert!(LineCodec::next(&mut rb).unwrap().is_none());
    }

    #[test]
    fn length_prefix_roundtrip() {
        let codec = LengthPrefixCodec::default();
        let mut rb = RingBuffer::new(128);
        let f1 = LengthPrefixCodec::encode(b"foo");
        let f2 = LengthPrefixCodec::encode(b"barbaz");
        rb.write(&f1);
        rb.write(&f2);
        assert_eq!(codec.next(&mut rb).unwrap().unwrap(), b"foo");
        assert_eq!(codec.next(&mut rb).unwrap().unwrap(), b"barbaz");
        assert!(codec.next(&mut rb).unwrap().is_none());
    }

    #[test]
    fn length_prefix_partial_header() {
        let codec = LengthPrefixCodec::default();
        let mut rb = RingBuffer::new(64);
        rb.write(&[0, 0, 0]); // only 3 of 4 header bytes
        assert!(codec.next(&mut rb).unwrap().is_none());
        rb.write(&[5, b'h', b'e']); // complete header len=5, partial payload
        assert!(codec.next(&mut rb).unwrap().is_none());
        rb.write(b"llo");
        // wait: header said len=5 but we only wrote 3 payload bytes "hel" then "lo" =5? recount
        // bytes written after header: 'h','e','l','l','o' = 5 bytes? we wrote "he" then "llo" => "hello" 5 bytes
        assert_eq!(codec.next(&mut rb).unwrap().unwrap(), b"hello");
    }

    #[test]
    fn length_prefix_too_large() {
        let codec = LengthPrefixCodec::new(4);
        let mut rb = RingBuffer::new(64);
        rb.write(&LengthPrefixCodec::encode(b"12345")); // len=5 > max 4
        match codec.next(&mut rb) {
            Err(CodecError::FrameTooLarge { len, max }) => {
                assert_eq!(len, 5);
                assert_eq!(max, 4);
            }
            other => panic!("expected FrameTooLarge, got {:?}", other.map(|o| o.map(|v| v.len()))),
        }
    }

    #[test]
    fn length_prefix_across_wrap() {
        let codec = LengthPrefixCodec::default();
        let mut rb = RingBuffer::new(16);
        // write enough to wrap: 16 cap. Fill, drain some, write frame.
        rb.write(&[0u8; 12]);
        let mut tmp = [0u8; 8];
        rb.read_into(&mut tmp).unwrap(); // r=8, w=12, buffered=4
        let frame = LengthPrefixCodec::encode(b"wrap-test"); // 4+9=13 bytes, total buffered becomes 4+13=17>16 grow
        rb.write(&frame);
        // drain leftover 4 zero bytes first
        let mut z = [0u8; 4];
        rb.read_into(&mut z).unwrap();
        assert_eq!(codec.next(&mut rb).unwrap().unwrap(), b"wrap-test");
    }
}
