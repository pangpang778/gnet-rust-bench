//! Non-blocking I/O buffers.
//!
//! - [`RingBuffer`] is the inbound ring buffer: a fixed-capacity circular
//!   buffer over a single `Vec<u8>` with separate read/write positions.
//! - [`LinkedListBuffer`] is a FIFO of byte chunks used for outbound data
//!   that outgrows the ring.
//! - [`ElasticBuffer`] combines the two for outbound writes, mirroring
//!   gnet's `pkg/buffer/elastic`: a small ring first, then a linked list.

use std::collections::VecDeque;
use std::fmt;

const DEFAULT_BUFFER_SIZE: usize = 1024;
const GROW_THRESHOLD: usize = 4 * 1024;

/// Error returned when reading from an empty buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmptyError;

impl fmt::Display for EmptyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("buffer is empty")
    }
}

impl std::error::Error for EmptyError {}

/// Error returned when peeking or discarding more bytes than are buffered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShortBufferError;

impl fmt::Display for ShortBufferError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("buffer shorter than requested")
    }
}

impl std::error::Error for ShortBufferError {}

/// Round `n` up to the next power of two.
fn ceil_pow2(mut n: usize) -> usize {
    if n == 0 {
        return 1;
    }
    n -= 1;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    n += 1;
    n
}

/// Contiguous read view over buffered bytes: up to two contiguous slices,
/// covering the ring wrap-around case.
#[derive(Debug, Clone, Copy)]
pub struct Peek<'a> {
    head: &'a [u8],
    tail: &'a [u8],
}

impl<'a> Peek<'a> {
    /// First contiguous chunk. Empty when nothing is buffered.
    pub fn head(&self) -> &'a [u8] {
        self.head
    }

    /// Second chunk, non-empty only when the data wraps around the ring end.
    pub fn tail(&self) -> &'a [u8] {
        self.tail
    }

    /// Iterate over both chunks in order, skipping empty ones.
    pub fn chunks(&self) -> [&'a [u8]; 2] {
        [self.head, self.tail]
    }

    /// Total bytes across both chunks.
    pub fn len(&self) -> usize {
        self.head.len() + self.tail.len()
    }

    /// True when no bytes are buffered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A circular buffer over a single contiguous allocation.
///
/// Data occupies the region between `r` (next read position) and `w`
/// (next write position), wrapping around the end of the allocation.
/// `is_empty` disambiguates the `r == w` full/empty case.
#[derive(Debug)]
pub struct RingBuffer {
    buf: Vec<u8>,
    r: usize,
    w: usize,
    is_empty: bool,
}

impl Default for RingBuffer {
    fn default() -> Self {
        Self::new(0)
    }
}

impl RingBuffer {
    /// Create a ring buffer with the given capacity.
    ///
    /// The capacity is rounded up to a power of two; `0` starts empty
    /// and allocates lazily on the first write.
    pub fn new(capacity: usize) -> Self {
        if capacity == 0 {
            return Self {
                buf: Vec::new(),
                r: 0,
                w: 0,
                is_empty: true,
            };
        }
        let size = ceil_pow2(capacity);
        Self {
            buf: vec![0; size],
            r: 0,
            w: 0,
            is_empty: true,
        }
    }

    /// Peek up to `n` buffered bytes without advancing the read position.
    ///
    /// Returns a [`Peek`] view of one or two contiguous slices covering the
    /// bytes in order. `n <= 0` peeks at everything buffered.
    pub fn peek(&self, n: usize) -> Peek<'_> {
        if self.is_empty {
            return Peek {
                head: &[],
                tail: &[],
            };
        }
        let want = if n == 0 || n > self.buffered() {
            self.buffered()
        } else {
            n
        };
        if self.w > self.r {
            let m = (self.w - self.r).min(want);
            return Peek {
                head: &self.buf[self.r..self.r + m],
                tail: &[],
            };
        }
        // w <= r: data wraps around (or the buffer is completely full).
        let total = self.size() - self.r + self.w;
        let m = total.min(want);
        if self.r + m <= self.size() {
            Peek {
                head: &self.buf[self.r..self.r + m],
                tail: &[],
            }
        } else {
            let c1 = self.size() - self.r;
            Peek {
                head: &self.buf[self.r..],
                tail: &self.buf[..m - c1],
            }
        }
    }

    /// Discard up to `n` bytes by advancing the read position.
    ///
    /// Returns the number of bytes actually discarded.
    pub fn discard(&mut self, n: usize) -> usize {
        if n == 0 {
            return 0;
        }
        let buffered = self.buffered();
        if n < buffered {
            self.r = (self.r + n) % self.size();
            n
        } else {
            self.reset();
            buffered
        }
    }

    /// Copy up to `buf.len()` bytes into `buf` and advance the read
    /// position. Returns the number of bytes read.
    ///
    /// Errors with [`EmptyError`] when the buffer is empty.
    pub fn read(&mut self, buf: &mut [u8]) -> Result<usize, EmptyError> {
        if buf.is_empty() {
            return Ok(0);
        }
        if self.is_empty {
            return Err(EmptyError);
        }
        let n = self.buffered().min(buf.len());
        self.copy_to_front(n, buf);
        self.advance_read(n);
        Ok(n)
    }

    /// Read a single byte.
    pub fn read_byte(&mut self) -> Result<u8, EmptyError> {
        if self.is_empty {
            return Err(EmptyError);
        }
        let b = self.buf[self.r];
        self.r += 1;
        if self.r == self.size() {
            self.r = 0;
        }
        if self.r == self.w {
            self.reset();
        }
        Ok(b)
    }

    /// Write all of `data`, growing the allocation when it does not fit.
    pub fn write(&mut self, data: &[u8]) -> usize {
        if data.is_empty() {
            return 0;
        }
        let free = self.available();
        if data.len() > free {
            self.grow(self.size() + data.len() - free);
        }
        if self.w >= self.r {
            let c1 = self.size() - self.w;
            if c1 >= data.len() {
                self.buf[self.w..self.w + data.len()].copy_from_slice(data);
                self.w += data.len();
            } else {
                self.buf[self.w..].copy_from_slice(&data[..c1]);
                let c2 = data.len() - c1;
                self.buf[..c2].copy_from_slice(&data[c1..]);
                self.w = c2;
            }
        } else {
            self.buf[self.w..self.w + data.len()].copy_from_slice(data);
            self.w += data.len();
        }
        if self.w == self.size() {
            self.w = 0;
        }
        self.is_empty = false;
        data.len()
    }

    /// Write a single byte, growing when the buffer is full.
    pub fn write_byte(&mut self, c: u8) {
        if self.available() < 1 {
            self.grow(1);
        }
        self.buf[self.w] = c;
        self.w += 1;
        if self.w == self.size() {
            self.w = 0;
        }
        self.is_empty = false;
    }

    /// Write a string slice.
    pub fn write_str(&mut self, s: &str) -> usize {
        self.write(s.as_bytes())
    }

    /// Number of bytes available to read.
    pub fn buffered(&self) -> usize {
        if self.r == self.w {
            if self.is_empty {
                0
            } else {
                self.size()
            }
        } else if self.w > self.r {
            self.w - self.r
        } else {
            self.size() - self.r + self.w
        }
    }

    /// Length of the underlying allocation.
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Whether the underlying allocation is empty.
    pub fn is_allocated(&self) -> bool {
        !self.buf.is_empty()
    }

    /// Capacity of the underlying allocation (same as [`len`](Self::len)).
    pub fn capacity(&self) -> usize {
        self.buf.len()
    }

    /// Number of bytes that can still be written without growing.
    pub fn available(&self) -> usize {
        if self.r == self.w {
            if self.is_empty {
                self.size()
            } else {
                0
            }
        } else if self.w < self.r {
            self.r - self.w
        } else {
            self.size() - self.w + self.r
        }
    }

    /// All buffered bytes copied into a single `Vec`.
    /// Does not move the read position.
    pub fn bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.buffered());
        for chunk in self.peek(0).chunks().iter().filter(|s| !s.is_empty()) {
            out.extend_from_slice(chunk);
        }
        out
    }

    /// Whether the buffer is full (no room to write without growing).
    pub fn is_full(&self) -> bool {
        self.r == self.w && !self.is_empty
    }

    /// Whether the buffer holds no readable bytes.
    pub fn is_empty(&self) -> bool {
        self.is_empty
    }

    /// Reset read/write positions, keeping the allocation.
    pub fn reset(&mut self) {
        self.is_empty = true;
        self.r = 0;
        self.w = 0;
    }

    /// Next buffered byte without consuming it, or `None` if empty.
    pub fn next_byte(&self) -> Option<u8> {
        if self.is_empty {
            None
        } else {
            Some(self.buf[self.r])
        }
    }

    fn size(&self) -> usize {
        self.buf.len()
    }

    fn copy_to_front(&self, n: usize, dst: &mut [u8]) {
        if self.w > self.r || self.r + n <= self.size() {
            let end = self.r + n;
            dst[..n].copy_from_slice(&self.buf[self.r..end]);
        } else {
            let c1 = self.size() - self.r;
            dst[..c1].copy_from_slice(&self.buf[self.r..]);
            dst[c1..n].copy_from_slice(&self.buf[..n - c1]);
        }
    }

    fn advance_read(&mut self, n: usize) {
        self.r = (self.r + n) % self.size();
        if self.r == self.w {
            self.reset();
        }
    }

    fn grow(&mut self, min_capacity: usize) {
        let old_size = self.size();
        let new_cap = if old_size == 0 {
            DEFAULT_BUFFER_SIZE.max(min_capacity)
        } else {
            let double = old_size.saturating_mul(2);
            if min_capacity <= double {
                if old_size < GROW_THRESHOLD {
                    double
                } else {
                    // Grow by 1.25x steps like the Go implementation.
                    let mut n = old_size;
                    while n < min_capacity {
                        n = n.saturating_add(n / 4).max(n + 1);
                    }
                    n.max(min_capacity)
                }
            } else {
                ceil_pow2(min_capacity)
            }
        };
        let mut new_buf = vec![0u8; new_cap];
        let old_len = self.buffered();
        // Copy buffered bytes contiguously to the front of the new buffer.
        let mut written = 0;
        for chunk in self.peek(0).chunks().iter().filter(|s| !s.is_empty()) {
            new_buf[written..written + chunk.len()].copy_from_slice(chunk);
            written += chunk.len();
        }
        debug_assert_eq!(written, old_len);
        self.buf = new_buf;
        self.r = 0;
        self.w = old_len;
        self.is_empty = old_len == 0;
    }
}

/// A FIFO of byte chunks: the overflow side of [`ElasticBuffer`].
///
/// Chunks are stored in a `VecDeque`; `head`/`tail` byte offsets track
/// partial consumption of the front chunk. Rust-safe equivalent of
/// gnet's linked-list buffer.
#[derive(Debug, Default)]
pub struct LinkedListBuffer {
    chunks: VecDeque<(usize, usize)>, // (start, end) into `data`
    data: Vec<u8>,
}

impl LinkedListBuffer {
    /// Create an empty list buffer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append `bytes`, copying them into the internal arena.
    pub fn push_back(&mut self, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        let start = self.data.len();
        self.data.extend_from_slice(bytes);
        self.chunks.push_back((start, self.data.len()));
    }

    /// Append `bytes` without copying (always copies here; kept for API
    /// parity with the Go version).
    pub fn append(&mut self, bytes: &[u8]) {
        self.push_back(bytes)
    }

    /// Copy up to `buf.len()` bytes into `buf` in FIFO order.
    /// Returns the number of bytes read, or [`EmptyError`] if empty.
    pub fn read(&mut self, buf: &mut [u8]) -> Result<usize, EmptyError> {
        if buf.is_empty() {
            return Ok(0);
        }
        if self.is_empty() {
            return Err(EmptyError);
        }
        let mut n = 0;
        while n < buf.len() {
            let Some(&(start, end)) = self.chunks.front() else {
                break;
            };
            let avail = end - start;
            if avail == 0 {
                self.chunks.pop_front();
                continue;
            }
            let take = avail.min(buf.len() - n);
            buf[n..n + take].copy_from_slice(&self.data[start..start + take]);
            n += take;
            if take == avail {
                self.chunks.pop_front();
            } else if let Some(front) = self.chunks.front_mut() {
                front.0 = start + take;
            }
        }
        Ok(n)
    }

    /// Peek at up to `n` bytes, returned as a `Vec` of slices in order.
    ///
    /// Errors with [`ShortBufferError`] when `n` exceeds buffered bytes.
    /// `n == 0` peeks at everything.
    pub fn peek(&self, n: usize) -> Result<Vec<&[u8]>, ShortBufferError> {
        let want = if n == 0 { usize::MAX } else { n };
        let buffered = self.buffered();
        if want != usize::MAX && want > buffered {
            return Err(ShortBufferError);
        }
        let mut out = Vec::new();
        let mut cum = 0usize;
        for &(start, end) in &self.chunks {
            let avail = end - start;
            if avail == 0 {
                continue;
            }
            let take = avail.min(want.saturating_sub(cum));
            out.push(&self.data[start..start + take]);
            cum += take;
            if cum >= want || cum >= buffered {
                break;
            }
        }
        Ok(out)
    }

    /// Discard up to `n` bytes from the front. Returns bytes discarded.
    pub fn discard(&mut self, n: usize) -> usize {
        let mut remaining = n;
        let mut discarded = 0;
        while remaining > 0 {
            let Some(&(start, end)) = self.chunks.front() else {
                break;
            };
            let avail = end - start;
            if avail <= remaining {
                self.chunks.pop_front();
                remaining -= avail;
                discarded += avail;
            } else {
                if let Some(front) = self.chunks.front_mut() {
                    front.0 = start + remaining;
                }
                discarded += remaining;
                remaining = 0;
            }
        }
        discarded
    }

    /// Number of bytes available to read.
    pub fn buffered(&self) -> usize {
        self.chunks.iter().map(|&(s, e)| e - s).sum()
    }

    /// Number of chunks in the list.
    pub fn len(&self) -> usize {
        self.chunks.len()
    }

    /// Whether the list holds no bytes.
    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    /// Drop all chunks. The byte arena is kept for reuse.
    pub fn reset(&mut self) {
        self.chunks.clear();
        self.data.clear();
    }

    /// Remove and return the front chunk's bytes (copy).
    pub fn pop(&mut self) -> Option<Vec<u8>> {
        let (start, end) = self.chunks.pop_front()?;
        Some(self.data[start..end].to_vec())
    }

    /// Total arena capacity retained.
    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }
}

/// Hybrid outbound buffer: a [`RingBuffer`] up to `max_static_bytes`,
/// overflowing into a [`LinkedListBuffer`].
///
/// Port of gnet's `pkg/buffer/elastic.Buffer`.
#[derive(Debug)]
pub struct ElasticBuffer {
    max_static_bytes: usize,
    ring: RingBuffer,
    list: LinkedListBuffer,
}

impl ElasticBuffer {
    /// Create an elastic buffer. `max_static_bytes` must be positive.
    pub fn new(max_static_bytes: usize) -> Result<Self, &'static str> {
        if max_static_bytes == 0 {
            return Err("max_static_bytes must be positive");
        }
        Ok(Self {
            max_static_bytes,
            ring: RingBuffer::new(max_static_bytes),
            list: LinkedListBuffer::new(),
        })
    }

    /// Copy up to `buf.len()` bytes out, draining the ring first then the list.
    pub fn read(&mut self, buf: &mut [u8]) -> Result<usize, EmptyError> {
        match self.ring.read(buf) {
            Ok(n) if n == buf.len() => Ok(n),
            Ok(n) => {
                if buf.is_empty() {
                    return Ok(n);
                }
                match self.list.read(&mut buf[n..]) {
                    Ok(m) => Ok(n + m),
                    Err(EmptyError) => Ok(n),
                }
            }
            Err(EmptyError) => match self.list.read(buf) {
                Ok(n) => Ok(n),
                Err(EmptyError) => Err(EmptyError),
            },
        }
    }

    /// Peek at up to `n` bytes across ring and list without consuming.
    ///
    /// `n == 0` or `n > buffered` peeks at everything.
    pub fn peek(&self, n: usize) -> Result<Vec<Vec<u8>>, ShortBufferError> {
        let buffered = self.buffered();
        let want = if n == 0 || n > buffered { buffered } else { n };
        if want == 0 {
            return Ok(Vec::new());
        }
        let ring_peek = self.ring.peek(want);
        let ring_bytes: usize = ring_peek.len();
        let mut out: Vec<Vec<u8>> = ring_peek
            .chunks()
            .iter()
            .filter(|s| !s.is_empty())
            .map(|c| c.to_vec())
            .collect();
        if ring_bytes < want {
            let rest = want - ring_bytes;
            match self.list.peek(rest) {
                Ok(chunks) => out.extend(chunks.into_iter().map(|c| c.to_vec())),
                Err(ShortBufferError) => {
                    // List holds less than requested; append what it has.
                    if let Ok(chunks) = self.list.peek(0) {
                        out.extend(chunks.into_iter().map(|c| c.to_vec()));
                    }
                }
            }
        }
        Ok(out)
    }

    /// Discard up to `n` bytes, draining the ring first then the list.
    /// Returns bytes actually discarded.
    pub fn discard(&mut self, n: usize) -> usize {
        let from_ring = self.ring.discard(n);
        if from_ring >= n {
            return from_ring;
        }
        from_ring + self.list.discard(n - from_ring)
    }

    /// Append `data`: to the ring while it fits under the static cap,
    /// otherwise to the list.
    pub fn write(&mut self, data: &[u8]) -> usize {
        if data.is_empty() {
            return 0;
        }
        if !self.list.is_empty() || self.ring.buffered() >= self.max_static_bytes {
            self.list.push_back(data);
            return data.len();
        }
        if self.ring.len() >= self.max_static_bytes {
            let writable = self.ring.available();
            if data.len() > writable {
                self.ring.write(&data[..writable]);
                self.list.push_back(&data[writable..]);
                return data.len();
            }
        }
        self.ring.write(data)
    }

    /// Append multiple slices.
    pub fn writev(&mut self, bufs: &[&[u8]]) -> usize {
        if bufs.is_empty() {
            return 0;
        }
        if !self.list.is_empty() || self.ring.buffered() >= self.max_static_bytes {
            let mut n = 0;
            for b in bufs {
                self.list.push_back(b);
                n += b.len();
            }
            return n;
        }
        let mut writable = if self.ring.len() < self.max_static_bytes {
            self.max_static_bytes - self.ring.buffered()
        } else {
            self.ring.available()
        };
        let mut cum = 0;
        let mut consumed = 0;
        for b in bufs {
            cum += b.len();
            if b.len() > writable {
                self.ring.write(&b[..writable]);
                self.list.push_back(&b[writable..]);
                consumed += 1;
                break;
            }
            let n = self.ring.write(b);
            writable = writable.saturating_sub(n);
            consumed += 1;
        }
        for b in &bufs[consumed..] {
            cum += b.len();
            self.list.push_back(b);
        }
        cum
    }

    /// Number of bytes available to read across both buffers.
    pub fn buffered(&self) -> usize {
        self.ring.buffered() + self.list.buffered()
    }

    /// Whether both buffers are empty.
    pub fn is_empty(&self) -> bool {
        self.ring.is_empty() && self.list.is_empty()
    }

    /// Reset both buffers, optionally updating the static cap.
    pub fn reset(&mut self, max_static_bytes: Option<usize>) {
        self.ring.reset();
        self.list.reset();
        if let Some(n) = max_static_bytes {
            if n > 0 {
                self.max_static_bytes = n;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------- RingBuffer ----------

    #[test]
    fn ring_write_read_basic() {
        let mut rb = RingBuffer::new(16);
        assert_eq!(rb.write(b"hello"), 5);
        assert_eq!(rb.buffered(), 5);
        assert!(!rb.is_empty());
        let mut out = [0u8; 16];
        assert_eq!(rb.read(&mut out).unwrap(), 5);
        assert_eq!(&out[..5], b"hello");
        assert!(rb.is_empty());
        assert_eq!(rb.read(&mut out), Err(EmptyError));
    }

    #[test]
    fn ring_wraparound() {
        let mut rb = RingBuffer::new(8); // capacity rounded to 8
        rb.write(b"abcd"); // w=4
        let mut out = [0u8; 4];
        rb.read(&mut out).unwrap(); // r=4
        assert_eq!(&out, b"abcd");
        rb.write(b"efgh"); // w wraps: w=0, data at 4..8 then 0..0? w=4+4=8->0
                           // Actually w was 4, free = size - w + r? r==w case handled below.
        rb.write(b"ij");
        assert_eq!(rb.buffered(), 6);
        let mut out = [0u8; 16];
        let n = rb.read(&mut out).unwrap();
        assert_eq!(&out[..n], b"efghij");
    }

    #[test]
    fn ring_peek_no_advance() {
        let mut rb = RingBuffer::new(8);
        rb.write(b"hello world");
        let p = rb.peek(5);
        assert_eq!(p.head(), b"hello");
        assert_eq!(p.tail(), b"");
        assert_eq!(rb.buffered(), 11);
        rb.discard(5);
        let p = rb.peek(0);
        let all: Vec<u8> = p.chunks().into_iter().flatten().copied().collect();
        assert_eq!(all, b" world");
    }

    #[test]
    fn ring_peek_wrap_returns_two_chunks() {
        let mut rb = RingBuffer::new(8);
        rb.write(b"abcdefgh"); // fills completely? cap 8, buffered 8, is_full
        let mut out = [0u8; 3];
        rb.read(&mut out).unwrap(); // r=3
        rb.write(b"XYZ"); // w was 0 after wrap; writes at 0..3? w=0
                          // Now buffered = 8-3+3 = 8? No: r=3,w=3,full -> buffered=8. Hmm.
                          // Simpler: craft explicit wrap.
        let mut rb = RingBuffer::new(8);
        rb.write(b"aaaa");
        rb.write(b"bbbb"); // w=0 (wrapped), r=0, buffered=8 full
        rb.discard(6); // r=6, buffered=2 ("bb" at positions 6,7)
        rb.write(b"cc"); // w=2
                         // buffered: r=6,w=2 -> size-r+w = 8-6+2 = 4 -> "bb" + "cc"
        assert_eq!(rb.buffered(), 4);
        let p = rb.peek(0);
        let all: Vec<u8> = p.chunks().into_iter().flatten().copied().collect();
        assert_eq!(all, b"bbcc");
        assert_eq!(p.head(), b"bb");
        assert_eq!(p.tail(), b"cc");
    }

    #[test]
    fn ring_discard_semantics() {
        let mut rb = RingBuffer::new(8);
        rb.write(b"hello");
        assert_eq!(rb.discard(2), 2);
        assert_eq!(rb.buffered(), 3);
        assert_eq!(rb.discard(100), 3);
        assert!(rb.is_empty());
        assert_eq!(rb.discard(10), 0);
    }

    #[test]
    fn ring_grow_preserves_data() {
        let mut rb = RingBuffer::new(4);
        rb.write(b"abcdef"); // forces grow beyond 4
        assert_eq!(rb.buffered(), 6);
        let mut out = [0u8; 16];
        let n = rb.read(&mut out).unwrap();
        assert_eq!(&out[..n], b"abcdef");
    }

    #[test]
    fn ring_full_capacity_no_growth_on_exact_fit() {
        let mut rb = RingBuffer::new(8);
        rb.write(b"12345678");
        assert_eq!(rb.buffered(), 8);
        assert_eq!(rb.len(), 8);
        assert!(rb.is_full());
        assert_eq!(rb.available(), 0);
        // one more byte triggers growth
        rb.write_byte(b'9');
        assert!(rb.buffered() > 8);
        let mut out = Vec::new();
        let mut tmp = [0u8; 32];
        while let Ok(n) = rb.read(&mut tmp) {
            out.extend_from_slice(&tmp[..n]);
            if n == 0 {
                break;
            }
        }
        assert_eq!(out, b"123456789");
    }

    #[test]
    fn ring_bytes_and_write_str() {
        let mut rb = RingBuffer::new(32);
        rb.write_str("abc");
        rb.write(b"def");
        assert_eq!(rb.bytes(), b"abcdef");
        assert_eq!(rb.next_byte(), Some(b'a'));
        rb.discard(1);
        assert_eq!(rb.next_byte(), Some(b'b'));
    }

    // ---------- LinkedListBuffer ----------

    #[test]
    fn ll_push_read_order() {
        let mut ll = LinkedListBuffer::new();
        ll.push_back(b"foo");
        ll.push_back(b"bar");
        ll.push_back(b"baz");
        assert_eq!(ll.buffered(), 9);
        assert_eq!(ll.len(), 3);
        let mut out = [0u8; 9];
        assert_eq!(ll.read(&mut out).unwrap(), 9);
        assert_eq!(&out, b"foobarbaz");
        assert!(ll.is_empty());
        assert_eq!(ll.read(&mut out), Err(EmptyError));
    }

    #[test]
    fn ll_partial_read_and_discard() {
        let mut ll = LinkedListBuffer::new();
        ll.push_back(b"hello");
        ll.push_back(b"world");
        let mut out = [0u8; 7];
        assert_eq!(ll.read(&mut out).unwrap(), 7);
        assert_eq!(&out, b"hellowo");
        assert_eq!(ll.buffered(), 3);
        assert_eq!(ll.discard(2), 2);
        assert_eq!(ll.buffered(), 1);
        let chunks = ll.peek(0).unwrap();
        let all: Vec<u8> = chunks.concat();
        assert_eq!(all, b"d");
    }

    #[test]
    fn ll_peek_short_buffer() {
        let mut ll = LinkedListBuffer::new();
        ll.push_back(b"ab");
        ll.push_back(b"cd");
        assert!(ll.peek(5).is_err());
        let chunks = ll.peek(3).unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0], b"ab");
        assert_eq!(chunks[1], b"c");
        // peek does not consume
        assert_eq!(ll.buffered(), 4);
    }

    #[test]
    fn ll_reset_and_pop() {
        let mut ll = LinkedListBuffer::new();
        ll.push_back(b"xy");
        ll.push_back(b"z");
        assert_eq!(ll.pop().unwrap(), b"xy");
        assert_eq!(ll.pop().unwrap(), b"z");
        assert!(ll.pop().is_none());
        ll.push_back(b"q");
        ll.reset();
        assert!(ll.is_empty());
        assert_eq!(ll.buffered(), 0);
    }

    // ---------- ElasticBuffer ----------

    #[test]
    fn elastic_stays_in_ring_under_cap() {
        let mut eb = ElasticBuffer::new(64).unwrap();
        eb.write(b"hello");
        assert_eq!(eb.buffered(), 5);
        let mut out = [0u8; 5];
        assert_eq!(eb.read(&mut out).unwrap(), 5);
        assert_eq!(&out, b"hello");
        assert!(eb.is_empty());
    }

    #[test]
    fn elastic_overflow_to_list() {
        let mut eb = ElasticBuffer::new(8).unwrap();
        eb.write(b"aaaabbbb"); // fills ring to cap
        eb.write(b"cccc"); // must spill to list
        assert_eq!(eb.buffered(), 12);
        let mut out = [0u8; 12];
        assert_eq!(eb.read(&mut out).unwrap(), 12);
        assert_eq!(&out, b"aaaabbbbcccc");
        assert!(eb.is_empty());
    }

    #[test]
    fn elastic_list_persists_after_ring_drains() {
        let mut eb = ElasticBuffer::new(8).unwrap();
        eb.write(b"12345678");
        eb.write(b"WXYZ");
        // Drain only the ring's worth.
        let mut out = [0u8; 8];
        assert_eq!(eb.read(&mut out).unwrap(), 8);
        assert_eq!(&out, b"12345678");
        // List data still readable.
        let mut out2 = [0u8; 4];
        assert_eq!(eb.read(&mut out2).unwrap(), 4);
        assert_eq!(&out2, b"WXYZ");
        assert!(eb.is_empty());
    }

    #[test]
    fn elastic_discard_across_boundary() {
        let mut eb = ElasticBuffer::new(8).unwrap();
        eb.write(b"12345678");
        eb.write(b"WXYZ");
        assert_eq!(eb.discard(10), 10);
        assert_eq!(eb.buffered(), 2);
        let chunks = eb.peek(0).unwrap();
        let all: Vec<u8> = chunks.concat();
        assert_eq!(all, b"YZ");
    }

    #[test]
    fn elastic_peek_and_writev() {
        let mut eb = ElasticBuffer::new(16).unwrap();
        let bufs: [&[u8]; 3] = [b"aa", b"bb", b"cc"];
        assert_eq!(eb.writev(&bufs), 6);
        let chunks = eb.peek(4).unwrap();
        let all: Vec<u8> = chunks.concat();
        assert_eq!(all, b"aabb");
        eb.discard(6);
        assert!(eb.is_empty());
    }

    #[test]
    fn elastic_writev_overflow() {
        let mut eb = ElasticBuffer::new(4).unwrap();
        let bufs: [&[u8]; 3] = [b"aabb", b"ccdd", b"eeff"];
        let n = eb.writev(&bufs);
        assert_eq!(n, 12);
        assert_eq!(eb.buffered(), 12);
        let mut out = [0u8; 12];
        assert_eq!(eb.read(&mut out).unwrap(), 12);
        assert_eq!(&out, b"aabbccddeeff");
    }

    #[test]
    fn elastic_reset() {
        let mut eb = ElasticBuffer::new(8).unwrap();
        eb.write(b"12345678");
        eb.write(b"overflow");
        eb.reset(Some(16));
        assert!(eb.is_empty());
        eb.write(b"new");
        assert_eq!(eb.buffered(), 3);
    }

    #[test]
    fn elastic_rejects_zero_cap() {
        assert!(ElasticBuffer::new(0).is_err());
    }

    // ---------- randomized round-trip ----------

    #[test]
    fn ring_random_ops_consistent() {
        // Simple deterministic PRNG (xorshift64).
        let mut state = 0x9E3779B97F4A7C15u64;
        let mut rng = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let mut rb = RingBuffer::new(16);
        let mut model: Vec<u8> = Vec::new();
        for _ in 0..2000 {
            match rng() % 3 {
                0 => {
                    let n = (rng() % 20) as usize;
                    let data: Vec<u8> = (0..n).map(|_| (rng() % 256) as u8).collect();
                    rb.write(&data);
                    model.extend_from_slice(&data);
                }
                1 => {
                    let n = (rng() % 15) as usize;
                    let mut buf = vec![0u8; n];
                    if let Ok(got) = rb.read(&mut buf) {
                        let want: Vec<u8> = model.drain(..got.min(model.len())).collect();
                        assert_eq!(&buf[..got], &want[..]);
                    }
                }
                _ => {
                    let n = (rng() % 15) as usize;
                    let dropped = rb.discard(n);
                    let want = dropped.min(model.len());
                    model.drain(..want);
                }
            }
            // Verify peek-all equals model.
            let all: Vec<u8> = rb.peek(0).chunks().into_iter().flatten().copied().collect();
            assert_eq!(all, model);
            assert_eq!(rb.buffered(), model.len());
        }
    }
}
