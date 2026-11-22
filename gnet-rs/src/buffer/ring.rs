//! Inbound ring buffer: fixed-capacity power-of-two circular buffer with
//! `peek` (non-advancing, may return head+tail), `discard` (advance), and
//! grow-on-write like the Go implementation.
//!
//! Port of `gnet-go/pkg/buffer/ring`.

use std::cmp;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RingError {
    IsEmpty,
}

impl std::fmt::Display for RingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RingError::IsEmpty => write!(f, "ring-buffer is empty"),
        }
    }
}

impl std::error::Error for RingError {}

pub const MIN_READ: usize = 512;
pub const DEFAULT_BUFFER_SIZE: usize = 1024;
const BUFFER_GROW_THRESHOLD: usize = 4 * 1024;

/// Ceil to power of two (returns value unchanged if already power of two,
/// minimum 1).
pub fn ceil_to_power_of_two(n: usize) -> usize {
    if n == 0 {
        return 1;
    }
    if n.is_power_of_two() {
        return n;
    }
    n.next_power_of_two()
}

pub struct RingBuffer {
    buf: Vec<u8>,
    /// logical size = buf.len() (kept implicit in Go as `size`; here buf.len()).
    r: usize,
    w: usize,
    is_empty: bool,
}

impl RingBuffer {
    /// New returns a ring buffer with the given capacity (rounded up to power
    /// of two). A zero size allocates lazily.
    pub fn new(size: usize) -> Self {
        if size == 0 {
            return Self {
                buf: Vec::new(),
                r: 0,
                w: 0,
                is_empty: true,
            };
        }
        let size = ceil_to_power_of_two(size);
        Self {
            buf: vec![0u8; size],
            r: 0,
            w: 0,
            is_empty: true,
        }
    }

    #[inline]
    fn size(&self) -> usize {
        self.buf.len()
    }

    /// Peek returns up to `n` readable bytes as (head, tail). `n <= 0`
    /// returns all readable bytes.
    ///
    /// The returned slices borrow `self` — callers must `discard` or `read`
    /// before mutating.
    pub fn peek(&self, n: usize) -> (&[u8], &[u8]) {
        if self.is_empty {
            return (&[], &[]);
        }
        if n == 0 {
            return self.peek_all();
        }
        let m = self.buffered().min(n);
        if m == 0 {
            return (&[], &[]);
        }
        if self.w > self.r {
            return (&self.buf[self.r..self.r + m], &[]);
        }
        // w <= r: wrapped region. head = buf[r..min(r+m, size)], tail = buf[..remaining]
        if self.r + m <= self.size() {
            return (&self.buf[self.r..self.r + m], &[]);
        }
        let c1 = self.size() - self.r;
        let c2 = m - c1;
        (&self.buf[self.r..], &self.buf[..c2])
    }

    /// Returns all readable bytes without advancing the read pointer.
    /// Prefer `peek_all_iov` style: returns two slices for zero-copy writev.
    pub fn peek_all(&self) -> (&[u8], &[u8]) {
        if self.is_empty {
            return (&[], &[]);
        }
        if self.w > self.r {
            return (&self.buf[self.r..self.w], &[]);
        }
        // w <= r
        if self.w == self.r {
            // full buffer (is_empty false and r==w means full)
            return (&self.buf[self.r..], &self.buf[..self.w]);
        }
        let head = &self.buf[self.r..];
        let tail = if self.w != 0 { &self.buf[..self.w] } else { &[] };
        (head, tail)
    }

    /// Convenience: copy all readable bytes into a Vec (allocation).
    pub fn bytes(&self) -> Vec<u8> {
        let (h, t) = self.peek_all();
        let mut v = Vec::with_capacity(h.len() + t.len());
        v.extend_from_slice(h);
        v.extend_from_slice(t);
        v
    }

    /// Discard advances the read pointer by n bytes, returning how many were
    /// actually discarded.
    pub fn discard(&mut self, n: usize) -> usize {
        if n == 0 {
            return 0;
        }
        let buffered = self.buffered();
        if n < buffered {
            self.r = (self.r + n) % self.size();
            return n;
        }
        let discarded = buffered;
        self.reset();
        discarded
    }

    /// Read copies up to `p.len()` bytes into `p`, advancing the read pointer.
    /// Returns bytes copied. Returns `RingError::IsEmpty` if empty and `p`
    /// non-empty.
    pub fn read_into(&mut self, p: &mut [u8]) -> Result<usize, RingError> {
        if p.is_empty() {
            return Ok(0);
        }
        if self.is_empty {
            return Err(RingError::IsEmpty);
        }
        let buffered = self.buffered();
        let n = buffered.min(p.len());
        if self.w > self.r {
            p[..n].copy_from_slice(&self.buf[self.r..self.r + n]);
        } else {
            let c1 = (self.size() - self.r).min(n);
            p[..c1].copy_from_slice(&self.buf[self.r..self.r + c1]);
            if c1 < n {
                let c2 = n - c1;
                p[c1..n].copy_from_slice(&self.buf[..c2]);
            }
        }
        self.r = (self.r + n) % self.size();
        if self.r == self.w {
            self.reset();
        }
        Ok(n)
    }

    /// Read one byte.
    pub fn read_byte(&mut self) -> Result<u8, RingError> {
        if self.is_empty {
            return Err(RingError::IsEmpty);
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

    /// Write appends all of `p`, growing the buffer if needed. Always consumes
    /// all input (grows as required).
    pub fn write(&mut self, p: &[u8]) -> usize {
        let n = p.len();
        if n == 0 {
            return 0;
        }
        let free = self.available();
        if n > free {
            self.grow(self.size() + n - free);
        }
        // copy p into buf starting at w, wrapping once
        if self.w >= self.r {
            let c1 = self.size() - self.w;
            if c1 >= n {
                self.buf[self.w..self.w + n].copy_from_slice(p);
                self.w += n;
            } else {
                self.buf[self.w..].copy_from_slice(&p[..c1]);
                let c2 = n - c1;
                self.buf[..c2].copy_from_slice(&p[c1..]);
                self.w = c2;
            }
        } else {
            // w < r: space before r is contiguous from w
            self.buf[self.w..self.w + n].copy_from_slice(p);
            self.w += n;
        }
        if self.w == self.size() {
            self.w = 0;
        }
        self.is_empty = false;
        n
    }

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

    /// Number of readable bytes.
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

    /// Capacity of underlying buffer.
    pub fn cap(&self) -> usize {
        self.size()
    }

    /// Bytes available for writing.
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

    pub fn is_full(&self) -> bool {
        self.buffered() == self.size() && self.size() > 0
    }

    pub fn is_empty(&self) -> bool {
        self.is_empty
    }

    pub fn reset(&mut self) {
        self.r = 0;
        self.w = 0;
        self.is_empty = true;
    }

    /// Grow to hold at least `min_size` bytes of capacity.
    fn grow(&mut self, min_size: usize) {
        let old_size = self.size();
        let new_size = cmp::max(
            ceil_to_power_of_two(min_size.max(old_size * 2)),
            BUFFER_GROW_THRESHOLD,
        );
        let mut new_buf = vec![0u8; new_size];
        // copy readable data out linearly
        let data = self.bytes();
        new_buf[..data.len()].copy_from_slice(&data);
        self.buf = new_buf;
        self.r = 0;
        self.w = data.len();
        self.is_empty = data.is_empty();
    }
}

impl Default for RingBuffer {
    fn default() -> Self {
        Self::new(DEFAULT_BUFFER_SIZE)
    }
}
