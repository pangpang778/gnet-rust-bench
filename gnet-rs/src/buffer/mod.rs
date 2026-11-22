pub mod ring;

pub mod elastic {
    //! Elastic outbound buffer: a ring buffer backed by a fixed `max_static`
    //! capacity; overflow goes to a linked-list of byte slices so large writes
    //! do not force reallocation of the ring.
    //!
    //! Port of `gnet-go/pkg/buffer/elastic/elastic_ring_list_buffer.go`.

    use super::ring::RingBuffer;
    use std::collections::VecDeque;

    pub struct Buffer {
        max_static_bytes: usize,
        ring: RingBuffer,
        list: VecDeque<Vec<u8>>,
        buffered_list: usize,
    }

    impl Buffer {
        pub fn new(max_static_bytes: usize) -> Self {
            assert!(max_static_bytes > 0, "max_static_bytes must be positive");
            let ring_size = super::ring::ceil_to_power_of_two(max_static_bytes.max(1));
            Self {
                max_static_bytes,
                ring: RingBuffer::new(ring_size),
                list: VecDeque::new(),
                buffered_list: 0,
            }
        }

        /// Write appends `p` to the buffer. Always consumes all of `p`.
        pub fn write(&mut self, p: &[u8]) -> usize {
            if p.is_empty() {
                return 0;
            }
            // Once the list is engaged, or the ring already holds >= max_static
            // bytes, everything goes to the list to keep packet ordering.
            if !self.list.is_empty() || self.ring.buffered() >= self.max_static_bytes {
                self.list.push_back(p.to_vec());
                self.buffered_list += p.len();
                return p.len();
            }
            // Ring already at its static capacity ceiling: fill remaining ring
            // space, overflow the rest to the list.
            if self.ring.cap() >= self.max_static_bytes {
                let writable = self.ring.available().min(self.max_static_bytes);
                if p.len() > writable {
                    let (head, tail) = p.split_at(writable);
                    self.ring.write(head);
                    self.list.push_back(tail.to_vec());
                    self.buffered_list += tail.len();
                    return p.len();
                }
            }
            self.ring.write(p)
        }

        /// Peek returns up to `n` bytes as iovecs (head, tail) without
        /// advancing. `n <= 0` or `n > buffered` returns all buffered bytes.
        /// Returns owned Vecs to avoid borrow issues — the common case is a
        /// single contiguous head.
        pub fn peek(&self, n: usize) -> Vec<&[u8]> {
            let want = if n == 0 || n > self.buffered() {
                self.buffered()
            } else {
                n
            };
            let mut out: Vec<&[u8]> = Vec::new();
            let mut remaining = want;
            // ring part
            let ring_len = self.ring.buffered();
            let take_ring = ring_len.min(remaining);
            if take_ring > 0 {
                let (h, t) = self.ring.peek(take_ring);
                if !h.is_empty() {
                    out.push(h);
                }
                if !t.is_empty() {
                    out.push(t);
                }
            }
            remaining -= take_ring;
            // list part
            for chunk in self.list.iter() {
                if remaining == 0 {
                    break;
                }
                let take = chunk.len().min(remaining);
                out.push(&chunk[..take]);
                remaining -= take;
            }
            out
        }

        /// Discard skips `n` bytes, returning how many were discarded.
        pub fn discard(&mut self, n: usize) -> usize {
            let from_ring = self.ring.discard(n);
            if n <= from_ring {
                return from_ring;
            }
            let mut remaining = n - from_ring;
            let mut discarded = from_ring;
            while remaining > 0 {
                match self.list.front_mut() {
                    Some(front) => {
                        if front.len() <= remaining {
                            remaining -= front.len();
                            discarded += front.len();
                            self.buffered_list -= front.len();
                            self.list.pop_front();
                        } else {
                            front.drain(..remaining);
                            self.buffered_list -= remaining;
                            discarded += remaining;
                            remaining = 0;
                        }
                    }
                    None => break,
                }
            }
            discarded
        }

        pub fn read_into(&mut self, p: &mut [u8]) -> usize {
            let mut n = 0;
            if !self.ring.is_empty() {
                n += self.ring.read_into(&mut p[..]).unwrap_or(0);
            }
            if n < p.len() {
                let mut off = n;
                while off < p.len() {
                    match self.list.front_mut() {
                        Some(front) => {
                            let take = front.len().min(p.len() - off);
                            p[off..off + take].copy_from_slice(&front[..take]);
                            off += take;
                            if take == front.len() {
                                self.buffered_list -= front.len();
                                self.list.pop_front();
                            } else {
                                front.drain(..take);
                                self.buffered_list -= take;
                            }
                        }
                        None => break,
                    }
                }
                n = off;
            }
            n
        }

        pub fn buffered(&self) -> usize {
            self.ring.buffered() + self.buffered_list
        }

        pub fn is_empty(&self) -> bool {
            self.ring.is_empty() && self.list.is_empty()
        }

        pub fn reset(&mut self) {
            self.ring.reset();
            self.list.clear();
            self.buffered_list = 0;
        }
    }
}

#[cfg(test)]
mod ring_tests {
    use super::ring::RingBuffer;
    use super::*;

    #[test]
    fn ring_write_read_basic() {
        let mut rb = RingBuffer::new(16);
        assert_eq!(rb.write(b"hello"), 5);
        assert_eq!(rb.buffered(), 5);
        let (h, t) = rb.peek(0);
        assert_eq!(h, b"hello");
        assert!(t.is_empty());
        let mut buf = [0u8; 5];
        assert_eq!(rb.read_into(&mut buf).unwrap(), 5);
        assert_eq!(&buf, b"hello");
        assert!(rb.is_empty());
    }

    #[test]
    fn ring_wraparound() {
        let mut rb = RingBuffer::new(8);
        rb.write(b"abcdef"); // w=6 r=0
        let mut tmp = [0u8; 2];
        rb.read_into(&mut tmp).unwrap(); // r=2
        rb.write(b"XYZ"); // writes at 6,7,0 -> w=1
        assert_eq!(rb.buffered(), 7);
        let all = rb.bytes();
        assert_eq!(all, b"cdefXYZ");
        let mut out = [0u8; 7];
        assert_eq!(rb.read_into(&mut out).unwrap(), 7);
        assert_eq!(&out, b"cdefXYZ");
    }

    #[test]
    fn ring_discard() {
        let mut rb = RingBuffer::new(8);
        rb.write(b"abcdef");
        assert_eq!(rb.discard(2), 2);
        assert_eq!(rb.bytes(), b"cdef");
        assert_eq!(rb.discard(100), 4);
        assert!(rb.is_empty());
    }

    #[test]
    fn ring_peek_partial_wrap() {
        let mut rb = RingBuffer::new(8);
        rb.write(b"abcdef");
        let mut tmp = [0u8; 2];
        rb.read_into(&mut tmp).unwrap();
        rb.write(b"XYZ");
        // readable = "cdefXYZ" at positions r=2..8,0..1
        // peek(5) fits inside the contiguous head region buf[2..7] = "cdefX"
        let (h, t) = rb.peek(5);
        assert_eq!(h, b"cdefX");
        assert!(t.is_empty());
        // peek(7) spans the wrap: head buf[2..8]="cdefXY", tail buf[..1]="Z"
        let (h, t) = rb.peek(7);
        assert_eq!(h, b"cdefXY");
        assert_eq!(t, b"Z");
    }

    #[test]
    fn ring_grow() {
        let mut rb = RingBuffer::new(4);
        let big = vec![7u8; 100];
        rb.write(&big);
        assert_eq!(rb.buffered(), 100);
        let got = rb.bytes();
        assert_eq!(got, big);
    }

    #[test]
    fn elastic_overflow_to_list() {
        let mut b = elastic::Buffer::new(8);
        b.write(b"12345678"); // fills ring
        b.write(b"ABCDEFGH"); // overflows to list
        assert_eq!(b.buffered(), 16);
        let iov = b.peek(0);
        let total: usize = iov.iter().map(|s| s.len()).sum();
        assert_eq!(total, 16);
        let mut out = vec![0u8; 16];
        assert_eq!(b.read_into(&mut out), 16);
        assert_eq!(&out[..8], b"12345678");
        assert_eq!(&out[8..], b"ABCDEFGH");
        assert!(b.is_empty());
    }

    #[test]
    fn elastic_discard_across_ring_and_list() {
        let mut b = elastic::Buffer::new(8);
        b.write(b"12345678");
        b.write(b"ABCDEFGH");
        assert_eq!(b.discard(10), 10);
        assert_eq!(b.buffered(), 6);
        let mut out = vec![0u8; 6];
        b.read_into(&mut out);
        // "12345678ABCDEFGH" minus first 10 bytes = "CDEFGH"
        assert_eq!(&out, b"CDEFGH");
    }
}
