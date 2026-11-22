//! Load balancers for distributing accepted connections across
//! backends (event loops / workers).
//!
//! Port of `gnet-go/load_balancer.go`: RoundRobin, LeastConnections
//! and SourceAddrHash.

use std::net::SocketAddr;

/// Picks which backend handles the next accepted connection.
///
/// `T` is the backend identifier (event-loop handle, worker id, …).
/// Implementations are generic over it; indices returned by
/// [`Balancer::next`] refer to registration order via
/// [`Balancer::add`].
pub trait Balancer<T> {
    /// Register a backend, returning its index.
    fn add(&mut self, backend: T) -> usize;

    /// Pick the index of the backend for the next connection.
    ///
    /// `source` is the remote address of the incoming connection, used
    /// by hash-based balancers. Returns `None` when no backends exist.
    fn next(&mut self, source: Option<&SocketAddr>) -> Option<usize>;

    /// Number of registered backends.
    fn len(&self) -> usize;

    /// Whether no backends are registered.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Notify the balancer that a connection was assigned to `index`.
    ///
    /// Only [`LeastConnections`] needs this; the default is a no-op.
    fn conn_opened(&mut self, _index: usize) {}

    /// Notify the balancer that a connection on `index` closed.
    fn conn_closed(&mut self, _index: usize) {}
}

/// Round-robin: hands out backends in registration order, cycling.
#[derive(Debug)]
pub struct RoundRobin<T> {
    backends: Vec<T>,
    next_index: usize,
}

impl<T> RoundRobin<T> {
    /// Create an empty balancer.
    pub fn new() -> Self {
        Self {
            backends: Vec::new(),
            next_index: 0,
        }
    }

    /// Create a balancer pre-populated with `backends`.
    pub fn with_backends(backends: Vec<T>) -> Self {
        Self {
            backends,
            next_index: 0,
        }
    }
}

impl<T> Default for RoundRobin<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Balancer<T> for RoundRobin<T> {
    fn add(&mut self, backend: T) -> usize {
        let index = self.backends.len();
        self.backends.push(backend);
        index
    }

    fn next(&mut self, _source: Option<&SocketAddr>) -> Option<usize> {
        if self.backends.is_empty() {
            return None;
        }
        let index = self.next_index % self.backends.len();
        self.next_index = self.next_index.wrapping_add(1);
        Some(index)
    }

    fn len(&self) -> usize {
        self.backends.len()
    }
}

/// Least-connections: picks the backend serving the fewest active
/// connections. Ties resolve to the lowest index, matching the Go
/// implementation.
#[derive(Debug)]
pub struct LeastConnections<T> {
    backends: Vec<T>,
    active: Vec<usize>,
}

impl<T> LeastConnections<T> {
    /// Create an empty balancer.
    pub fn new() -> Self {
        Self {
            backends: Vec::new(),
            active: Vec::new(),
        }
    }

    /// Create a balancer pre-populated with `backends`.
    pub fn with_backends(backends: Vec<T>) -> Self {
        let active = vec![0; backends.len()];
        Self { backends, active }
    }
}

impl<T> Default for LeastConnections<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Balancer<T> for LeastConnections<T> {
    fn add(&mut self, backend: T) -> usize {
        let index = self.backends.len();
        self.backends.push(backend);
        self.active.push(0);
        index
    }

    fn next(&mut self, _source: Option<&SocketAddr>) -> Option<usize> {
        if self.backends.is_empty() {
            return None;
        }
        let mut best = 0;
        for (i, &count) in self.active.iter().enumerate() {
            if count < self.active[best] {
                best = i;
            }
        }
        Some(best)
    }

    fn len(&self) -> usize {
        self.backends.len()
    }

    fn conn_opened(&mut self, index: usize) {
        if let Some(count) = self.active.get_mut(index) {
            *count += 1;
        }
    }

    fn conn_closed(&mut self, index: usize) {
        if let Some(count) = self.active.get_mut(index) {
            *count = count.saturating_sub(1);
        }
    }
}

/// Source-address hash: maps the remote address to a backend with
/// FNV-1a hashing, so a given source always lands on the same backend
/// while the backend set is stable.
#[derive(Debug)]
pub struct SourceAddrHash<T> {
    backends: Vec<T>,
}

impl<T> SourceAddrHash<T> {
    /// Create an empty balancer.
    pub fn new() -> Self {
        Self {
            backends: Vec::new(),
        }
    }

    /// Create a balancer pre-populated with `backends`.
    pub fn with_backends(backends: Vec<T>) -> Self {
        Self { backends }
    }
}

impl<T> Default for SourceAddrHash<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// FNV-1a 64-bit hash — small, deterministic, no dependencies.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

impl<T> Balancer<T> for SourceAddrHash<T> {
    fn add(&mut self, backend: T) -> usize {
        let index = self.backends.len();
        self.backends.push(backend);
        index
    }

    fn next(&mut self, source: Option<&SocketAddr>) -> Option<usize> {
        if self.backends.is_empty() {
            return None;
        }
        // Missing source hashes to a fixed bucket — deterministic and
        // consistent with the Go implementation's fixed-point behavior.
        let key = source.map_or_else(String::new, |a| a.to_string());
        Some((fnv1a(key.as_bytes()) as usize) % self.backends.len())
    }

    fn len(&self) -> usize {
        self.backends.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), port)
    }

    #[test]
    fn round_robin_cycles_through_backends() {
        let mut lb = RoundRobin::with_backends(vec!["a", "b", "c"]);
        let picks: Vec<usize> = (0..7).map(|_| lb.next(None).unwrap()).collect();
        assert_eq!(picks, vec![0, 1, 2, 0, 1, 2, 0]);
        assert_eq!(lb.len(), 3);
    }

    #[test]
    fn round_robin_empty_returns_none() {
        let mut lb: RoundRobin<u8> = RoundRobin::new();
        assert_eq!(lb.next(None), None);
        assert!(lb.is_empty());
    }

    #[test]
    fn round_robin_add_assigns_sequential_indices() {
        let mut lb = RoundRobin::new();
        assert_eq!(lb.add("x"), 0);
        assert_eq!(lb.add("y"), 1);
        assert_eq!(lb.next(None), Some(0));
        assert_eq!(lb.next(None), Some(1));
    }

    #[test]
    fn least_connections_picks_least_loaded() {
        let mut lb = LeastConnections::with_backends(vec![0u8, 1, 2]);
        // All at zero: first index wins the tie, like the Go version.
        assert_eq!(lb.next(None), Some(0));
        lb.conn_opened(0);
        assert_eq!(lb.next(None), Some(1));
        lb.conn_opened(1);
        lb.conn_opened(1);
        // Backend 2 is now the least loaded.
        assert_eq!(lb.next(None), Some(2));
        lb.conn_opened(2);
        // All equal again -> lowest index.
        assert_eq!(lb.next(None), Some(0));
    }

    #[test]
    fn least_connections_conn_closed_releases_capacity() {
        let mut lb = LeastConnections::with_backends(vec![0u8, 1]);
        lb.conn_opened(0);
        lb.conn_opened(0);
        assert_eq!(lb.next(None), Some(1));
        lb.conn_closed(0);
        // Still 1 vs 0 -> backend 1 stays least loaded.
        assert_eq!(lb.next(None), Some(1));
        lb.conn_closed(0);
        assert_eq!(lb.next(None), Some(0));
    }

    #[test]
    fn least_connections_empty_returns_none() {
        let mut lb: LeastConnections<u8> = LeastConnections::new();
        assert_eq!(lb.next(None), None);
    }

    #[test]
    fn source_addr_hash_is_stable_per_address() {
        let mut lb = SourceAddrHash::with_backends(vec![0u8, 1, 2, 3]);
        let a = addr(1000);
        let first = lb.next(Some(&a)).unwrap();
        for _ in 0..10 {
            assert_eq!(lb.next(Some(&a)), Some(first));
        }
    }

    #[test]
    fn source_addr_hash_distributes_across_backends() {
        let mut lb = SourceAddrHash::with_backends(vec![0u8, 1, 2, 3]);
        let mut seen = [false; 4];
        for port in 1024..2048u16 {
            let idx = lb.next(Some(&addr(port))).unwrap();
            seen[idx] = true;
        }
        assert!(
            seen.iter().all(|&s| s),
            "expected all backends hit, got {seen:?}"
        );
    }

    #[test]
    fn source_addr_hash_empty_returns_none() {
        let mut lb: SourceAddrHash<u8> = SourceAddrHash::new();
        assert_eq!(lb.next(Some(&addr(1))), None);
    }

    #[test]
    fn source_addr_hash_no_source_is_deterministic() {
        let mut lb = SourceAddrHash::with_backends(vec![0u8, 1, 2]);
        let first = lb.next(None).unwrap();
        assert_eq!(lb.next(None), Some(first));
    }
}
