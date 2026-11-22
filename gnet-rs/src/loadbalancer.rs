//! Load balancing across event loops.
//!
//! Port of `gnet-go/load_balancer.go`. Selection is done by the acceptor
//! (or by `Engine::register`) picking the loop index for a new connection.

use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LoadBalancing {
    #[default]
    RoundRobin,
    LeastConnections,
    SourceAddrHash,
}

/// Trait object-safe: given the number of loops and per-loop connection
/// counts, return the loop index to assign a new connection to.
/// `remote_addr` is the string form used for hashing (e.g. "127.0.0.1:54321").
pub trait Balancer: Send + Sync {
    fn pick(&self, remote_addr: &str, conn_counts: &[usize]) -> usize;
}

/// Round-robin: cycles through loops.
pub struct RoundRobin {
    counter: AtomicU64,
}

impl RoundRobin {
    pub fn new() -> Self {
        Self {
            counter: AtomicU64::new(0),
        }
    }
}

impl Default for RoundRobin {
    fn default() -> Self {
        Self::new()
    }
}

impl Balancer for RoundRobin {
    fn pick(&self, _remote_addr: &str, conn_counts: &[usize]) -> usize {
        let n = conn_counts.len().max(1);
        let idx = self.counter.fetch_add(1, Ordering::Relaxed) as usize % n;
        idx
    }
}

/// Least-connections: picks the loop with fewest active connections.
pub struct LeastConnections;

impl Balancer for LeastConnections {
    fn pick(&self, _remote_addr: &str, conn_counts: &[usize]) -> usize {
        conn_counts
            .iter()
            .enumerate()
            .min_by_key(|(_, &c)| c)
            .map(|(i, _)| i)
            .unwrap_or(0)
    }
}

/// Source-address hash: CRC32 of the remote address string mod loop count.
/// We use a simple FNV-1a instead of CRC32 to avoid the crc32 dependency;
/// distribution quality is equivalent for this purpose.
pub struct SourceAddrHash;

impl Balancer for SourceAddrHash {
    fn pick(&self, remote_addr: &str, conn_counts: &[usize]) -> usize {
        let n = conn_counts.len().max(1);
        let mut h: u64 = 0xcbf29ce484222325;
        for &b in remote_addr.as_bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        (h % n as u64) as usize
    }
}

pub fn make_balancer(lb: LoadBalancing) -> Box<dyn Balancer> {
    match lb {
        LoadBalancing::RoundRobin => Box::new(RoundRobin::new()),
        LoadBalancing::LeastConnections => Box::new(LeastConnections),
        LoadBalancing::SourceAddrHash => Box::new(SourceAddrHash),
    }
}

/// Per-loop connection counters shared between acceptor and loops.
#[derive(Default)]
pub struct ConnCounts {
    counts: Vec<AtomicUsize>,
}

impl ConnCounts {
    pub fn new(n: usize) -> Self {
        Self {
            counts: (0..n).map(|_| AtomicUsize::new(0)).collect(),
        }
    }
    pub fn inc(&self, idx: usize) {
        self.counts[idx].fetch_add(1, Ordering::Relaxed);
    }
    pub fn dec(&self, idx: usize) {
        self.counts[idx].fetch_sub(1, Ordering::Relaxed);
    }
    pub fn snapshot(&self) -> Vec<usize> {
        self.counts
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .collect()
    }
    pub fn len(&self) -> usize {
        self.counts.len()
    }
}

// Keep `Hash`/`Hasher` imports used for future CRC option without warnings.
#[allow(dead_code)]
fn _unused_hash<T: Hash>(t: &T) -> u64 {
    let mut s = std::collections::hash_map::DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_robin_cycles() {
        let lb = RoundRobin::new();
        let counts = vec![0usize; 3];
        let picks: Vec<_> = (0..7)
            .map(|_| lb.pick("x", &counts))
            .collect();
        assert_eq!(picks, vec![0, 1, 2, 0, 1, 2, 0]);
    }

    #[test]
    fn least_connections_picks_min() {
        let lb = LeastConnections;
        let counts = vec![5, 2, 9];
        assert_eq!(lb.pick("any", &counts), 1);
        let counts2 = vec![0, 0, 0];
        assert_eq!(lb.pick("any", &counts2), 0);
    }

    #[test]
    fn source_addr_hash_is_stable() {
        let lb = SourceAddrHash;
        let counts = vec![0usize; 4];
        let a = lb.pick("127.0.0.1:1234", &counts);
        let b = lb.pick("127.0.0.1:1234", &counts);
        assert_eq!(a, b);
        assert!(a < 4);
        // different addrs should mostly spread
        let c = lb.pick("127.0.0.1:9999", &counts);
        assert!(c < 4);
    }

    #[test]
    fn conn_counts_inc_dec() {
        let cc = ConnCounts::new(2);
        cc.inc(0);
        cc.inc(0);
        cc.inc(1);
        assert_eq!(cc.snapshot(), vec![2, 1]);
        cc.dec(0);
        assert_eq!(cc.snapshot(), vec![1, 1]);
    }
}
