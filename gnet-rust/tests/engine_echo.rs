//! End-to-end engine test: real TCP echo through `engine::run`.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use gnet::{engine, Action, Conn, Engine, EventHandler};

struct Echo {
    engine: Arc<std::sync::Mutex<Option<Engine>>>,
    accepted: Arc<AtomicUsize>,
    echoed: Arc<AtomicUsize>,
}

impl EventHandler for Echo {
    fn on_boot(&mut self, e: &Engine) -> Action {
        *self.engine.lock().unwrap() = Some(e.clone());
        Action::None
    }

    fn on_open(&mut self, conn: &mut Conn) -> (Vec<u8>, Action) {
        self.accepted.fetch_add(1, Ordering::SeqCst);
        let _ = conn.write(b"welcome\n");
        (Vec::new(), Action::None)
    }

    fn on_traffic(&mut self, conn: &mut Conn) -> Action {
        let mut buf = [0u8; 4096];
        let n = conn.read(&mut buf).unwrap_or(0);
        if n > 0 {
            self.echoed.fetch_add(n, Ordering::SeqCst);
            let _ = conn.write(&buf[..n]);
            conn.discard(n);
        }
        Action::None
    }
}

#[test]
fn tcp_echo_roundtrip_and_shutdown() {
    let engine_slot = Arc::new(std::sync::Mutex::new(None));
    let accepted = Arc::new(AtomicUsize::new(0));
    let echoed = Arc::new(AtomicUsize::new(0));

    let handler = Echo {
        engine: engine_slot.clone(),
        accepted: accepted.clone(),
        echoed: echoed.clone(),
    };

    // Fixed port keeps the test simple; retry-friendly bind.
    let addr = "tcp://127.0.0.1:18099";
    let server = thread::spawn(move || {
        engine::run_with(handler, addr, |o| {
            o.num_event_loop = 2;
        })
        .expect("engine run");
    });

    // Wait for boot.
    let eng = loop {
        if let Some(e) = engine_slot.lock().unwrap().clone() {
            break e;
        }
        thread::sleep(Duration::from_millis(20));
    };

    let mut client = TcpStream::connect("127.0.0.1:18099").expect("connect");
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // Read welcome banner.
    let mut banner = [0u8; 8];
    client.read_exact(&mut banner).expect("banner");
    assert_eq!(&banner, b"welcome\n");

    // Echo payload.
    client.write_all(b"hello gnet").unwrap();
    let mut reply = [0u8; 10];
    client.read_exact(&mut reply).expect("echo reply");
    assert_eq!(&reply, b"hello gnet");

    // Give the loop a beat to register traffic, then stop.
    thread::sleep(Duration::from_millis(100));
    assert!(accepted.load(Ordering::SeqCst) >= 1);
    assert!(echoed.load(Ordering::SeqCst) >= 10);

    // stop() from a separate thread is the supported pattern; here
    // we're already on a plain thread so call directly.
    eng.stop();
    server.join().expect("server thread");
    assert!(eng.is_shutdown());
}
