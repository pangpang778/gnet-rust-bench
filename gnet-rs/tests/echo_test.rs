//! Integration tests: echo server over TCP, graceful shutdown, codec usage.

use gnet_rs::{run, Action, Conn, Engine, EventHandler, Options};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

struct Echo {
    conns: Arc<AtomicUsize>,
    engine: Arc<Mutex<Option<Engine>>>,
}

impl EventHandler for Echo {
    fn on_boot(&self, eng: Engine) -> Action {
        *self.engine.lock().unwrap() = Some(eng);
        Action::None
    }
    fn on_open(&self, _c: &mut Conn) -> (Option<Vec<u8>>, Action) {
        self.conns.fetch_add(1, Ordering::SeqCst);
        (None, Action::None)
    }
    fn on_traffic(&self, c: &mut Conn) -> Action {
        let data = c.inbound.peek(0);
        if data.is_empty() {
            return Action::None;
        }
        let n = data.len();
        c.inbound.discard(n);
        let _ = c.send(&data);
        Action::None
    }
    fn on_close(&self, _c: &mut Conn, _err: Option<&std::io::Error>) -> Action {
        self.conns.fetch_sub(1, Ordering::SeqCst);
        Action::None
    }
}

fn pick_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

#[test]
fn tcp_echo_roundtrip() {
    let port = pick_port();
    let addr = format!("tcp://127.0.0.1:{}", port);
    let conns = Arc::new(AtomicUsize::new(0));
    let engine_slot: Arc<Mutex<Option<Engine>>> = Arc::new(Mutex::new(None));
    let handler = Echo { conns: conns.clone(), engine: engine_slot.clone() };
    let addr2 = addr.clone();
    let server = std::thread::spawn(move || {
        run(handler, &addr2, Options::default().num_event_loop(1)).unwrap();
    });

    let mut stream = None;
    for _ in 0..100 {
        match TcpStream::connect(format!("127.0.0.1:{}", port)) {
            Ok(s) => { stream = Some(s); break; }
            Err(_) => std::thread::sleep(Duration::from_millis(20)),
        }
    }
    let mut stream = stream.expect("server did not come up");
    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    stream.set_write_timeout(Some(Duration::from_secs(5))).unwrap();

    let payload = b"hello gnet-rs echo!";
    stream.write_all(payload).unwrap();
    let mut buf = vec![0u8; payload.len()];
    stream.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, payload);

    let payload2 = b"second frame";
    stream.write_all(payload2).unwrap();
    let mut buf2 = vec![0u8; payload2.len()];
    stream.read_exact(&mut buf2).unwrap();
    assert_eq!(&buf2, payload2);

    drop(stream);
    for _ in 0..100 {
        if conns.load(Ordering::SeqCst) == 0 { break; }
        std::thread::sleep(Duration::from_millis(20));
    }

    {
        let eng = engine_slot.lock().unwrap();
        if let Some(e) = eng.as_ref() { e.stop(); }
    }
    let _ = server.join();
}

#[test]
fn noop_handler_boots_and_stops() {
    let port = pick_port();
    let addr = format!("tcp://127.0.0.1:{}", port);
    struct H { eng: Arc<Mutex<Option<Engine>>> }
    impl EventHandler for H {
        fn on_boot(&self, e: Engine) -> Action {
            *self.eng.lock().unwrap() = Some(e);
            Action::None
        }
    }
    let slot: Arc<Mutex<Option<Engine>>> = Arc::new(Mutex::new(None));
    let h = H { eng: slot.clone() };
    let addr2 = addr.clone();
    let t = std::thread::spawn(move || {
        let _ = run(h, &addr2, Options::default().num_event_loop(1));
    });
    std::thread::sleep(Duration::from_millis(300));
    {
        let g = slot.lock().unwrap();
        g.as_ref().unwrap().stop();
    }
    t.join().unwrap();
}
