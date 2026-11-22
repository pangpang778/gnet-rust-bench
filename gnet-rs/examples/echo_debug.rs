use gnet_rs::conn::Conn;
use gnet_rs::{run, Action, EventHandler, Options};

struct EchoHandler;

impl EventHandler for EchoHandler {
    fn on_open(&self, c: &mut Conn) -> (Option<Vec<u8>>, Action) {
        println!("[{}] on_open", c.id);
        (None, Action::None)
    }
    fn on_traffic(&self, c: &mut Conn) -> Action {
        let n = c.inbound.buffered();
        println!("[{}] on_traffic: buffered={}", c.id, n);
        if n == 0 { return Action::None; }
        let mut buf = vec![0u8; n];
        let got = c.inbound.read_into(&mut buf);
        println!("[{}] read_into got={}", c.id, got);
        if got == 0 { return Action::None; }
        println!("[{}] sending {:?}", c.id, &buf[..got]);
        c.send(&buf[..got]).unwrap();
        Action::None
    }
    fn on_close(&self, c: &mut Conn, err: Option<&std::io::Error>) -> Action {
        println!("[{}] on_close err={:?}", c.id, err);
        Action::None
    }
}

fn main() {
    let addr = "tcp://127.0.0.1:9001";
    println!("starting on {}", addr);
    run(EchoHandler, addr, Options::default().num_event_loop(1)).unwrap();
}
