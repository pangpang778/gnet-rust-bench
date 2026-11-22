use gnet_rs::conn::Conn;
use gnet_rs::{run, Action, EventHandler, Options};

struct EchoHandler;

impl EventHandler for EchoHandler {
    fn on_open(&self, _c: &mut Conn) -> (Option<Vec<u8>>, Action) {
        println!("connection opened");
        (None, Action::None)
    }
    fn on_traffic(&self, c: &mut Conn) -> Action {
        println!("on_traffic called");
        let n = c.inbound.buffered();
        println!("buffered={}", n);
        if n == 0 { return Action::None; }
        let mut buf = vec![0u8; n];
        let got = c.inbound.read_into(&mut buf);
        println!("got={}", got);
        if got == 0 { return Action::None; }
        c.send(&buf[..got]).unwrap();
        Action::None
    }
    fn on_close(&self, _c: &mut Conn, _err: Option<&std::io::Error>) -> Action { Action::None }
}

fn main() {
    println!("starting");
    run(EchoHandler, "tcp://127.0.0.1:9000", Options::default().num_event_loop(1)).unwrap();
}
