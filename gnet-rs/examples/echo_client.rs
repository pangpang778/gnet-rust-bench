use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;
fn main() -> std::io::Result<()> {
    let mut s = TcpStream::connect("127.0.0.1:9000")?;
    s.set_read_timeout(Some(Duration::from_secs(5)))?;
    s.write_all(b"test123")?;
    let mut b = vec![0u8; 7];
    s.read_exact(&mut b)?;
    assert_eq!(&b, b"test123");
    println!("ok");
    Ok(())
}
