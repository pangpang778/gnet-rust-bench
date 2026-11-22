use gnet_rs::buffer::ring::RingBuffer;
fn main() {
    let mut rb = RingBuffer::new(8);
    rb.write(b"abcdef");
    let mut tmp = [0u8; 2];
    rb.read_into(&mut tmp).unwrap();
    rb.write(b"XYZ");
    println!("buffered={}", rb.buffered());
    let (h, t) = rb.peek(5);
    println!("peek(5): h={:?} t={:?}", h, t);
    let (h, t) = rb.peek(7);
    println!("peek(7): h={:?} t={:?}", h, t);
}
