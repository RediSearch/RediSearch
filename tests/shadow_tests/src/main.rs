use std::io::{Read, Write};
use std::net::TcpStream;

fn main() -> std::io::Result<()> {
    let mut stream = TcpStream::connect("127.0.0.1:6379")?;

    stream.write_all(b"PING\r\n")?;

    let mut buffer = [0; 1024];
    let n = stream.read(&mut buffer)?;

    println!("Received: {}", String::from_utf8_lossy(&buffer[..n]));

    Ok(())
}
