use std::io::{Read, Write};
use std::net::TcpStream;

fn main() -> std::io::Result<()> {
    let mut client = RedisClient::new("127.0.0.1:6379")?;
    let response = client.query("PING")?;

    println!("Response: {}", response);

    Ok(())
}

struct RedisClient {
    stream: TcpStream,
}

impl RedisClient {
    fn new(address: &str) -> std::io::Result<Self> {
        let stream = TcpStream::connect(address)?;
        Ok(RedisClient { stream })
    }

    fn query(&mut self, command: &str) -> std::io::Result<String> {
        self.stream.write_all(command.as_bytes())?;
        self.stream.write_all(b"\r\n")?;

        let mut buffer = [0; 1024];
        let n = self.stream.read(&mut buffer)?;

        Ok(String::from_utf8_lossy(&buffer[..n]).to_string())
    }
}
