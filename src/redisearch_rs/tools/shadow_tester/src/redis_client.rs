use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::thread::sleep;
use std::time::Duration;

pub struct RedisClient {
    stream: TcpStream,
    server_process: Child,
}

impl RedisClient {
    pub fn new(port: u16, rltest_path: &PathBuf, so_path: PathBuf) -> std::io::Result<Self> {
        // Start a RLTest server in the background
        let mut server_process = Command::new(rltest_path)
            .env("ENV_ONLY", "1")
            .env("REJSON", "0")
            .env("MODULE", so_path)
            .env("REDIS_PORT", port.to_string())
            .spawn()?;

        // Wait a few seconds for the connection to be ready
        for _ in 0..5 {
            let address = format!("127.0.0.1:{port}");

            match TcpStream::connect(&address) {
                Ok(stream) => {
                    println!("Redis server connnection ready at {address}");
                    stream.set_read_timeout(Some(Duration::from_millis(350)))?;

                    return Ok(Self {
                        stream,
                        server_process,
                    });
                }
                Err(_) => {
                    println!("Server not ready yet at {address}, retrying...");
                    sleep(Duration::from_secs(1));
                }
            }
        }

        kill_process(&mut server_process)?;

        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Server not ready",
        ))
    }

    pub fn query(&mut self, command: &str) -> std::io::Result<String> {
        self.stream.write_all(command.as_bytes())?;
        self.stream.write_all(b"\r\n")?;

        // Read all available data
        let mut buffer = Vec::new();
        loop {
            let mut chunk = [0; 1024];
            match self.stream.read(&mut chunk) {
                Ok(0) => break, // Connection closed (shouldn't happen with Redis)
                Ok(n) => buffer.extend_from_slice(&chunk[0..n]),
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    break;
                } // No more data available
                Err(e) => return Err(e), // Actual error
            }
        }

        Ok(String::from_utf8_lossy(&buffer).to_string())
    }
}

impl Drop for RedisClient {
    fn drop(&mut self) {
        println!("killing RLTest server");

        kill_process(&mut self.server_process).expect("to be able to kill the RLTest server");
    }
}

/// Kill the `runtests.sh` process correctly.
///
/// Technically `child` here has 3 processes:
/// - `runtests.sh` which will spawn
/// - `python3 -m RLTest` which will spawn
/// - `redis-server`
///
/// Now, the kill in this function will only kill the `runtests.sh` process. This means users
/// will have to manually kill the `python3 -m RLTest` process and the `redis-server` process
/// using a`SIGINT` signal.
///
/// However, there is a short cut here. Killing (`SIGINT`) the `redis-server` process will
/// automatically cause its `python3 -m RLTest` parent process to die as well. This just makes
/// cleaning up easier when you are iterating a lot.
fn kill_process(child: &mut Child) -> std::io::Result<()> {
    child.kill()?;
    child.wait()?;

    Ok(())
}
