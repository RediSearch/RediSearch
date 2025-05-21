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

        let mut buffer = [0; 1024];
        let n = self.stream.read(&mut buffer)?;

        Ok(String::from_utf8_lossy(&buffer[..n]).to_string())
    }
}

impl Drop for RedisClient {
    fn drop(&mut self) {
        println!("killing RLTest server");

        kill_process(&mut self.server_process).expect("to be able to kill the RLTest server");
    }
}

/// Kill the `runtests.sh` process correctly.
/// Technically this has an issue since the `runtests.sh` process does not correctly stop the
/// RLTest process it spawns when it is killed. To kill the RLTest process, one has to manually
/// send a ``SIGINT` signal to the `redis-server` it spawned.
fn kill_process(child: &mut Child) -> std::io::Result<()> {
    child.kill()?;
    child.wait()?;

    Ok(())
}
