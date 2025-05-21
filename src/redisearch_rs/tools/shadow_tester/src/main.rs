use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::thread::sleep;
use std::time::Duration;

fn main() -> std::io::Result<()> {
    let base_client = RedisClient::new(6379)?;
    let changeset_client = RedisClient::new(6380)?;

    let mut test_runner = TestRunner::new(base_client, changeset_client);
    test_runner.add_command("PING");

    let success = test_runner.run()?;

    if success {
        println!("All queries are the same!");
    } else {
        println!("Some queries failed.");
    }

    Ok(())
}

struct TestRunner {
    success: bool,
    commands: Vec<String>,
    base_client: RedisClient,
    changeset_client: RedisClient,
}

impl TestRunner {
    fn new(base_client: RedisClient, changeset_client: RedisClient) -> Self {
        TestRunner {
            success: true,
            commands: Vec::new(),
            base_client,
            changeset_client,
        }
    }

    fn add_command(&mut self, command: &str) {
        self.commands.push(command.to_string());
    }

    fn run(mut self) -> std::io::Result<bool> {
        for command in &self.commands {
            let base_response = self.base_client.query(command)?;
            let changeset_response = self.changeset_client.query(command)?;

            if base_response != changeset_response {
                eprintln!("Command: {} failed", command);
                eprintln!("Base response: {}", base_response);
                eprintln!("Changeset response: {}", changeset_response);
                self.success = false;
            }
        }

        Ok(self.success)
    }
}

struct RedisClient {
    stream: TcpStream,
    server_process: Child,
}

impl RedisClient {
    fn new(port: u16) -> std::io::Result<Self> {
        // Start a RLTest server in the background
        let mut server_process = Command::new("../../tests/pytests/runtests.sh")
            .env("ENV_ONLY", "1")
            .env("REJSON", "0")
            .env(
                "MODULE",
                "../../bin/linux-x64-release/search-community/redisearch.so",
            )
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

    fn query(&mut self, command: &str) -> std::io::Result<String> {
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
