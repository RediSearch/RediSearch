use std::io::{Read, Write};
use std::net::TcpStream;

fn main() -> std::io::Result<()> {
    let base_client = RedisClient::new("127.0.0.1:6379")?;
    let changeset_client = RedisClient::new("127.0.0.1:6380")?;

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
