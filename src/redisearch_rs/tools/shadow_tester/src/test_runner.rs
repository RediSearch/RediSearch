use crate::redis_client::RedisClient;

pub struct TestRunner {
    success: bool,
    commands: Vec<String>,
    base_client: RedisClient,
    changeset_client: RedisClient,
}

impl TestRunner {
    pub fn new(base_client: RedisClient, changeset_client: RedisClient) -> Self {
        TestRunner {
            success: true,
            commands: Vec::new(),
            base_client,
            changeset_client,
        }
    }

    pub fn add_command(&mut self, command: &str) {
        self.commands.push(command.to_string());
    }

    pub fn run(mut self) -> std::io::Result<bool> {
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
