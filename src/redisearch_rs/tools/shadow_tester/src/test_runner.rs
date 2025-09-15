use crate::redis_client::RedisClient;

pub struct TestRunner {
    commands: Vec<String>,
    base_client: RedisClient,
    changeset_client: RedisClient,
}

impl TestRunner {
    pub fn new(
        dataset: Vec<String>,
        base_client: RedisClient,
        changeset_client: RedisClient,
    ) -> Self {
        TestRunner {
            commands: dataset,
            base_client,
            changeset_client,
        }
    }

    pub fn add_command(&mut self, command: &str) {
        self.commands.push(command.to_string());
    }

    pub fn run(mut self) -> std::io::Result<bool> {
        let mut success = true;

        for command in &self.commands {
            let base_response = self.base_client.query(command)?;
            let changeset_response = self.changeset_client.query(command)?;

            if base_response != changeset_response {
                eprintln!("Command: {} failed", command);
                eprintln!("Base response: {}", base_response);
                eprintln!("Changeset response: {}", changeset_response);
                success = false;
            }
        }

        Ok(success)
    }
}
