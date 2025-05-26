use std::collections::HashMap;
use std::fmt::Debug;

use pretty_assertions::Comparison;
use redis::{FromRedisValue, RedisResult, Value};

use crate::redis_client::RedisClient;

/// Command and arguments for a Redis query
#[derive(Clone)]
struct Command {
    command: String,
    args: Vec<String>,
}

pub struct TestRunner {
    commands: Vec<Command>,
    base_client: RedisClient,
    changeset_client: RedisClient,
}

impl TestRunner {
    pub fn new(
        dataset: Vec<(String, Vec<String>)>,
        base_client: RedisClient,
        changeset_client: RedisClient,
    ) -> Self {
        let commands = dataset
            .into_iter()
            .map(|(command, args)| Command { command, args })
            .collect();

        TestRunner {
            commands,
            base_client,
            changeset_client,
        }
    }

    pub fn add_command(&mut self, command: &str, args: &[&str]) {
        self.commands.push(Command {
            command: command.to_string(),
            args: args.iter().map(|s| s.to_string()).collect(),
        });
    }

    pub fn run(mut self) -> RedisResult<bool> {
        let mut success = true;

        for Command { command, args } in &self.commands.clone() {
            let s = match command.as_str() {
                "FT.SEARCH" => self.run_command::<FtSearchResponse>(command, args),
                _ => self.run_command::<Value>(command, args),
            };

            if !s {
                success = false;
            }
        }

        Ok(success)
    }

    fn run_command<T: FromRedisValue + PartialEq + Debug>(
        &mut self,
        command: &str,
        args: &[String],
    ) -> bool {
        let base_response: RedisResult<T> = self.base_client.query(command, args);
        let changeset_response: RedisResult<T> = self.changeset_client.query(command, args);

        if let Err(e) = &base_response {
            println!(
                "Error in base response for '{command} {}': {e}",
                args.join(" ")
            );
        }

        if base_response != changeset_response {
            let diff = Comparison::new(&base_response, &changeset_response);

            println!("'{command} {}' has different responses!", args.join(" "));
            println!("{diff}");

            false
        } else {
            true
        }
    }
}

/// Response from the FT.SEARCH command
#[derive(Debug, PartialEq)]
struct FtSearchResponse {
    ///Total number of results
    total: usize,

    /// HashMap of results, where the key is the document ID and the value is a HashMap of fields
    results: HashMap<String, HashMap<String, Value>>,
}

impl FromRedisValue for FtSearchResponse {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let Value::Array(values) = v else {
            todo!("not an array");
        };

        let total = usize::from_redis_value(&values[0])?;
        let result_vec = <(String, HashMap<String, Value>)>::from_redis_values(&values[1..])?;

        let results = HashMap::from_iter(result_vec.into_iter());

        Ok(Self { total, results })
    }
}
