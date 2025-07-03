use std::collections::HashMap;
use std::fmt::Debug;

use pretty_assertions::Comparison;
use redis::{Arg, Cmd, FromRedisValue, RedisResult, Value};

use crate::redis_client::RedisClient;

pub struct TestRunner {
    commands: Vec<Cmd>,
    base_client: RedisClient,
    changeset_client: RedisClient,
}

impl TestRunner {
    pub fn new(dataset: Vec<Cmd>, base_client: RedisClient, changeset_client: RedisClient) -> Self {
        TestRunner {
            commands: dataset,
            base_client,
            changeset_client,
        }
    }

    pub fn add_command(&mut self, command: Cmd) {
        self.commands.push(command);
    }

    pub fn run(mut self) -> RedisResult<bool> {
        let mut success = true;

        for command in self.commands.split_off(0) {
            let command_name = arg_to_str(
                command
                    .args_iter()
                    .next()
                    .expect("Command to startwith a command name"),
            );

            let s = match command_name.as_str() {
                "FT.SEARCH" => self.run_command::<FtSearchResponse>(command),
                _ => self.run_command::<Value>(command),
            };

            if !s {
                success = false;
            }
        }

        Ok(success)
    }

    fn run_command<T: FromRedisValue + PartialEq + Debug>(&mut self, command: Cmd) -> bool {
        let base_response: RedisResult<T> = self.base_client.query(&command);
        let changeset_response: RedisResult<T> = self.changeset_client.query(&command);

        if let Err(e) = &base_response {
            println!(
                "Error in base response for '{}': {e}",
                command
                    .args_iter()
                    .map(arg_to_str)
                    .collect::<Vec<_>>()
                    .join(" ")
            );
        }

        if base_response != changeset_response {
            let diff = Comparison::new(&base_response, &changeset_response);

            println!(
                "'{}' has different responses!",
                command
                    .args_iter()
                    .map(arg_to_str)
                    .collect::<Vec<_>>()
                    .join(" ")
            );
            println!("{diff}");

            false
        } else {
            true
        }
    }
}

/// Helper to turn a [`redis::Arg`] into a [`String`]
fn arg_to_str(arg: Arg<&[u8]>) -> String {
    let Arg::Simple(s) = arg else {
        panic!("Expected a simple argument");
    };

    String::from_utf8(s.to_vec()).expect("Arg to be valid UTF-8")
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
