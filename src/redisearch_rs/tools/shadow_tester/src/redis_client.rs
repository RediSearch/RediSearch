use std::path::PathBuf;
use std::process::{Child, Command};
use std::thread::sleep;
use std::time::Duration;

use redis::{cmd, Client, Connection, FromRedisValue, RedisResult, ToRedisArgs};

pub struct RedisClient {
    server_process: Child,
    connection: Connection,
}

impl RedisClient {
    pub fn new(port: u16, rltest_path: &PathBuf, so_path: PathBuf) -> std::io::Result<Self> {
        // Start a RLTest server in the background
        let server_process = Command::new(rltest_path)
            .env("ENV_ONLY", "1")
            .env("REJSON", "0")
            .env("MODULE", so_path)
            .env("REDIS_PORT", port.to_string())
            .spawn()?;

        // Wait a few seconds for the connection to be ready
        let address = format!("redis://127.0.0.1:{port}");
        for _ in 0..5 {
            let client = Client::open(address.clone()).expect("To create redis client");
            match client.get_connection() {
                Ok(connection) => {
                    println!("Redis server connection ready at {address}");
                    return Ok(Self {
                        server_process,
                        connection,
                    });
                }
                Err(_) => {
                    println!("Server not ready yet at {address}, retrying...");
                    sleep(Duration::from_secs(1));
                }
            }
        }

        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to connect to RLTest server",
        ))
    }

    pub fn query<T: FromRedisValue>(
        &mut self,
        command: &str,
        args: impl ToRedisArgs,
    ) -> RedisResult<T> {
        cmd(command).arg(args).query(&mut self.connection)
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
