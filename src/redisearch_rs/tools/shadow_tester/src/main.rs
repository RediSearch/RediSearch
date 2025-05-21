use clap::Parser;
use redis_client::RedisClient;
use test_runner::TestRunner;

mod args;
mod redis_client;
mod test_runner;

fn main() -> std::io::Result<()> {
    let options = args::Options::parse();

    let base_client = RedisClient::new(6379, &options.rltest_command, options.baseline_so)?;
    let changeset_client = RedisClient::new(6380, &options.rltest_command, options.changeset_so)?;

    let mut test_runner = TestRunner::new(base_client, changeset_client);
    test_runner.add_command("PING");

    let success = test_runner.run()?;

    if success {
        println!("All responses are the same!");
    } else {
        println!("Some queries failed.");
    }

    Ok(())
}
