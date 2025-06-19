use clap::Parser;
use datasets::get_1984_index;
use redis::cmd;
use redis_client::RedisClient;
use test_runner::TestRunner;

mod args;
mod datasets;
mod redis_client;
mod test_runner;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = args::Options::parse();

    let base_client = RedisClient::new(6379, &options.rltest, options.baseline_so)?;
    let changeset_client = RedisClient::new(6380, &options.rltest, options.changeset_so)?;

    let queries = get_1984_index()?;

    let mut test_runner = TestRunner::new(queries, base_client, changeset_client);
    test_runner.add_command(
        cmd("FT.SEARCH")
            .arg("idx:1984")
            .arg("thoughtcrime")
            .to_owned(),
    );

    let success = test_runner.run()?;

    if success {
        println!("All responses are the same!");
    } else {
        println!("Some queries failed.");
    }

    Ok(())
}
