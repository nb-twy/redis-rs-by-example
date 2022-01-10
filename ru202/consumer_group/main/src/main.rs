use std::error;
use std::process::{Child, Command};
use std::thread;
use std::time::Duration;

use rand::prelude::*;
use redis::Commands;

use rs_util;

const KEY: &str = "numbers";
const GROUP: &str = "primes";
const MEMBERS: i16 = 10;

fn setup(config: &rs_util::Config) {
    // Initialize the Stream and the primes consumer group
    // To do: Add error handling so that failures don't just end in panics.
    // Connect to the Redis server
    let mut con = rs_util::get_connection(config).unwrap();
    // Make sure the stream does not already exist
    let _: () = con.del(KEY).unwrap();
    // Create the stream and the consumer group
    let _: () = con.xgroup_create_mkstream(KEY, GROUP, 0).unwrap();
}

fn producer(config: &rs_util::Config) {
    // Produce a stream of natural numbers

    // Make named connection
    let mut con = rs_util::get_connection(config).unwrap();
    let _: () = redis::cmd("CLIENT")
        .arg(&["SETNAME", "PRODUCER"])
        .query(&mut con)
        .unwrap();

    let mut n = 0;
    let mut rng = thread_rng();
    loop {
        // Write data to stream
        let _id: String = con
            .xadd(KEY, "*", &[("n".to_string(), n.to_string())])
            .unwrap();
        // Pause for a random amount of time
        n += 1;
        let sleep_time: u64 = (rng.gen_range(1000..=2000) as f64 / (MEMBERS as f64)).floor() as u64;
        thread::sleep(Duration::from_millis(sleep_time));
    }
}

struct Consumer {
    name: String,
    process_id: Child,
}

fn consumers() -> Vec<Consumer> {
    let mut consumers: Vec<Consumer> = vec![];
    for i in 1..=MEMBERS {
        let name = format!("BOB-{:02}", i);
        let process_id = Command::new("./consumer_group_consumer")
                        .args([KEY, GROUP, &name])
                        .spawn().unwrap();
        consumers.push(Consumer { name, process_id });
    }
    consumers
}

fn main() -> Result<(), Box<dyn error::Error>> {
    let app_name = String::from("ru202-consumer-group");
    let about = String::from(
        "
    Redis University 202 - Streams: Consumer Group Demo
            A multi-threaded application that demonstrates how multiple members of the
            same consumer group work together to process a stream. It also demonstrates
            how individual consumers can recover from complete failures without
            catastrophic effects.",
    );
    let config = rs_util::app_config(app_name, about);

    // Initialize the stream and group
    setup(&config);

    // Start the consumers in separate child processes
    consumers();
    producer(&config);

    Ok(())
}
