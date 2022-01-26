use std::error;
use std::thread::sleep;
use std::time::Duration;

use clap_v3::{App, Arg};
use rand::prelude::*;
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{Commands, ConnectionInfo, RedisResult};
use is_prime::*;
use colored::Colorize;

fn main() -> Result<(), Box<dyn error::Error>> {
    let matches = App::new("rrbe-address-port")
        .about("Demo redis-rs for non-default connection info")
        .version("0.1.0")
        .arg(
            Arg::with_name("HOST")
                .help("The resolvable host name or IP address of the Redis server")
                .long("host")
                .short('h')
                .default_value("127.0.0.1"),
        )
        .arg(
            Arg::with_name("PORT")
                .help("TCP port number of the Redis server")
                .long("port")
                .short('p')
                .default_value("6379"),
        )
        .arg(
            Arg::with_name("DB")
                .help("DB number on the Redis server")
                .long("db")
                .short('d')
                .default_value("0"),
        )
        .arg(Arg::with_name("STREAM").help("Stream name"))
        .arg(Arg::with_name("GROUP").help("Consumer group name"))
        .arg(Arg::with_name("CONSUMER").help("Consumer instance name"))
        .get_matches();

    let host: String = matches.value_of("HOST").unwrap().to_string();
    let port: u16 = matches.value_of("PORT").unwrap().parse().unwrap_or(6379);
    let db: i64 = matches.value_of("DB").unwrap().parse().unwrap_or(0);
    let con_info = ConnectionInfo {
        addr: redis::ConnectionAddr::Tcp(host, port),
        redis: redis::RedisConnectionInfo {
            db,
            username: None,
            password: None,
        },
    };

    let stream_name: String = matches.value_of("STREAM")
        .expect("[ERROR] Stream name missing!")
        .to_string();
    let group_name: String = matches.value_of("GROUP")
        .expect("[ERROR] Group name missing!")
        .to_string();
    let consumer_name: String = matches.value_of("CONSUMER")
        .expect("[ERROR] Consumer name missing!")
        .to_string();

    println!("Stream name: {}", stream_name);
    println!("Group name: {}", group_name);
    println!("Consumer name: {}", consumer_name);

    // Open connection to redis server
    let client = redis::Client::open(con_info)?;
    let mut con = client.get_connection()?;

    // Create the consumer
    consumer(&mut con, &stream_name, &group_name, &consumer_name);
    Ok(())
}

/// Start a consumer for the given stream and group
/// The consumer begins by determining if there are any pending items and processes them first.
/// Once any pending items are processed, the consumer begins processing any new messages.
/// If there are new new items on the stream for 100ms, the consumer releases its connection
/// and tries again four more times, doubling the timeout time each time.  If no new data
/// is available on the stream after 3.1 seconds, the consumer stops itself entirely.
/// Message processing consists of determining if the whole number read from the stream is a
/// prime number or not, printing the result to the screen, and acknowledging the item to redis.
fn consumer(con: &mut redis::Connection, stream_name: &str, group_name: &str, consumer_name: &str) {
    let mut rng = thread_rng();
    let mut timeout = 100;
    let mut retries = 0;
    let mut recovery = true;
    let mut from_id = "0".to_string();

    loop {
        // Each time a consumer reads from the stream, it may read a random number of entries
        // between 1 and 6.
        let count = rng.gen_range(1..6);
        let opts = StreamReadOptions::default()
            .group(group_name, consumer_name)
            .count(count)
            .block(timeout);
        // Using the ID 0 asks for any pending messages from the stream
        let reply: StreamReadReply = con
            .xread_options(&[&stream_name], &[&from_id], &opts)
            //             ^----------------^-Remember that xreadgroup allows us to read from multiple
            //             streams simultaneously.  That's why the stream_name and from_id properties
            //             are slices.
            .expect(&format!("[ERROR] {} - Failure reading from stream!", consumer_name));
        
        // Handle timeouts - when stream entries are not available to be read
        if reply.keys.is_empty() {
            if retries == 5 {
                println!("{}: Waited long enough - bye bye...", consumer_name);
                break;
            }
            retries += 1;
            timeout *= 2;
            continue;
        }

        // If we have recovered from a timeout situation, reset the timeout thresholds
        timeout = 100;
        retries = 0;

        if recovery {
            // If the response is empty, then there are no pending messages.
            if !reply.keys[0].ids.is_empty() {
                println!("{}: {}", consumer_name.yellow(), "Recovering pending messages...".cyan());
            } else {
                // If there are no messages to recover, switch to fetching new messages.
                println!("{}: {}", consumer_name.yellow(), "Processing new messages...".cyan());
                recovery = false;
                // Setting from_id to > tells redis to deliver the next undelivered item(s)
                from_id = ">".to_string();
                continue;
            }
        }

        // Process messages
        for stream in &reply.keys {
            for id in &stream.ids {
                let n: i32 = id.get("n").expect("[ERROR] Failure extracting data from stream item!");
                //                   ^-We know that "n" is the name of the field in the stream item.
                if is_prime(&n.to_string()) {
                    println!("{}: {} {}", consumer_name.yellow(), n.to_string().green(), "is a prime number".green());
                } else {
                    println!("{}: {} is a not prime number", consumer_name.yellow(), n);
                }
                let _: RedisResult<()> = con.xack(&stream_name, &group_name, &[&id.id]);
                //  ^-We are throwing away the response received from acknowledging the item.
                //    The return value is the number of messages successfully acknowledged.
                //    We could process all messages received before acknowleding any, but that
                //    seems like it would add unnecessary complexity in this case.
                //    We could also check to make sure this value is equal to 1, indicating that
                //    that the one item we wished to acknowledge succeeded.

                // Add artificial time delay to allow for the chaos function to stop a process
                // before it is able to complete processing entries.
                sleep(Duration::from_millis(thread_rng().gen_range(1000..=2000)));
            }
        }
    }
}
