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

    let stream_name: String = matches.value_of("STREAM").unwrap().to_string();
    let group_name: String = matches.value_of("GROUP").unwrap().to_string();
    let consumer_name: String = matches.value_of("CONSUMER").unwrap().to_string();

    println!("Stream name: {}", stream_name);
    println!("Group name: {}", group_name);
    println!("Consumer name: {}", consumer_name);

    // Open connection to local redis server
    let client = redis::Client::open(con_info)?;
    let mut con = client.get_connection()?;

    consumer(&mut con, &stream_name, &group_name, &consumer_name);
    Ok(())
}

fn consumer(con: &mut redis::Connection, stream_name: &str, group_name: &str, consumer_name: &str) {
    let mut rng = thread_rng();
    let mut timeout = 100;
    let mut retries = 0;
    let mut recovery = true;
    let mut from_id = "0".to_string();

    loop {
        let count = rng.gen_range(1..6);
        let opts = StreamReadOptions::default()
            .group(group_name, consumer_name)
            .count(count)
            .block(timeout);
        // Using the ID 0 asks for any pending messages from the stream
        let reply: StreamReadReply = con
            .xread_options(&[&stream_name], &[&from_id], &opts)
            .unwrap();
        
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
                from_id = ">".to_string();
                continue;
            }
        }

        // Process messages
        for stream in &reply.keys {
            for id in &stream.ids {
                let n: i32 = id.get("n").unwrap();
                if is_prime(&n.to_string()) {
                    println!("{}: {} {}", consumer_name.yellow(), n.to_string().green(), "is a prime number".green());
                } else {
                    println!("{}: {} is a not prime number", consumer_name.yellow(), n);
                }
                let _: RedisResult<()> = con.xack(&stream_name, &group_name, &[&id.id]);

                // Add artificial time delay to allow for the chaos function to stop a process
                // before it is able to complete processing entries.
                sleep(Duration::from_millis(thread_rng().gen_range(1000..=2000)));
            }
        }
    }
}
