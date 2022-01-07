use std::error;
use std::thread::sleep;
use std::time::Duration;

use redis::{Commands, ConnectionInfo, RedisResult};
use redis::streams::{StreamReadOptions, StreamReadReply};
use clap_v3::{App, Arg};
use rand::prelude::*;

fn main() -> Result<(), Box<dyn error::Error>> {

    let matches = App::new("rrbe-address-port")
        .about("Demo redis-rs for non-default connection info")
        .version("0.1.0")
        .arg(
            Arg::with_name("HOST")
            .help("The resolvable host name or IP address of the Redis server")
            .long("host")
            .short('h')
            .default_value("127.0.0.1")
        )
        .arg(
            Arg::with_name("PORT")
            .help("TCP port number of the Redis server")
            .long("port")
            .short('p')
            .default_value("6379")
        )
        .arg(
            Arg::with_name("DB")
            .help("DB number on the Redis server")
            .long("db")
            .short('d')
            .default_value("0")
        )
        .arg(
            Arg::with_name("STREAM")
            .help("Stream name")
        )
        .arg(
            Arg::with_name("GROUP")
            .help("Consumer group name")
        )
        .arg(
            Arg::with_name("CONSUMER")
            .help("Consumer instance name")
        )
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
        }
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
    let mut from_id = ">".to_string();

    loop {
        let count = rng.gen_range(1..6);
        let opts = StreamReadOptions::default()
                    .group(group_name, consumer_name)
                    .count(count)
                    .block(timeout);
        let reply: StreamReadReply = con.xread_options(&[&stream_name], &[&from_id], &opts).unwrap();
        
        if !reply.keys.is_empty() {
            for stream in &reply.keys {
                for id in &stream.ids {
                    println!("\tid: {}, n: {}", 
                        id.id,
                        id.get::<i32>("n").unwrap());
                    let _: RedisResult<()> = con.xack(&stream_name, &group_name, &[&id.id]);
                }
            }
        }

    }
}
