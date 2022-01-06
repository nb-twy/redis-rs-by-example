use std::error;

use redis::{Commands, ConnectionInfo, ConnectionLike};
use clap_v3::{App, Arg};

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

    Ok(())
}
