use std::error;

use clap_v3::{App, Arg};
use redis::{ConnectionInfo, Commands};

const SEND_ICON: &str = "\u{27A5}";
const RECV_ICON: &str = "\u{1F814}";

fn main() -> Result<(), Box<dyn error::Error>> {
    
    let matches = App::new("ru202-intro-producer")
        .about("
        Redis University 202 - Streams: Intro Lab
            Producer
            Simulate distributed temperature sensors streaming data")
        .version("0.1.0")
        .arg(
            Arg::with_name("HOST")
                .help("The resolvable hostname or IP address of the Redis server")
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
        .get_matches();

    let host: String = matches.value_of("HOST").unwrap().to_string();
    let port: u16 = matches.value_of("PORT").unwrap().parse().unwrap_or(6379);
    let con_info = ConnectionInfo {
        addr: redis::ConnectionAddr::Tcp(host, port),
        redis: redis::RedisConnectionInfo {
            db: 0,
            username: None,
            password: None,
        }
    };

    // Open connection to local redis server on default port
    let client = redis::Client::open(con_info)?;
    let mut con = client.get_connection()?;

    // Set key's value
    let key_name = "getting:started:key";
    let val = 42;
    println!("{icon} Setting {} => {}", key_name, val, icon = SEND_ICON);
    let resp: String = con.set(key_name, val)?;

    println!("{icon} Response from server to setting key's val: {}", resp, icon = RECV_ICON);

    // Get key's value and display it
    let count: i32 = con.get(key_name)?;
    // Display value of key
    println!("{icon} Value of {} read from server => {}", key_name, count, icon = RECV_ICON);

    // We should cleanup, so let's remove the key we created

    Ok(())
}
