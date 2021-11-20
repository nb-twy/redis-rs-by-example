use std::error;

use redis::{Commands, ConnectionInfo, ConnectionLike};
use clap_v3::{App, Arg};

const CON_ICON: &str = "\u{21CC}";
// const EXP_ICON: &str = "\u{1F9EA}";
const SEND_ICON: &str = "\u{27A5}";
const RECV_ICON: &str = "\u{1F814}";
const CLEAN_ICON: &str = "\u{1F9F9}";

fn show_description() {
    println!("

=====================================
| Use a Specific Address, Port & DB |
=====================================
** Required **
Redis server accessible at an IP address or resolvable hostname
on a port other than 6379.

-- Demonstration --
1) Get the hostname or host's IP address from the command line
2) Get the host's port from the command line
3) Get the protocol from the command line
4) Get the db number from the command line
5) Construct the connection URL
6) Connect to the redis server
7) Display the connection information
8) Write a value to a key
9) Read the value from that key
10) Display the key's value read from the server


");
}

fn main() -> Result<(), Box<dyn error::Error>> {
    show_description();

    // Parse command line arguments to acquire Redis server connection info
    // Usage: cargo run -- --host 10.10.1.50 --port 7000 --db 0
    //   or 
    //        ./rrbe-address-port --host 10.10.1.50 --port 7000 --db 0
    // All options have defaults, so they are optional.
    // The easiest way to test it is to run Redis in a container and change the exposed port.
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
        .get_matches();

    let host: String = matches.value_of("HOST").unwrap().to_string();
    //                                          ^-- Because of the default value for HOST, 
    //                                              unwrap will never have to deal with a 
    //                                              None variant.
    let port: u16 = matches.value_of("PORT").unwrap().parse().unwrap_or(6379);
    // While the default values mean that unwrap -----^
    // will never receive a None vairant, parsing
    // the command line arg to a number could easily
    // fail. We use the default if parsing fails, 
    // instead of providing a friendly error message.
    let db: i64 = matches.value_of("DB").unwrap().parse().unwrap_or(0);

    // While it is concise and easy for a human to read a connection URL,
    // e.g. redis://127.0.0.1:6379/0, redis-rs parses this URL into a
    // ConnectionInfo struct. Instead of trying to construct the URL correctly
    // from the command line arguments, only to have the URL parsed out again,
    // we can simply construct the ConnectionInfo struct from the data directly.
    let con_info = ConnectionInfo {
        addr: redis::ConnectionAddr::Tcp(host, port),
        redis: redis::RedisConnectionInfo {
            db,
            username: None,
            password: None,
        }
    };

    // ======== Everything that follows is identical to the getting-started example. ========
    // Open connection to local redis server on default port
    let client = redis::Client::open(con_info)?;

    // Display connection information
    println!("{icon} Connecting to: {}", client.get_connection_info().addr, icon = CON_ICON);
    let mut con = client.get_connection()?;
    //                   ^-- get_connection actually opens socket communication, whether over TCP or Unix sockets.
    println!("{icon} Connection open: {}", con.is_open(), icon = CON_ICON);

    // Set key's value
    let key_name = "addr:port:key";
    let val = 42;
    println!("{icon} Setting {} => {}", key_name, val, icon = SEND_ICON);
    let resp: String = con.set(key_name, val)?;
    //        ^-- Tell it what type you'd like back.
    //         -- The "FromRedisValue" trait is implemented for most types.

    println!("{icon} Response from server to setting key's val: {}", resp, icon = RECV_ICON);

    // Get key's value and display it
    let count: i32 = con.get(key_name)?;
    // Display value of key
    println!("{icon} Value of {} read from server => {}", key_name, count, icon = RECV_ICON);

    // We should cleanup, so let's remove the key we created
    println!("{icon} Cleaning up.", icon = CLEAN_ICON);
    let _ : () = con.del(key_name)?;
    //  ^-- Ignore the server's response, which is just "OK".
    //   -- If it fails, it will return a Result<Error>.
    Ok(())
}
