use std::error;
use std::str;

use clap_v3::{App, Arg};
use redis::{ConnectionInfo, ConnectionLike, Value, FromRedisValue};

const CON_ICON: &str = "\u{21CC}";
const SEND_ICON: &str = "\u{27A5}";
const RECV_ICON: &str = "\u{1F814}";
const CLEAN_ICON: &str = "\u{1F9F9}";

fn show_description() {
    println!(
        "

======================
| Demo Low-level API |
======================
** Required **
Redis server accessible at an IP address or resolvable hostname

-- Demonstration --
1) If no Redis command is provided on the command line, 
   > execute 'set low:level:api:key 128'
   > execute 'get low:level:api:key'
   > execute 'del low:level:api:key'
2) If a Redis command is provided on the command line, execute it
   using the low-level API provided by redis-rs.


"
    );
}

fn main() -> Result<(), Box<dyn error::Error>> {
    show_description();

    // Parse command line arguments to acquire Redis server connection info
    // Example usage: cargo run -- --host 10.10.1.50 --port 7000 --db 0
    //       or
    //                ./rrbe-low-level-api --host 10.10.1.50 --port 7000 --db 0
    // All options have defaults, so they are optional.
    // Example with command: cargo run -- set mykey 128
    // If no command is given, only the hard-coded examples will be run.

    let matches = App::new("rrbe-low-level-api")
        .about("Demo usage of redis-rs' low-level api")
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
        .arg(
            Arg::with_name("DB")
                .help("DB number on the Redis server")
                .long("db")
                .short('d')
                .default_value("0"),
        )
        .arg(
            Arg::with_name("COMMAND")
                .help("Redis command to be executed")
                .multiple(true),
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
    let user_cmd: Vec<&str> = match matches.values_of("COMMAND") {
        Some(values) => values.collect(),
        //   ^-- If a command is provided, collect all the tokens into a vector
        //       of strings to be processed later
        None => vec![]
        // ^-- If no command was provided, return an empty vector.
    };

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

    // Open redis connection
    let client = redis::Client::open(con_info)?;
    //                          ^-- open() only validates the connection information and configures the connection.
    //                          |-- No sockets are actually opened at this point.

    // Display connection information
    println!("{icon} Connecting to: {}", client.get_connection_info().addr, icon = CON_ICON);
    let mut con = client.get_connection()?;
    //                   ^-- get_connection actually opens socket communication, whether over TCP or Unix sockets.
    println!("{icon} Connection open: {}", con.is_open(), icon = CON_ICON);

    // If no command was provided on the command line, execute a few basic commands using the low-level API.
    // This is useful in this example to demonstrate how the low-level command is constructed and then
    // executed.
    if user_cmd.is_empty() {
        let key_name = "low:level:api:key";
        let val = 128;
        println!("{icon} Setting {} => {}", key_name, val, icon = SEND_ICON);
        let resp: String = redis::cmd("SET").arg(key_name).arg(val).query(&mut con)?;
        //                 ^           ^     ^                      ^     ^-- Note that query() requires a mutable
        //                 |           |     |                      |     |-- borrow of the connection
        //                 |           |     |                      |-- Execute the command
        //                 |           |     |-- args can are set after a command has been identified
        //                 |           |     |-- args can be specified individually or as a vector of &str
        //                 |           |-- Identify the Redis command
        //                 |-- Use the low-level API.
        //                 |-- The high-level API is syntactic sugar in that it is compiled down to the same exact
        //                 |-- code used to execute the low-level API.
        println!("{icon} Response from server to setting key's val: {}", resp, icon = RECV_ICON);

        // Get key's value and display it
        let count: i32 = redis::cmd("GET").arg(key_name).query(&mut con)?;
        //         ^-- The library's FromRediValue trait is implemented for most types.
        //         |-- This makes it easy to specify the desired return type.

        // Display value of key
        println!("{icon} Value of {} read from server => {}", key_name, count, icon = RECV_ICON);

        // Let's clean up!
        println!("{icon} Cleaning up.", icon = CLEAN_ICON);
        let _ : () = redis::cmd("DEL").arg(key_name).query(&mut con)?;
        //  ^-- Ignore the server's response, which is int(1), telling us how many keys were deleted successfully.
        //  |-- If it fails, it will return a Result<Error>.

        // If a command was given on the command line, execute it.
    } else {
        let resp = match redis::Cmd::new().arg(&user_cmd).query(&mut con) {
        //                           ^     ^-- The low-level API allows us to construct a Redis command from
        //                           |     |-- a Vec<&str>. This allows us to reproduce the basic functionality
        //                           |     |-- of the standard redis-cli with little effort.
        //                           |-- Instead of using the syntatic sugar redis::cmd() method, we instantiate
        //                           |-- a new Cmd struct, providing the entire command.

            // If you're just getting started, these match conditions can be distracting.  It is not
            // important to understand what is going on here in any great detail to get started.
            // We need to match all the possible return types in order to reproduce the completely generic
            // behavior of redis-cli.
            // Most of the time, you will execute single commands and will know the expected return type.
            // As demonstrated above, when you know the expected return type, or at least the type that is most
            // convenient for you to get back, you can annotate the return type and allow the library's
            // FromRedisValue trait implementations take care of the hard work for you.
            Ok(Value::Int(val)) => val.to_string(),
            Ok(Value::Nil) => "Nil".to_string(),
            Ok(Value::Data(ref bytes)) => str::from_utf8(bytes)?.to_string(),
            Ok(Value::Okay) => "OK".to_string(),
            Ok(Value::Status(ref val)) => val.to_string(),
            Ok(Value::Bulk(ref items)) => {
                let vec: Vec<String> = FromRedisValue::from_redis_values(items)?;
                vec.join("")
            },
            Err(err) => err.to_string()
        };

        println!("{icon} Response from server: {}", resp, icon = RECV_ICON);
    }


    Ok(())
}
