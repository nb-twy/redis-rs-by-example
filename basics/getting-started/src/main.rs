use redis::{Commands, ConnectionLike};
use std::error;

const CON_ICON: &str = "\u{21CC}";
// const EXP_ICON: &str = "\u{1F9EA}";
const SEND_ICON: &str = "\u{27A5}";
const RECV_ICON: &str = "\u{1F814}";
const CLEAN_ICON: &str = "\u{1F9F9}";

fn show_description() {
    println!("

===================
| Getting Started |
===================
** Required **
Redis server accessible at 127.0.0.1 on the default port, 6379.

-- Demonstration --
1) Connect to the redis server
2) Display the connection information
3) Write a value to a key
4) Show the server's response to setting the key's value
5) Read the value from that key
6) Display the key's value read from the server


");
}

fn main() -> Result<(), Box<dyn error::Error>> {
    show_description();

    // Open connection to local redis server on default port
    let client = redis::Client::open("redis://127.0.0.1:6379")?;
    //                          ^     ^       ^         ^-- Port number
    //                          |     |       |-- IP Address of redis server
    //                          |     |-- Protocol to use -- One of: redis, rediss, unix, redis+unix
    //                          |-- "open" only validates the connection information and configured the connection.
    //                          |-- No sockets are actually opened at this point.

    // Display connection information
    println!("{icon} Connecting to: {}", client.get_connection_info().addr, icon = CON_ICON);
    let mut con = client.get_connection()?;
    //                   ^-- get_connection actually opens socket communication, whether over TCP or Unix sockets.
    println!("{icon} Connection open: {}", con.is_open(), icon = CON_ICON);

    // Set key's value
    let key_name = "getting:started:key";
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
