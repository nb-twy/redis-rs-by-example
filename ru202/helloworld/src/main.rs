use std::error::Error;

use redis::Commands;

fn main() -> Result<(), Box<dyn Error>> {
    let app_name = String::from("ru202-hello-world");
    let about = String::from(
        "
    Redis University 202 - Streams: Helloworld
            A very simple app that assumes there is a key named hello already
            in the redis DB.  It gets this value and displays its value on the screen.",
    );
    let config = rs_util::app_config(app_name, about);
    let mut con = rs_util::get_connection(&config)?;
    let resp: String = con.get("hello")?;
    println!("{}", resp);
    Ok(())
}