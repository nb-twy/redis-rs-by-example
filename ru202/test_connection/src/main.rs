use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let app_name = String::from("ru202-test-connection");
    let about = String::from(
        "
    Redis University 202 - Streams: test_connection
            A very simple app that executes redis' ping and prints the response.",
    );
    let config = rs_util::app_config(app_name, about);
    let mut con = rs_util::get_connection(&config)?;
    let resp: String = redis::cmd("PING").query(&mut con).unwrap();
    println!("{}", resp);
    Ok(())
}