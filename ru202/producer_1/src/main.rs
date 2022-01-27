use std::error::Error;
/// The simplest Natural Numbers Stream producer
/// Every run starts the count of numbers from zero
use std::thread;
use std::time::Duration;

use chrono::prelude::*;
use redis::Commands;

fn main() -> Result<(), Box<dyn Error>> {
    let app_name = String::from("ru202-producer-1");
    let about = String::from(
        "
    Redis University 202 - Streams: producer_1
            A very simple app to write the list of natural numbers to a 
            stream named numbers.",
    );
    let config = rs_util::app_config(app_name, about);
    let mut con = rs_util::get_connection(&config)?;
    let stream_name = "numbers";
    let mut n = 1;

    // Make sure the stream does not exist before writing data to it
    let _: u8 = con.del(stream_name)
        .expect(&format!("[ERROR] Failure deleting stream {}", stream_name));
    
    loop {
        // Write data to stream
        let id: String = con
            .xadd(stream_name, "*", &[("n".to_string(), n.to_string())])
            .expect(&format!(
                "[ERROR] Failure writing number {} to stream: {}",
                n, stream_name
            ));
        let dt = Local::now();
        println!("{}: Produced the number {} as message ID {}",
            dt.format("%T"), n, id);
        
        n += 1;

        // Obtain educational Stream growth statistics
        let length: i64 = con
            .xlen(stream_name)
            .expect("[ERROR] Failure reading stream length!");
        let usage: i64 = redis::cmd("MEMORY")
            .arg("USAGE")
            .arg(stream_name)
            .query(&mut con)
            .expect("[ERROR Failure reading stream's memory usage!");
        let dt = Local::now();
        println!(
            "{}: Stream {} has {} messages and uses {} bytes.",
            dt.format("%T"),
            stream_name,
            length,
            usage
        );

        // Pause the processing for 1 second.  This could be randomized or removed, depending on need.
        thread::sleep(Duration::from_secs(1));
    }
}
