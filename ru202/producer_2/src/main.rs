use std::error::Error;
/// Natural Numbers Stream Producer: 0 to 100
/// The stream is removed before each run.
/// Each run begins at 0 and ends at 100.

use chrono::prelude::*;
use redis::Commands;

fn main() -> Result<(), Box<dyn Error>> {
    let app_name = String::from("ru202-producer-2");
    let about = String::from(
        "
    Redis University 202 - Streams: producer_2
            A very simple app to write the natural numbers from 0 to 100
            to a stream named numbers.",
    );
    let config = rs_util::app_config(app_name, about);
    let mut con = rs_util::get_connection(&config)?;
    let stream_name = "numbers";

    // Make sure the stream does not exist before writing data to it
    let _: u8 = con.del(stream_name)
        .expect(&format!("[ERROR] Failure deleting stream {}", stream_name));
    
    for n in 0..=100 {
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
    }

    Ok(())
}
