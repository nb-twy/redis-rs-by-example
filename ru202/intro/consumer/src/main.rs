use std::error;
use std::process::exit;
use std::thread::sleep;
use std::time::Duration;

use redis::{Commands, RedisResult, streams};
use hostname;

use rs_util;

fn write_to_data_warehouse(data: &streams::StreamReadReply) {
    if !data.keys.is_empty() {
        for stream in &data.keys {
            println!("Stream: {}", stream.key);
            for id in &stream.ids {
                println!("\tid: {} data: [postal_code: {}, current_temp: {}]", 
                    id.id,
                    id.get::<i32>("postal_code").unwrap(),
                    id.get::<i32>("current_temp").unwrap());
                println!("\tWritten to data warehouse.");
            }
        }
    }
}

fn main() -> Result<(), Box<dyn error::Error>> {
    let app_name = String::from("ru202-intro-consumer");
    let about = String::from("
    Redis University 202 - Streams: Intro Lab
        Consumer
        Simulate consuming the stream as a single member of a consumer group
        and writing the data to a data warehouse.");

    let config = rs_util::app_config(app_name, about);
    let mut con = rs_util::get_connection(&config)?;

    // Set up information for the consumer group
    let stream_key = "stream:weather";  // name of the stream to read from
    let group_name = "data_warehouse_writer";   // name of the consumer group
    let consumer_name = format!("consumer-{:?}-a", hostname::get()?);   // name of this consumer
    let block_ms = 5000;    // the amount of time this consumer will block while waiting for data from the stream
                            // before releasing the connection
    let stream_offsets = ">";   // the consumer will read only entries in the stream that were never delivered to
                                // any other consumer in its group
    let stream_read_options = streams::StreamReadOptions::default()
                                .block(block_ms)
                                .group(group_name, consumer_name);

    // Make sure that the stream exists, if not exit with an error code, instead of 0.
    if !con.exists(&stream_key)? {
        println!("Stream {} does not exist.  Try running the producer first.", stream_key);
        exit(1)
    }

    // Attempt to create the group.  If the group already exists, tell the user.
    let result: RedisResult<String> = con.xgroup_create(stream_key, group_name, 0);
    match result {
        Ok(_) => (),
        Err(_) => println!("Group {} already exists.", group_name)
    }

    loop {
        let results: RedisResult<streams::StreamReadReply> = 
                con.xread_options(&[stream_key], &[stream_offsets], &stream_read_options);
        match results {
            Ok(data) => write_to_data_warehouse(&data),
            Err(e) => println!("[Error] {:?}", e)
        }
        sleep(Duration::from_secs(1));
    }
}
