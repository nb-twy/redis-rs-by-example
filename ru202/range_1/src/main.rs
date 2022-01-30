/// Sum the numbers in the Stream of natural numbers
/// using range queries

use std::error::Error;

use redis::Commands;
use redis::streams::StreamRangeReply;

fn main() -> Result<(), Box<dyn Error>> {
    // Initialize command line application
    let app_name = String::from("ru202-range-1");
    let about = String::from(
        "
    Redis University 202 - Streams: range_1
            Produce the sum of the natural numbers from the stream of
            natural numbers created by either producer_1 or producer_2
            using xrange queries.",
    );
    let config = rs_util::app_config(app_name, about);
    let mut con = rs_util::get_connection(&config)?;

    let stream_name = "numbers";
    let mut last_id = "0-1".to_string();    // The lowest valid full message ID in a Stream
    let end = "+";
    let count = 5;
    let mut n_sum = 0;

    // Read messages from the stream and produce the running sum forever
    // or until there are no more entries in the stream.
    loop {
        // Get the next batch of stream entries
        let entries: StreamRangeReply = con.xrange_count(stream_name, &last_id, end, count)
            .expect("[ERROR] Failure to read range of entries from stream!");
        
        // An empty response means we have exhausted the Stream
        if entries.ids.is_empty() {
            println!("[!] We have exhausted the stream. Good-bye!");
            break;
        }

        // Process each entry read from the stream, adding its value to the running sum
        for entry in entries.ids {
            last_id = entry.id.clone();
            n_sum += entry.get::<i64>("n").unwrap();
        }

        // Increment the last known ID for the next iteration
        last_id = rs_util::incr_id(&last_id);
        println!("The sum of the Natural Numbers Stream is {}.", n_sum);
    }

    Ok(())
}
