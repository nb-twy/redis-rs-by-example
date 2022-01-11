use std::error;
use std::process::{Child, Command};
use std::thread;
use std::time::Duration;
use std::io;
use std::sync::mpsc::{self, TryRecvError};

use rand::prelude::*;
use redis::Commands;
use colored::Colorize;

use rs_util;

const KEY: &str = "numbers";
const GROUP: &str = "primes";
const MEMBERS: i16 = 10;

fn setup(config: &rs_util::Config) {
    // Initialize the Stream and the primes consumer group
    // To do: Add error handling so that failures don't just end in panics.
    // Connect to the Redis server
    let mut con = rs_util::get_connection(config).unwrap();
    // Make sure the stream does not already exist
    let _: () = con.del(KEY).unwrap();
    // Create the stream and the consumer group
    let _: () = con.xgroup_create_mkstream(KEY, GROUP, 0).unwrap();
}

fn producer(config: rs_util::Config, rx: mpsc::Receiver<&str>) {
    // Produce a stream of natural numbers

    // Make named connection
    let mut con = rs_util::get_connection(&config).unwrap();
    let _: () = redis::cmd("CLIENT")
        .arg(&["SETNAME", "PRODUCER"])
        .query(&mut con)
        .unwrap();

    let mut n = 0;
    let mut rng = thread_rng();
    loop {
        // Check if the stop signal has been received
        match rx.try_recv() {
            Ok(val) => {
                if val == "STOP" {
                    println!("[>] Producer: Stop signal received: {}.", val);
                    break;
                }
            }
            Err(TryRecvError::Disconnected) => {
                println!("[>] Channel disconnected. Stopping producer thread.");
                break;
            }
            Err(TryRecvError::Empty) => {}
        }
        // Write data to stream
        let _id: String = con
            .xadd(KEY, "*", &[("n".to_string(), n.to_string())])
            .unwrap();
        // Pause for a random amount of time
        n += 1;
        let sleep_time: u64 = (rng.gen_range(1000..=2000) as f64 / (MEMBERS as f64)).floor() as u64;
        thread::sleep(Duration::from_millis(sleep_time));
    }
}

struct Consumer {
    name: String,
    process_id: Child,
}

fn consumers() -> Vec<Consumer> {
    let mut consumers: Vec<Consumer> = vec![];
    for i in 1..=MEMBERS {
        let name = format!("BOB-{:02}", i);
        consumers.push(new_consumer(name));
    }
    consumers
}

fn new_consumer(name: String) -> Consumer {
    let process_id = Command::new("./consumer_group_consumer")
                        .args([KEY, GROUP, &name])
                        .spawn().unwrap();
    Consumer { name, process_id }
}

fn chaos(mut consumers: Vec<Consumer>, rx: mpsc::Receiver<&str>) -> Vec<Consumer> {
    loop {
        // Check if the stop signal has been received
        match rx.try_recv() {
            Ok(val) => {
                if val == "STOP" {
                    println!("[>] Chaos: Stop signal received: {}.", val);
                    break;
                }
            }
            Err(TryRecvError::Disconnected) => {
                println!("[>] Channel disconnected. Stopping chaos thread.");
                break;
            }
            Err(TryRecvError::Empty) => {}
        }
        let mut rng = thread_rng();
        if rng.gen_range(2..=12) == 2 {
            let victim = rng.gen_range(0..MEMBERS) as usize;
            let name = format!("BOB-{:02}", victim + 1);
            consumers[victim].process_id.kill().expect("Failed to stop process");
            consumers[victim] = new_consumer(name);
            println!("{} {}", "CHAOS: Restarted".magenta(), consumers[victim].name.magenta());
        }
        thread::sleep(Duration::from_millis(rng.gen_range(1000..=2000)));
    }

    consumers
}

fn main() -> Result<(), Box<dyn error::Error>> {
    let app_name = String::from("ru202-consumer-group");
    let about = String::from(
        "
    Redis University 202 - Streams: Consumer Group Demo
            A multi-threaded application that demonstrates how multiple members of the
            same consumer group work together to process a stream. It also demonstrates
            how individual consumers can recover from complete failures without
            catastrophic effects.",
    );
    let config = rs_util::app_config(app_name, about);

    println!("Press ENTER to run the application now.");
    println!("Press ENTER again later to exit cleanly...");

    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();

    // Initialize the stream and group
    setup(&config);

    // Start the consumers in separate child processes
    let consumers = consumers();

    // Start the chaos function in a separate thread
    let (chaos_tx, chaos_rx) = mpsc::channel::<&str>();
    let chaos_handle = thread::spawn(move || chaos(consumers, chaos_rx));

    // Start the producer in its own thread
    let (prod_tx, prod_rx) = mpsc::channel::<&str>();
    let config_prod = config.clone();
    thread::spawn(move || producer(config_prod, prod_rx));

    // Wait for user input on the main thread to trigger cleanup
    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();

    // Clean up
    println!("\n\nCleaning up and exiting...");
    // 1. Stop the producer thread
    println!("[>] Stopping producer thread...");
    prod_tx.send("STOP").expect("[ERROR] Failed to stop the producer thread!");

    // 2. Stop the chaos thread
    println!("[>] Stopping the chaos thread...");
    chaos_tx.send("STOP").expect("[ERROR] Failed to stop the chaos thread!");
    let consumers = chaos_handle.join().unwrap();

    // 3. Stop the consumers
    println!("[>] Stopping consumer processes...");
    for mut consumer in consumers {
        consumer.process_id.kill().expect(&format!("[ERROR] Failed to stop {}", consumer.name));
    }

    // 4. Delete the stream key from Redis
    let mut con = rs_util::get_connection(&config).unwrap();
    let _: i32 = con.del(KEY).expect("[ERROR] Failed to delete the stream key!");

    println!("\n\nGood-bye!");

    Ok(())
}
