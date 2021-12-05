use std::error;
use std::process::exit;
use std::thread::sleep;
use std::time::Duration;
use std::collections::VecDeque;

use clap_v3::{App, Arg};
use redis::{ConnectionInfo, Commands, RedisResult, streams};
use hostname;

struct Window {
    size: usize,
    data: VecDeque<i32>,
    sum: i32,
    average: f32,
}

impl Window {
    pub fn new(size: usize) -> Window {
        Window { 
            size,
            sum: 0,
            data: VecDeque::with_capacity(size),
            average: 0.0,
        }
    }

    pub fn append(&mut self, entry: i32) {
        if self.data.len() == self.size {
            let out = self.data.pop_back().unwrap();
            self.sum -= out;
        }
        self.data.push_front(entry);
        self.sum += entry;
        self.average = self.sum as f32 / self.data.len() as f32;
    }

    pub fn get_average(&self) -> f32 {
        self.average
    }
}

fn show_processing(data: &streams::StreamId) {
    println!("Processing");
    println!("\tid: {}", data.id);
    println!("\tpostal_code: {}", data.get::<i32>("postal_code").unwrap());
    println!("\tcurrent_temp: {}", data.get::<i32>("current_temp").unwrap());
}

fn main() -> Result<(), Box<dyn error::Error>> {
    
    let matches = App::new("ru202-intro-consumer-average")
        .about("
        Redis University 202 - Streams: Intro Lab
            Consumer Average
            Simulate consuming the stream as a single member of a consumer group
            and calculating the rolling window average of the temperature.")
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
        .get_matches();

    let host: String = matches.value_of("HOST").unwrap().to_string();
    let port: u16 = matches.value_of("PORT").unwrap().parse().unwrap_or(6379);
    let con_info = ConnectionInfo {
        addr: redis::ConnectionAddr::Tcp(host, port),
        redis: redis::RedisConnectionInfo {
            db: 0,
            username: None,
            password: None,
        }
    };

    // Open a connection to the Redis server with the default info or what was provided on the command line
    let client = redis::Client::open(con_info)?;
    let mut con = client.get_connection()?;

    // Set up information for the consumer group
    let stream_key = "stream:weather";  // name of the stream to read from
    let group_name = "rolling_average_printer";   // name of the consumer group
    // name of this consumer
    // Note: If we are running the consumer app and this app from the same host, the consumer names will be
    //       identical.  This is okay because only the consumer name is unique within the consumer group.
    let consumer_name = format!("consumer-{:?}-a", hostname::get()?);
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

    // Calculate and display the rolling window average as each message is read from the stream
    let window_size = 10;
    let mut window = Window::new(window_size);

    loop {
        let results: RedisResult<streams::StreamReadReply> = con.xread_options(&[stream_key], &[stream_offsets], &stream_read_options);
        match results {
            Ok(data) => { 
                if !data.keys.is_empty() {
                    for id in &data.keys[0].ids {
                        // Show the user the data that is to be processed
                        show_processing(id);
                        // Show the rolling window average
                        window.append(id.get("current_temp").unwrap());
                        println!("Rolling Average: {}", window.get_average());
                    }
                }
            },
            Err(e) => println!("[Error] {:?}", e)
        }
        sleep(Duration::from_secs(1));
    }
}
