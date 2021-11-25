use std::error;
use std::thread::sleep;
use std::time::Duration;

use clap_v3::{App, Arg};
use redis::{ConnectionInfo, Commands};
use rand::prelude::*;

const POSTAL_CODES: [i32; 4] = [94016, 80014, 60659, 10011];
const MAX_TEMP: i32 = 100;
const MIN_TEMP: i32 = 0;

#[derive(Debug)]
struct Measurement {
    postal_code: i32,
    current_temp: i32,
}

impl Measurement {
    pub fn new() -> Measurement {
        Measurement { postal_code: POSTAL_CODES[0],
                      current_temp: 50}
    }

    pub fn get_next(&mut self) -> &Self {
        let mut rng = thread_rng();
        let rnd: f64 = rng.gen();
        if rnd >= 0.5 {
            if self.current_temp + 1 <= MAX_TEMP {
                self.current_temp += 1;
            } 
        } else {
            if self.current_temp - 1 >= MIN_TEMP {
                self.current_temp -= 1;
            }
        }
        
        self
    }
}

fn main() -> Result<(), Box<dyn error::Error>> {
    
    let matches = App::new("ru202-intro-producer")
        .about("
        Redis University 202 - Streams: Intro Lab
            Producer
            Simulate distributed temperature sensors streaming data")
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

    // Open connection to local redis server on default port
    let client = redis::Client::open(con_info)?;
    let mut con = client.get_connection()?;

    // Set key's value
    let stream_name = "stream:weather";
    let mut measurement = Measurement::new();

    loop {
        let entry = measurement.get_next();
        // To Do: Is there a better way to enumerate the struct's key:value pairs?
        let id: String = con.xadd(stream_name, "*", &[
            ("postal_code", entry.postal_code),
            ("current_temp", entry.current_temp)
        ])?;
        println!("Wrote {:?} with ID {}", entry, id);
        sleep(Duration::from_secs(1));
    }
}
