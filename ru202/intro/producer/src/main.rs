use std::error;
use std::thread::sleep;
use std::time::Duration;

use redis::Commands;
use rand::prelude::*;

use rs_util;

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

    pub fn to_stream_data(&self) -> Vec<(String, String)> {
        vec![(String::from("postal_code"), self.postal_code.to_string()),
        (String::from("current_temp"), self.current_temp.to_string())]
    }
}

fn main() -> Result<(), Box<dyn error::Error>> {
    
    let app_name = String::from("ru202-intro-producer");
    let about = String::from("
    Redis University 202 - Streams: Intro Lab
            Producer
            Simulate distributed temperature sensors streaming data");
    let config = rs_util::app_config(app_name, about);
    let mut con = rs_util::get_connection(&config)?;

    // Set key's value
    let stream_key = "stream:weather";
    let mut measurement = Measurement::new();

    loop {
        let entry = measurement.get_next();
        let id: String = con.xadd(stream_key, "*", &entry.to_stream_data()[..])?;
        println!("Wrote {:?} with ID {}", entry, id);
        sleep(Duration::from_secs(1));
    }
}
