use clap_v3::{App, Arg};
use redis::{Connection, ConnectionInfo, RedisResult};

#[derive(Clone, Debug)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub db: i64,
    pub username: Option<String>,
    pub password: Option<String>
}

pub fn app_config (name: String, about: String) -> Config {
    let matches = App::new(name)
    .about(&about[..])
    .version("0.1.0")
    .arg(
        Arg::with_name("HOST")
            .help("The resolvable hostname or IP address of the Redis server")
            .long("host")
            .short('h')
            .default_value("127.0.0.1")
    )
    .arg(
        Arg::with_name("PORT")
            .help("TCP port number of the Redis server")
            .long("port")
            .short('p')
            .default_value("6379")
    )
    .arg(
        Arg::with_name("DB")
        .help("DB number")
        .long("db")
        .short('d')
        .default_value("0")
    )
    .arg(
        Arg::with_name("USERNAME")
        .help("Username for the connection")
        .long("username")
        .short('u')
    )
    .arg(
        Arg::with_name("PASSWORD")
        .help("Password for connection")
        .long("password")
        .short('w')
    )
    .get_matches();

    Config {
        host: matches.value_of("HOST").unwrap().to_string(),
        port: matches.value_of("PORT").unwrap().parse().unwrap_or(6379),
        db: matches.value_of("DB").unwrap().parse().unwrap_or(0),
        username: match matches.value_of("USERNAME") {
            Some(val) => Some(val.to_string()),
            None => None
        },
        password: match matches.value_of("PASSWORD") {
            Some(val) => Some(val.to_string()),
            None => None
        }
    }
}

pub fn get_connection (config: &Config) -> RedisResult<Connection> {
    let con_info = ConnectionInfo {
        addr: redis::ConnectionAddr::Tcp(config.host.clone(), config.port),
        redis: redis::RedisConnectionInfo {
            db: config.db,
            username: config.username.clone(),
            password: config.password.clone(),
        }
    };

    // Open a connection to the Redis server with the default info or what was provided on the command line
    let client = redis::Client::open(con_info)?;
    client.get_connection()
}

const MAX_SEQ: i64 = 2^64 -1;

/// Increment a Stream message ID by one
pub fn incr_id(id: &str) -> String {
    let parts: Vec<i64> = id.split('-').map(|part| part.parse::<i64>()
        .expect("[ERROR] Could not parse stream entry id!")).collect();
    let mut time = parts[0];
    let mut seq = parts[1];

    if seq == MAX_SEQ {
        time +=1;
        seq = 0;
    } else {
        seq += 1;
    }

    format!("{}-{}", time, seq)
}

/// Decrement a Stream message ID by one
pub fn decr_id(id: &str) -> String {
    let parts: Vec<i64> = id.split('-').map(|part| part.parse::<i64>()
        .expect("[ERROR] Could not parse stream entry id!")).collect();
    let mut time = parts[0];
    let mut seq = parts[1];

    if seq == 0 {
        time -=1;
        seq = MAX_SEQ;
    } else {
        seq -= 1;
    }

    format!("{}-{}", time, seq)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_incr_id() {
        let id = "1643414204175-0";
        assert_eq!(incr_id(id), "1643414204175-1");
    }

    #[test]
    fn test_incr_id_max() {
        let id = format!("1643414204175-{}", 2^64 - 1);
        assert_eq!(incr_id(&id), "1643414204176-0")
    }

    #[test]
    fn test_decr_id() {
        let id = format!("1643414204175-23");
        assert_eq!(decr_id(&id), "1643414204175-22");
    }

    #[test]
    fn test_decr_id_zero() {
        let id = format!("1643414204175-0");
        assert_eq!(decr_id(&id), format!("1643414204174-{}", 2^64 - 1));
    }
}