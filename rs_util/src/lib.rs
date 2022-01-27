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