use clap_v3::{App, Arg};
use redis::{Connection, ConnectionInfo, RedisResult};

#[derive(Clone, Debug)]
pub struct Config {
    pub host: String,
    pub port: u16
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

    Config {
        host: matches.value_of("HOST").unwrap().to_string(),
        port: matches.value_of("PORT").unwrap().parse().unwrap_or(6379)
    }
}

pub fn get_connection (config: &Config) -> RedisResult<Connection> {
    let con_info = ConnectionInfo {
        addr: redis::ConnectionAddr::Tcp(config.host.clone(), config.port),
        redis: redis::RedisConnectionInfo {
            db: 0,
            username: None,
            password: None,
        }
    };

    // Open a connection to the Redis server with the default info or what was provided on the command line
    let client = redis::Client::open(con_info)?;
    client.get_connection()
}