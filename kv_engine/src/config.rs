use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub logger: Logger,
    pub interfaces: Vec<Interface>,
}

#[derive(Debug, Deserialize)]
pub struct Logger {
    pub filename: String,
}

#[derive(Debug, Deserialize)]
pub struct Interface {
    pub port: u16,
    pub host: String,
}
