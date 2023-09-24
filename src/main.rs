use serde_derive::{Deserialize, Serialize};
use serde_json::Result;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::thread::sleep;
use std::time::Duration;

use std::str;

use rusqlite::Connection;

#[derive(Debug, Serialize, Deserialize)]
struct Config {
    input: String,
    output: String,
    interval: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct LogRow {
    timestamp: String,
    module: String,
    level: String,
    message: String,
}

fn parse_data(buffer: String) -> LogRow {
    let tokens = buffer.split(" ").collect::<Vec<&str>>();

    let row = LogRow {
        timestamp: String::from(tokens[0]),
        module: String::from(tokens[1]),
        level: String::from(tokens[2]),
        message: String::from(tokens[3]),
    };

    row
}

fn save_data(row: LogRow) {
    let conn = Connection::open("warehouse/logs.db").unwrap();

    conn.execute(
        "INSERT OR IGNORE INTO LOGS
            (timestamp, module, level, message)
            values (?1, ?2, ?3, ?4)
            ",
        (&row.timestamp, &row.module, &row.level, &row.message),
    )
    .unwrap();

    conn.close().unwrap();
}

fn initialize_db() {
    let conn = Connection::open("warehouse/logs.db").unwrap();

    conn.execute(
        "CREATE TABLE IF NOT EXISTS logs (
            timestamp datetime,
            module varchar(30),
            level varchar(10),
            message text,
            PRIMARY KEY(timestamp, module)
        )
        ",
        (),
    )
    .unwrap();

    conn.close().unwrap();
}

fn main() {
    // Reads the config from config/collector.json
    let file = File::open("test_log.log").unwrap();
    let mut reader = BufReader::new(file);

    loop {
        println!("Checking for more logs");
        let buffer = reader.fill_buf().unwrap();

        if buffer.len() > 0 {
            println!("Found more longs, dumping...");
            let lines = std::str::from_utf8(buffer).unwrap();

            for line in lines.split('\n').collect::<Vec<_>>() {
                println!("LINE: {}", line);
                let row = parse_data(line.to_string());
                save_data(row);
            }
        } else {
            println!("No new logs found...")
        }

        // Tells the reader not to return any more read bytes
        let length = buffer.len();
        reader.consume(length);
        sleep(Duration::from_secs(2));
    }
}
