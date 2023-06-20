use couchstore::{DBOpenOptions, Db};
use serde_json::Value;

fn v_bucket_hash(key: &str, num_vbuckets: u32) -> u16 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(key.as_bytes());
    let crc = hasher.finalize();
    let hash = (crc >> 16) & 0x7fff & (num_vbuckets - 1);
    hash as u16
}

fn main() {
    let action = std::env::args().nth(1).unwrap();

    let key = "airline_10";
    let value = r#"{
        "callsign": "Aerocondor",
        "country": "Portugal",
        "iata": "2B",
        "icao": "ARD",
        "id": 10,
        "name": "Aerocondor",
        "type": "airline"
    }"#
    .as_bytes()
    .to_vec();
    let num_vbuckets = 1024;
    let vbucket = v_bucket_hash(&key, num_vbuckets);

    match action.as_str() {
        "get" => {
            let mut db = Db::open(
                format!("./data/travel-sample/{vbucket}.couch.1"),
                DBOpenOptions::default(),
            );
            let val = db.get(key).unwrap();
            let json = serde_json::from_slice::<Value>(val.as_slice()).unwrap();
            println!("{}", json);
        }
        "set" => {
            let path = format!("./data/travel-sample/{vbucket}.couch.1");
            {
                std::fs::remove_file(&path);
                let mut db = Db::open(&path, DBOpenOptions::default());
                db.set(key, value);
            }
            {
                let mut db = Db::open(&path, DBOpenOptions::default());
                let val = db.get(key).unwrap();
                let json = serde_json::from_slice::<Value>(val.as_slice()).unwrap();
                println!("{}", json);
            }
        }
        _ => panic!("Invalid action"),
    }
}
