use couchstore::{DBOpenOptions, Db};
use serde_json::Value;

fn v_bucket_hash(key: &str, num_vbuckets: u32) -> u16 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(key.as_bytes());
    let crc = hasher.finalize();
    let hash = (crc >> 16) & 0x7fff & (num_vbuckets - 1);
    hash as u16
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Airline {
    callsign: String,
    country: String,
    iata: String,
    icao: String,
    id: u32,
    name: String,
    r#type: String,
    key: String,
}

fn main() {
    let action = std::env::args().nth(1).unwrap();
    let key = std::env::args().nth(2).unwrap();

    let value = Airline {
        callsign: "Aerocondor".to_string(),
        country: "Portugal".to_string(),
        iata: "2B".to_string(),
        icao: "ARD".to_string(),
        id: 10,
        name: "Aerocondor".to_string(),
        r#type: "airline".to_string(),
        key: key.clone(),
    };

    let value = serde_json::to_vec(&value).unwrap();

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
                let mut db = Db::open(&path, DBOpenOptions::default());
                db.set(key, value);
        
        }
        _ => panic!("Invalid action"),
    }
}
