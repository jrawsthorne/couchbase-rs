use couchstore::{DBOpenOptions, Db, OpenOptions};
use serde_json::Value;
use std::process::exit;

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

const NUM_VBUCKETS: u32 = 1024;
const PATH: &str = "./data/travel-sample";

fn main() {
    if std::env::args().len() < 3 {
        println!(
            "Usage: {} <get|set> <key>",
            std::env::args().nth(0).unwrap()
        );
        exit(1);
    }

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

    let vbucket = v_bucket_hash(&key, NUM_VBUCKETS);

    std::fs::create_dir_all(PATH).unwrap();

    let mut db = Db::open(
        format!("{PATH}/{vbucket}.couch.1"),
        DBOpenOptions::default(),
    );

    let key = key.as_bytes().to_vec();

    match action.as_str() {
        "get" => {
            let docinfo = db.docinfo_by_id(key).unwrap();
            let doc = db
                .open_doc_with_docinfo(&docinfo, OpenOptions::DECOMPRESS_DOC_BODIES)
                .unwrap();
            let json = serde_json::from_slice::<Value>(doc.data.as_slice()).unwrap();
            println!("{}", json);
        }
        "set" => {
            db.set(key, value);
            db.commit();
        }
        _ => panic!("Invalid action"),
    }
}
