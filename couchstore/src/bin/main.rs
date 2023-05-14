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
    let key = std::env::args().nth(1).unwrap();
    let num_vbuckets = 1024;
    let vbucket = v_bucket_hash(&key, num_vbuckets);
    let mut db = Db::open(
            format!("/home/jrawsthorne/Code/github.com/couchbase/couchstore/data/travel-sample/{vbucket}.couch.1"),
            DBOpenOptions::default(),
        );
    let val = db.get(key).unwrap();
    let json = serde_json::from_slice::<Value>(val.as_slice()).unwrap();
    println!("{}", json);
}
