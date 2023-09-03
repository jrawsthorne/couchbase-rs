pub mod hash_table;
pub mod kv_shard;
pub mod kv_store;
pub mod stored_value;
pub mod vbucket;
pub mod vbucket_map;

#[derive(Debug, Clone)]
pub struct Config {
    pub max_vbuckets: u16,
    pub max_shards: u16,
    pub dbname: String,
}
