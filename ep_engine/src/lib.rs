pub mod ep_bucket;
pub mod failover_table;
pub mod hash_table;
pub mod kv_shard;
pub mod kv_store;
pub mod stored_value;
pub mod vbucket;
pub mod vbucket_map;
pub mod warmup;

#[derive(Debug, Clone)]
pub struct Config {
    pub max_vbuckets: u16,
    pub max_shards: u16,
    pub dbname: String,
}
