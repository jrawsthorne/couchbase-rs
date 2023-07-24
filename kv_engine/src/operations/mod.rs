pub mod cluster_config;
pub mod dcp;
pub mod get;
pub mod hello;
pub mod sasl_auth;
pub mod select_bucket;
pub mod set;

pub fn v_bucket_hash(key: &[u8], num_vbuckets: u32) -> u16 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(key);
    let crc = hasher.finalize();
    let hash = (((crc) >> 16) & 0x7fff) & (num_vbuckets - 1);
    hash as u16
}
