use std::sync::Arc;

use crate::{
    kv_store::CouchKVStore,
    vbucket::{VBucketPtr, Vbid},
    vbucket_map::VBucketMap,
    Config,
};

pub struct EPBucket {
    pub vbucket_map: VBucketMap,
}

impl EPBucket {
    pub fn new(config: Config) -> EPBucketPtr {
        EPBucketPtr::new(EPBucket {
            vbucket_map: VBucketMap::new(config.clone()),
        })
    }

    pub fn get_store_by_shard(&self, shard_id: usize) -> &CouchKVStore {
        self.vbucket_map.shards[shard_id].store()
    }

    pub fn get_vbucket(&self, vbid: Vbid) -> Option<VBucketPtr> {
        self.vbucket_map.get_bucket(vbid)
    }

    pub fn get_vbuckets(&self) -> &VBucketMap {
        &self.vbucket_map
    }
}

pub type EPBucketPtr = Arc<EPBucket>;
