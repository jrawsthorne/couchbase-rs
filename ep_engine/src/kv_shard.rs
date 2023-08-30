use crate::{
    kv_store::CouchKVStoreConfig,
    vbucket::{VBucketPtr, Vbid},
};
use parking_lot::{Mutex, MutexGuard};

pub struct KVShard {
    config: CouchKVStoreConfig,
    vbuckets: Vec<Mutex<Option<VBucketPtr>>>,
}

impl KVShard {
    pub fn new(config: CouchKVStoreConfig) -> Self {
        let num_vbuckets = (config.max_vbuckets as f64 / config.max_shards as f64).ceil() as usize;
        let mut vbuckets = Vec::with_capacity(num_vbuckets);
        vbuckets.resize_with(num_vbuckets, Default::default);
        KVShard { config, vbuckets }
    }

    pub fn get_bucket(&self, id: Vbid) -> Option<VBucketPtr> {
        if id.0 < self.config.max_vbuckets {
            let bucket = &*self.get_locked_bucket(id);
            if let Some(bucket) = bucket {
                assert_eq!(bucket.id, id);
            }
            bucket.clone()
        } else {
            None
        }
    }

    pub fn get_vbuckets(&self) -> Vec<Vbid> {
        let mut vbuckets = Vec::new();
        for bucket in &self.vbuckets {
            let bucket = &*bucket.lock();
            if let Some(bucket) = bucket {
                vbuckets.push(bucket.id);
            }
        }
        vbuckets
    }

    pub fn set_bucket(&self, vb: VBucketPtr) {
        self.get_locked_bucket(vb.id).replace(vb);
    }

    fn get_locked_bucket(&self, id: Vbid) -> MutexGuard<Option<VBucketPtr>> {
        assert_eq!(id.0 % self.config.max_shards, self.config.shard_id);
        let idx = (id.0 / self.config.max_shards) as usize;
        let bucket = &self.vbuckets[idx];
        bucket.lock()
    }
}
