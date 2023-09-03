use crate::{
    kv_store::{CouchKVStore, CouchKVStoreConfig},
    vbucket::{VBucketPtr, Vbid},
    Config,
};
use parking_lot::{Mutex, MutexGuard};

pub struct KVShard {
    config: CouchKVStoreConfig,
    vbuckets: Vec<Mutex<Option<VBucketPtr>>>,
    store: CouchKVStore,
}

impl KVShard {
    pub fn new(config: Config, num_shards: u16, shard_id: u16) -> Self {
        let kv_config = CouchKVStoreConfig {
            max_vbuckets: config.max_vbuckets,
            max_shards: num_shards,
            db_name: config.dbname.clone(),
            shard_id,
        };
        let num_vbuckets = (config.max_vbuckets as f64 / config.max_shards as f64).ceil() as usize;
        let mut vbuckets = Vec::with_capacity(num_vbuckets);
        vbuckets.resize_with(num_vbuckets, Default::default);
        let store = CouchKVStore::new(kv_config.clone());
        KVShard {
            config: kv_config,
            vbuckets,
            store,
        }
    }

    pub fn get_bucket(&self, id: Vbid) -> Option<VBucketPtr> {
        if u16::from(id) < self.config.max_vbuckets {
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
        assert_eq!(u16::from(id) % self.config.max_shards, self.config.shard_id);
        let idx = (u16::from(id) / self.config.max_shards) as usize;
        let bucket = &self.vbuckets[idx];
        bucket.lock()
    }

    pub fn store(&self) -> &CouchKVStore {
        &self.store
    }
}
