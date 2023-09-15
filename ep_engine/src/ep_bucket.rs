use parking_lot::{Mutex, MutexGuard};
use std::{ops::Deref, sync::Arc};

use crate::{
    kv_store::CouchKVStore,
    stored_value::StoredValue,
    vbucket::{VBucketPtr, Vbid},
    vbucket_map::VBucketMap,
    Config,
};

pub struct EPBucket {
    pub vbucket_map: VBucketMap,
    vb_mutexes: Vec<Mutex<()>>,
}

impl EPBucket {
    pub fn new(config: Config) -> EPBucketPtr {
        let mut vb_mutexes = Vec::with_capacity(config.max_vbuckets as usize);
        vb_mutexes.resize_with(config.max_vbuckets as usize, Default::default);
        EPBucketPtr::new(EPBucket {
            vbucket_map: VBucketMap::new(config.clone()),
            vb_mutexes,
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

    /// Return a pointer to the given VBucket, acquiring the appropriate VB
    /// mutex lock at the same time.
    pub fn get_locked_vbucket(&self, vbid: Vbid) -> LockedVbucketPtr {
        let _guard = self.vb_mutexes[usize::from(vbid)].lock();
        let vb = self.vbucket_map.get_bucket(vbid);
        LockedVbucketPtr { vb, _guard }
    }

    pub fn flush_vbucket_unlocked(&self, _vb: &LockedVbucketPtr) {}

    pub fn get(&self, key: Vec<u8>) -> Option<StoredValue> {
        let vbid = v_bucket_hash(&key, 1024);
        // TODO: This is a hack to get around the fact that we don't have
        // collection support yet. We need to add support for collections
        let key_with_collection_id = {
            let mut key_with_collection_id = Vec::from("\0");
            key_with_collection_id.extend(key);
            key_with_collection_id
        };
        let vb = self.get_vbucket(Vbid::from(vbid)).unwrap();
        vb.get(&key_with_collection_id)
    }
}

pub type EPBucketPtr = Arc<EPBucket>;

pub struct LockedVbucketPtr<'a> {
    pub vb: Option<VBucketPtr>,
    _guard: MutexGuard<'a, ()>,
}

impl Deref for LockedVbucketPtr<'_> {
    type Target = Option<VBucketPtr>;

    fn deref(&self) -> &Self::Target {
        &self.vb
    }
}

pub fn v_bucket_hash(key: &[u8], num_vbuckets: u32) -> u16 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(key);
    let crc = hasher.finalize();
    let hash = (((crc) >> 16) & 0x7fff) & (num_vbuckets - 1);
    hash as u16
}
