use parking_lot::{Mutex, MutexGuard};
use std::{ops::Deref, sync::Arc};

use crate::{
    kv_store::CouchKVStore,
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
