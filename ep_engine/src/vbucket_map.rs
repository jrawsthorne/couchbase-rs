use crate::{
    kv_shard::KVShard,
    vbucket::{State, VBucketPtr, Vbid},
    Config,
};
use std::sync::atomic::{AtomicU16, Ordering};

pub struct VBucketMap {
    shards: Vec<KVShard>,
    size: usize,
    vb_state_count: [AtomicU16; 4],
}

impl VBucketMap {
    pub fn new(config: Config) -> VBucketMap {
        // TODO: Get from workload policy
        let num_shards = config.max_shards;
        let mut shards = Vec::with_capacity(num_shards as usize);
        for shard_id in 0..config.max_shards {
            shards.push(KVShard::new(config.clone(), num_shards, shard_id));
        }

        VBucketMap {
            shards,
            size: config.max_vbuckets as usize,
            vb_state_count: [
                AtomicU16::new(0),
                AtomicU16::new(0),
                AtomicU16::new(0),
                AtomicU16::new(0),
            ],
        }
    }

    pub fn get_bucket(&self, id: Vbid) -> Option<VBucketPtr> {
        if usize::from(id) < self.size {
            self.get_shard_by_vb_id(id).get_bucket(id)
        } else {
            None
        }
    }

    fn get_shard_by_vb_id(&self, id: Vbid) -> &KVShard {
        let idx = usize::from(id) % self.shards.len();
        &self.shards[idx]
    }

    pub fn add_bucket(&self, vb: VBucketPtr) {
        let id = usize::from(vb.id);
        if id < self.size {
            let state = vb.state();
            self.get_shard_by_vb_id(vb.id).set_bucket(vb);
            println!("Mapped new {} in state {:?}", id, state);
            self.inc_vb_state_count(state);
        } else {
            panic!("Cannot create {}, max vbuckets is {}", id, self.size);
        }
    }

    fn inc_vb_state_count(&self, state: State) {
        self.vb_state_count[vb_state_to_index(state)].fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_vb_state_count(&self, state: State) {
        self.vb_state_count[vb_state_to_index(state)].fetch_sub(1, Ordering::Relaxed);
    }

    fn get_vb_state_count(&self, state: State) -> u16 {
        self.vb_state_count[vb_state_to_index(state)].load(Ordering::Relaxed)
    }

    pub fn get_buckets(&self) -> Vec<Vbid> {
        let mut vbuckets = Vec::new();
        for i in 0..self.size {
            if let Some(bucket) = self.get_bucket(Vbid::from(i)) {
                vbuckets.push(bucket.id);
            }
        }
        vbuckets
    }

    pub fn get_buckets_in_state(&self, state: State) -> Vec<Vbid> {
        let mut vbuckets = Vec::new();
        for i in 0..self.size {
            if let Some(bucket) = self.get_bucket(Vbid::from(i)) {
                if bucket.state() == state {
                    vbuckets.push(bucket.id);
                }
            }
        }
        vbuckets
    }

    pub fn get_shard(&self, shard_id: u16) -> &KVShard {
        &self.shards[shard_id as usize]
    }

    pub fn get_num_shards(&self) -> usize {
        self.shards.len()
    }

    pub fn get_num_alive_vbuckets(&self) -> usize {
        let mut res = 0;
        for state in [State::Active, State::Replica, State::Pending] {
            res += self.get_vb_state_count(state) as usize
        }
        res
    }
}

/// Convert a vbucket state to an index in the vb_state_count array
fn vb_state_to_index(state: State) -> usize {
    match state {
        State::Active => 0,
        State::Replica => 1,
        State::Pending => 2,
        State::Dead => 3,
    }
}
