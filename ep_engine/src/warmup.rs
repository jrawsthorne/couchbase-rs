use crate::{
    ep_bucket::EPBucketPtr,
    vbucket::{self, VBucketPtr, VBucketState, Vbid},
    Config,
};
use dashmap::DashMap;
use rand::{
    distributions::{Bernoulli, Distribution},
    SeedableRng,
};
use std::collections::HashMap;

pub struct Warmup {
    store: EPBucketPtr,
    config: Config,
    shard_vb_states: Vec<HashMap<Vbid, VBucketState>>,
    /// vector of vectors of VBucket IDs (one vector per shard). Each vector
    /// contains all vBucket IDs which are present for the given shard.
    shard_vb_ids: Vec<Vec<Vbid>>,
    warmed_up_vbuckets: DashMap<Vbid, Option<VBucketPtr>>,
}

impl Warmup {
    pub fn new(store: EPBucketPtr, config: Config) -> Self {
        let shard_vb_states: Vec<HashMap<Vbid, VBucketState>> =
            vec![HashMap::new(); store.vbucket_map.get_num_shards()];
        let shard_vb_ids = vec![Vec::new(); store.vbucket_map.get_num_shards()];
        let warmed_up_vbuckets = DashMap::with_capacity(config.max_vbuckets as usize);
        Self {
            store,
            config,
            shard_vb_states,
            shard_vb_ids,
            warmed_up_vbuckets,
        }
    }

    pub fn warmup(&mut self) {
        self.initialise();
        // self.create_vbuckets();
        // self.load_collection_counts();
        // self.estimate_item_count();
        // // load_prepared_sync_writes();
        // self.populate_vbucket_map();
        // self.key_dump();
        // // self.load_access_log();
        // self.load_data();
    }

    pub fn initialise(&mut self) {
        // TODO: Warmup collection manifest
        self.populate_shard_vb_states();
    }

    fn get_num_kv_stores(&self) -> usize {
        self.store.vbucket_map.get_num_shards()
    }

    fn populate_shard_vb_states(&mut self) {
        let num_kvs = self.get_num_kv_stores();
        for shard_id in 0..num_kvs {
            let kv_store_vb_states = self
                .store
                .get_store_by_shard(shard_id)
                .list_persisted_vbuckets();
            for (i, &state) in kv_store_vb_states.iter().enumerate() {
                let state = if let Some(state) = state {
                    state
                } else {
                    continue;
                };
                let vb = (i * num_kvs) + shard_id;
                let shard_vb =
                    &mut self.shard_vb_states[vb % self.store.vbucket_map.get_num_shards()];
                shard_vb.insert(Vbid::from(vb), state.clone());
            }
        }

        for shard_id in 0..self.store.vbucket_map.shards.len() {
            let mut active_vbs = Vec::new();
            let mut other_vbs = Vec::new();

            for (&vb_id, vb_state) in &self.shard_vb_states[shard_id] {
                if vb_state.state == vbucket::State::Active {
                    active_vbs.push(vb_id);
                } else {
                    other_vbs.push(vb_id);
                }
            }

            // Push one active VB to the front.
            // When the ratio of RAM to VBucket is poor (big vbuckets) this will
            // ensure we at least bring active data in before replicas eat RAM.
            if let Some(active) = active_vbs.pop() {
                self.shard_vb_ids[shard_id].push(active);
            }

            // Now the VB lottery can begin.
            // Generate a psudeo random, weighted list of active/replica vbuckets.
            // The random seed is the shard ID so that re-running warmup
            // for the same shard and vbucket set always gives the same output and keeps
            // nodes of the cluster more equal after a warmup.
            let mut rng = rand::rngs::StdRng::seed_from_u64(shard_id as u64);
            // Give 'true' (aka active) 60% of the time
            // Give 'false' (aka other) 40% of the time.
            let distribute = Bernoulli::new(0.6).unwrap();

            while !active_vbs.is_empty() || !other_vbs.is_empty() {
                let active = distribute.sample(&mut rng);
                let source = if active {
                    &mut active_vbs
                } else {
                    &mut other_vbs
                };

                if let Some(vb) = source.pop() {
                    self.shard_vb_ids[shard_id].push(vb);
                } else {
                    // Once active or replica set is empty, just drain the other one.
                    let source = if active {
                        &mut other_vbs
                    } else {
                        &mut active_vbs
                    };
                    self.shard_vb_ids[shard_id].append(source);
                }
            }
        }
    }

    fn create_vbuckets(&self, shard_id: usize) {
        // let max_entries = self.store.get_max_failover_entries();

        for (&vbid, state) in &self.shard_vb_states[shard_id] {
            let vb = self.store.get_vbucket(vbid);

            if vb.is_none() {}
        }
    }

    fn load_collection_counts(&self) {
        todo!()
    }

    fn estimate_item_count(&self) {
        todo!()
    }

    fn populate_vbucket_map(&self) {
        todo!()
    }

    fn key_dump(&self) {
        todo!()
    }

    fn load_data(&self) {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{ep_bucket::EPBucket, vbucket};

    #[test]
    fn test_populate_shard_vb_states() {
        let config = Config {
            max_vbuckets: 1024,
            max_shards: 1,
            dbname: "../test-data/travel-sample".to_string(),
        };
        let store = EPBucket::new(config.clone());
        let mut warmup = Warmup::new(store, config);
        warmup.warmup();
        assert_eq!(
            warmup.shard_vb_states[0]
                .get(&Vbid::from(0usize))
                .unwrap()
                .state,
            vbucket::State::Active
        );
    }
}
