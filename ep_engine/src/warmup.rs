use crate::{
    ep_bucket::EPBucketPtr,
    failover_table::FailoverTable,
    item::Item,
    kv_store::Metadata,
    vbucket::{self, VBucket, VBucketPtr, VBucketState, Vbid},
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
    _config: Config,
    shard_vb_states: Vec<HashMap<Vbid, VBucketState>>,
    /// vector of vectors of VBucket IDs (one vector per shard). Each vector
    /// contains all vBucket IDs which are present for the given shard.
    shard_vb_ids: Vec<Vec<Vbid>>,
    warmed_up_vbuckets: DashMap<Vbid, VBucketPtr>,
}

impl Warmup {
    pub fn new(store: EPBucketPtr, config: Config) -> Self {
        let shard_vb_states: Vec<HashMap<Vbid, VBucketState>> =
            vec![HashMap::new(); store.vbucket_map.get_num_shards()];
        let shard_vb_ids = vec![Vec::new(); store.vbucket_map.get_num_shards()];
        let warmed_up_vbuckets = DashMap::with_capacity(config.max_vbuckets as usize);
        Self {
            store,
            _config: config,
            shard_vb_states,
            shard_vb_ids,
            warmed_up_vbuckets,
        }
    }

    pub fn warmup(&mut self) {
        self.initialise();
        for shard_id in 0..self.store.vbucket_map.get_num_shards() {
            self.create_vbuckets(shard_id);
        }
        // self.load_collection_counts();
        // self.estimate_item_count();
        // // load_prepared_sync_writes();
        for shard_id in 0..self.store.vbucket_map.get_num_shards() {
            self.populate_vbucket_map(shard_id);
        }
        for shard_id in 0..self.store.vbucket_map.get_num_shards() {
            self.key_dump(shard_id);
        }
        // // self.load_access_log();
        for shard_id in 0..self.store.vbucket_map.get_num_shards() {
            self.load_data(shard_id);
        }
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
        // TODO: Get from config
        let max_entries = 25;

        for (&vbid, state) in &self.shard_vb_states[shard_id] {
            let _vb = self.store.get_vbucket(vbid).unwrap_or_else(|| {
                let table = if state.failover_table.is_null() {
                    FailoverTable::new_empty(max_entries)
                } else {
                    FailoverTable::new(state.failover_table.clone(), max_entries, state.high_seqno)
                };
                let _shard = self.store.get_vbuckets().get_shard_by_vb_id(vbid);
                // TODO: get collection manifest
                let vb = VBucketPtr::new(VBucket::new(vbid, state.state, table));

                self.warmed_up_vbuckets.insert(vbid, vb.clone());

                vb
            });
        }
    }

    fn _load_collection_counts(&self) {
        todo!()
    }

    fn _estimate_item_count(&self) {
        todo!()
    }

    /// Adds all warmed up vbuckets (for the shard) to the bucket's VBMap,
    /// once added to the VBMap the rest of the system will be able to
    /// locate and operate on the VBucket, so this phase must only run once
    /// each vbucket is completely initialised.
    fn populate_vbucket_map(&self, shard_id: usize) {
        for &vbid in &self.shard_vb_ids[shard_id] {
            let vb = self.warmed_up_vbuckets.get(&vbid).unwrap().clone();
            // Take the vBucket lock to stop the flusher from racing with our
            // set vBucket state. It MUST go to disk in the first flush batch
            // or we run the risk of not rolling back replicas that we should
            let locked_vb = self.store.get_locked_vbucket(vbid);
            assert!(locked_vb.is_none());

            // TODO: self.checkpoint_manager.queue_set_vb_state();

            {
                // Note this lock is here for correctness - the VBucket is not
                // accessible yet, so its state cannot be changed by other code.
                let _state_lock = vb.get_state_lock();
                if vb.state() == vbucket::State::Active {
                    // TODO: Update collection map for vbucket
                }
            }

            self.store.flush_vbucket_unlocked(&locked_vb);

            self.store.vbucket_map.add_bucket(vb);
        }

        if shard_id == self.store.vbucket_map.shards.len() - 1 {
            // TODO: start flusher
            self.warmed_up_vbuckets.clear();
        }
    }

    fn key_dump(&self, shard_id: usize) {
        let store = self.store.get_store_by_shard(shard_id);
        let vbucket_map = &self.store.vbucket_map;
        let vbucket_filter = &self.shard_vb_ids[shard_id];
        for &vbid in vbucket_filter {
            let mut ctx = store.init_by_seqno_scan_context(vbid, 0);
            // TODO: Do this properly (in batches) like kv_engine
            ctx.db.changes_since(0, |_, doc_info| {
                let vb = vbucket_map.get_bucket(vbid).unwrap();
                let metadata = Metadata::decode(&doc_info.rev_meta[..]);
                let item = Item {
                    key: doc_info.id,
                    value: None,
                    cas: metadata.cas,
                    expiry_time: metadata.expiry_time,
                    flags: metadata.flags,
                    by_seqno: doc_info.db_seq,
                    rev_seqno: doc_info.rev_seq,
                };
                vb.insert_from_warmup(item);
            });
        }
    }

    fn load_data(&self, shard_id: usize) {
        let store = self.store.get_store_by_shard(shard_id);
        let vbucket_map = &self.store.vbucket_map;
        let vbucket_filter = &self.shard_vb_ids[shard_id];
        for &vbid in vbucket_filter {
            let mut ctx = store.init_by_seqno_scan_context(vbid, 0);
            // TODO: Do this properly (in batches) like kv_engine
            ctx.db.changes_since(0, move |db, doc_info| {
                let doc = if let Some(doc) = db.open_doc_with_docinfo(
                    &doc_info,
                    couchstore::OpenOptions::DECOMPRESS_DOC_BODIES,
                ) {
                    doc
                } else {
                    return;
                };

                let vb = vbucket_map.get_bucket(vbid).unwrap();
                let metadata = Metadata::decode(&doc_info.rev_meta[..]);
                let item = Item {
                    key: doc_info.id,
                    value: Some(doc.data),
                    cas: metadata.cas,
                    expiry_time: metadata.expiry_time,
                    flags: metadata.flags,
                    by_seqno: doc_info.db_seq,
                    rev_seqno: doc_info.rev_seq,
                };
                vb.insert_from_warmup(item);
            });
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{ep_bucket::EPBucket, vbucket};

    #[test]
    fn test_warmup() {
        let config = Config {
            max_vbuckets: 1024,
            max_shards: 1,
            dbname: "../test-data/travel-sample".to_string(),
        };
        let store = EPBucket::new(config.clone());
        let mut warmup = Warmup::new(store.clone(), config);
        warmup.warmup();
        assert_eq!(
            warmup.shard_vb_states[0]
                .get(&Vbid::from(0usize))
                .unwrap()
                .state,
            vbucket::State::Active
        );
        assert_eq!(warmup.store.vbucket_map.get_num_alive_vbuckets(), 1024);

        let val = store.get(Vec::from("landmark_25686")).unwrap();
        assert_eq!(val.cas, 1693175504558817280);
        assert!(val.value.is_some());
        assert!(val.is_resident());
    }
}
