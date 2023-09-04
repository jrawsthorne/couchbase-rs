use std::{
    collections::VecDeque,
    sync::atomic::{AtomicU64, Ordering},
};

use parking_lot::Mutex;

#[derive(Debug)]
pub struct FailoverTable {
    max_entries: usize,
    latest_uuid: AtomicU64,
    state: Mutex<State>,
}

#[derive(Default, Debug)]
struct State {
    table: VecDeque<FailoverEntry>,
}

impl FailoverTable {
    pub fn new(json: serde_json::Value, max_entries: usize, high_seqno: i64) -> FailoverTable {
        let table: VecDeque<FailoverEntry> = serde_json::from_value(json).unwrap();
        assert!(!table.is_empty());
        let latest_uuid = AtomicU64::new(table.front().unwrap().vb_uuid);
        let table = Self {
            state: Mutex::new(State { table }),
            max_entries,
            latest_uuid,
        };
        table.sanitise(high_seqno);
        table
    }

    pub fn new_empty(max_entries: usize) -> Self {
        let table = Self {
            max_entries,
            state: Default::default(),
            latest_uuid: AtomicU64::new(0),
        };
        table.create_entry(0);
        table
    }

    fn create_entry(&self, high_seqno: u64) {
        let table = &mut self.state.lock().table;

        // Our failover table represents only *our* branch of history.
        // We must remove branches we've diverged from.
        // Entries that we remove here are not erroneous entries because a
        // diverged branch due to node(s) failure(s).
        table.retain(|entry: &FailoverEntry| entry.by_seqno <= high_seqno);

        let entry = FailoverEntry {
            vb_uuid: rand::random(),
            by_seqno: high_seqno,
        };

        table.push_front(entry);

        self.latest_uuid.store(entry.vb_uuid, Ordering::SeqCst);

        // Cap the size of the table
        while table.len() > self.max_entries {
            table.pop_back();
        }
    }

    fn sanitise(&self, _high_seqno: i64) {}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct FailoverEntry {
    #[serde(rename = "id")]
    pub vb_uuid: u64,
    #[serde(rename = "seq")]
    pub by_seqno: u64,
}
