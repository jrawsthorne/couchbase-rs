use std::collections::HashMap;

use crate::{item::Item, stored_value::StoredValue};

#[derive(Debug, Default)]
pub struct HashTable {
    pub map: HashMap<Vec<u8>, StoredValue>,
}

impl HashTable {
    pub fn insert_from_warmup(&mut self, item: Item) {
        if let Some(v) = self.map.get_mut(&item.key) {
            assert!(v.cas == item.cas);
            assert!(!v.is_resident());

            v.restore_value(item);

            return;
        }

        let value = self.add_new_stored_value(item);

        value.mark_not_resident();
    }

    fn add_new_stored_value(&mut self, item: Item) -> &mut StoredValue {
        let value = StoredValue {
            value: None,
            cas: item.cas,
            by_seqno: item.by_seqno,
            expiry_time: item.expiry_time,
            flags: item.flags,
            rev_seqno: item.rev_seqno,
            bits: Default::default(),
            data_type: item.data_type,
        };
        self.map.entry(item.key).or_insert(value)
    }
}
