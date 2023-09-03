use std::collections::HashMap;

use crate::stored_value::StoredValue;

#[derive(Debug)]
pub struct HashTable {
    pub map: HashMap<Vec<u8>, StoredValue>,
}
