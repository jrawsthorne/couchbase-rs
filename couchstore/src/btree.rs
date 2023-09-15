use std::cmp::Ordering;

pub struct CouchfileLookupRequest {
    pub keys: Vec<Vec<u8>>,
    pub fold: bool,
    pub(crate) in_fold: bool,
    pub compare: fn(&[u8], &[u8]) -> Ordering,
}

impl CouchfileLookupRequest {
    pub fn new(keys: Vec<Vec<u8>>) -> Self {
        Self {
            keys,
            fold: false,
            in_fold: false,
            compare: |a, b| a.cmp(b),
        }
    }

    pub fn with_compare(mut self, compare: fn(&[u8], &[u8]) -> Ordering) -> CouchfileLookupRequest {
        self.compare = compare;
        self
    }

    pub fn fold(mut self) -> CouchfileLookupRequest {
        self.fold = true;
        self
    }
}
