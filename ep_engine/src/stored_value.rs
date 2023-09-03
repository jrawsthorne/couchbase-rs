/// Value that is stored in the hash table
#[derive(Debug)]
pub struct StoredValue {
    /// The value itself, None if the item has been
    /// evicted from memory
    pub value: Option<Vec<u8>>,
    pub cas: u64,
    pub by_seqno: u64,
    pub expiry: u32,
    pub flags: u32,
    pub rev_seqno: u64,
}
