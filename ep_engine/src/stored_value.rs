use bitflags::bitflags;

/// Value that is stored in the hash table
#[derive(Debug, Clone)]
pub struct StoredValue {
    /// The value itself, None if the item has been
    /// evicted from memory
    pub value: Option<Vec<u8>>,
    pub cas: u64,
    pub by_seqno: u64,
    pub expiry_time: u32,
    pub flags: u32,
    pub rev_seqno: u64,
    pub(crate) bits: StoredValueBits,
}

bitflags! {
    #[derive(Default, Debug, Clone, Copy)]
    pub struct StoredValueBits: u8 {
        const IS_DIRTY = 0;
        const IS_DELETED = 1;
        const IS_RESIDENT = 2;
        const IS_STALE = 3;
    }
}

impl StoredValue {
    pub fn mark_not_resident(&mut self) {
        self.value = None;
        self.bits.remove(StoredValueBits::IS_RESIDENT);
    }
}
