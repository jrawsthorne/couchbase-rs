pub struct Item {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub cas: u64,
    pub expiry_time: u32,
    pub flags: u32,
    pub by_seqno: u64,
    pub rev_seqno: u64,
}
