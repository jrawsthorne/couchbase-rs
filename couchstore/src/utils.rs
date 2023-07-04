use crate::constants::COUCH_BLOCK_SIZE;
use std::time::SystemTime;

pub(crate) fn align_to_next_block(offset: usize) -> usize {
    if offset % COUCH_BLOCK_SIZE != 0 {
        return offset + COUCH_BLOCK_SIZE - (offset % COUCH_BLOCK_SIZE);
    }
    return offset;
}

pub fn now() -> u64 {
    let now = SystemTime::now();
    let duration = now.duration_since(std::time::UNIX_EPOCH).unwrap();
    return duration.as_nanos().try_into().unwrap();
}
