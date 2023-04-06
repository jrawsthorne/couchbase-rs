use crate::constants::COUCH_BLOCK_SIZE;

pub(crate) fn align_to_next_block(offset: usize) -> usize {
    if offset % COUCH_BLOCK_SIZE != 0 {
        return offset + COUCH_BLOCK_SIZE - (offset % COUCH_BLOCK_SIZE);
    }
    return offset;
}
