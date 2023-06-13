use byteorder::{BigEndian, WriteBytesExt};
use std::io::{Cursor, Seek, SeekFrom, Write};

use crate::{constants::COUCH_BLOCK_SIZE, utils::align_to_next_block, DiskBlockType, TreeFile};

impl TreeFile {
    pub fn write_entire_buffer(&mut self, buf: &[u8], offset: usize) {
        self.file.seek(SeekFrom::Start(offset as u64)).unwrap();
        self.file.write_all(buf).unwrap();
        self.file.flush().unwrap();
    }

    pub fn raw_write(&mut self, disk_block_type: DiskBlockType, mut buf: &[u8], pos: usize) {
        let mut write_pos = pos;
        let mut block_remain = 0;
        // break up the write buffer into blocks adding the block prefix as needed
        while !buf.is_empty() {
            block_remain = COUCH_BLOCK_SIZE - (write_pos % COUCH_BLOCK_SIZE);
            if block_remain > buf.len() {
                block_remain = buf.len();
            }

            if write_pos % COUCH_BLOCK_SIZE == 0 {
                self.write_entire_buffer(&[disk_block_type.into()], write_pos);
                write_pos += 1;
                continue;
            }

            self.write_entire_buffer(&buf[..block_remain], write_pos);
            write_pos += block_remain;
            buf = &buf[block_remain..];
        }
    }

    pub fn write_header(&mut self, buf: &[u8]) -> usize {
        let mut write_pos = align_to_next_block(self.pos);

        let size = (buf.len() + 4) as u32; // Len before header includes hash len.
        let crc32 = crc32c::crc32c(buf);

        let mut header_buf = [0u8; 9];
        let mut cursor = Cursor::new(&mut header_buf[..]);

        let pos = write_pos;

        // Write the header's block header
        cursor.write_u8(DiskBlockType::Header.into()).unwrap();
        cursor.write_u32::<BigEndian>(size).unwrap();
        cursor.write_u32::<BigEndian>(crc32).unwrap();

        dbg!(&header_buf);
        dbg!(&buf);

        self.write_entire_buffer(&header_buf, write_pos);

        write_pos += header_buf.len();

        // Write actual header
        self.raw_write(DiskBlockType::Header, buf, write_pos);
        write_pos += buf.len();
        self.pos = write_pos;

        pos
    }

    pub fn db_write_buf(&mut self, buf: &[u8], pos: &mut u64, disk_size: &mut u32) {}

    pub fn db_write_buf_compressed(&mut self, buf: &[u8], pos: &mut u64, disk_size: &mut u32) {}
}
