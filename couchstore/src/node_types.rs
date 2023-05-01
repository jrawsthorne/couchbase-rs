use std::io::{Cursor, Read};

use crate::DiskVersion;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

#[derive(Default, Debug, PartialEq, Eq, Clone, Copy)]
pub struct RawFileHeaderV13 {
    pub version: DiskVersion,
    pub update_seq: u64,
    pub purge_seq: u64,
    pub purge_ptr: u64,
    pub timestamp: u64,
    pub seqrootsize: u16,
    pub idrootsize: u16,
    pub localrootsize: u16,
}

impl RawFileHeaderV13 {
    pub const ON_DISK_SIZE: usize = 33;

    pub fn decode(buf: &mut Cursor<&[u8]>) -> RawFileHeaderV13 {
        let version = DiskVersion::try_from(buf.read_u8().unwrap()).unwrap();
        let update_seq = buf.read_u48::<BigEndian>().unwrap();
        let purge_seq = buf.read_u48::<BigEndian>().unwrap();
        let purge_ptr = buf.read_u48::<BigEndian>().unwrap();
        let seqrootsize = buf.read_u16::<BigEndian>().unwrap();
        let idrootsize = buf.read_u16::<BigEndian>().unwrap();
        let localrootsize = buf.read_u16::<BigEndian>().unwrap();
        let timestamp = buf.read_u64::<BigEndian>().unwrap();
        RawFileHeaderV13 {
            version,
            update_seq,
            purge_seq,
            purge_ptr,
            timestamp,
            seqrootsize,
            idrootsize,
            localrootsize,
        }
    }

    pub fn encode(&self, buf: &mut Cursor<&mut [u8]>) {
        buf.write_u8(self.version.into()).unwrap();
        buf.write_u48::<BigEndian>(self.update_seq).unwrap();
        buf.write_u48::<BigEndian>(self.purge_seq).unwrap();
        buf.write_u48::<BigEndian>(self.purge_ptr).unwrap();
        buf.write_u16::<BigEndian>(self.seqrootsize).unwrap();
        buf.write_u16::<BigEndian>(self.idrootsize).unwrap();
        buf.write_u16::<BigEndian>(self.localrootsize).unwrap();
        buf.write_u64::<BigEndian>(self.timestamp).unwrap();
    }
}

pub type RawKvLength = [u8; 5];

fn decode_kv_length(kv: &RawKvLength, klen: &mut u32, vlen: &mut u32) {
    // 12, 28 bit
    let klen_raw = ((kv[0] as u16) << 4) | ((kv[1] as u16) & 0xf0) >> 4;
    *klen = klen_raw as u32;

    let mut vlen_raw: [u8; 4] = [0; 4];
    vlen_raw.copy_from_slice(&kv[1..5]);

    *vlen = u32::from_be_bytes([vlen_raw[0] & 0x0F, vlen_raw[1], vlen_raw[2], vlen_raw[3]]);
}

pub fn read_kv<'a>(buf: &mut Cursor<&'a [u8]>) -> Option<(&'a [u8], &'a [u8])> {
    let mut kv = [0; 5];
    let mut klen: u32 = 0;
    let mut vlen: u32 = 0;
    buf.read_exact(&mut kv).unwrap();
    decode_kv_length(&kv, &mut klen, &mut vlen);

    let key = buf
        .get_ref()
        .get(buf.position() as usize..(buf.position() + klen as u64) as usize)
        .unwrap();
    buf.set_position(buf.position() + klen as u64);
    let value = buf
        .get_ref()
        .get(buf.position() as usize..(buf.position() + vlen as u64) as usize)
        .unwrap();
    buf.set_position(buf.position() + vlen as u64);

    Some((key, value))
}

struct RawNodePointer {
    pub pointer: u64,
    pub sub_size: u16,
    pub reduce_value: Vec<u8>,
}

pub fn read_raw_node_pointer() {}