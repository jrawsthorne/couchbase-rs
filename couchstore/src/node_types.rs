use std::io::{self, Cursor, Read};

use crate::{DiskVersion, DocInfo};
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

    pub fn decode(mut buf: impl io::Read) -> RawFileHeaderV13 {
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

    pub fn _encode(&self, mut buf: impl io::Write) {
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

fn decode_kv_length(kv: &RawKvLength) -> (u32, u32) {
    // kv[0] is the first byte of the key length. Read BE so put as the first 8 bytes
    // kv[1] contains the last 4 bits of the key length and the first 4 bits of the value length
    // & 0xF0 to remove the last 4 bits of the second byte
    let klen_raw = ((kv[0] as u16) << 4) | ((kv[1] as u16) & 0xF0) >> 4;
    let klen = klen_raw as u32;

    let mut vlen_raw: [u8; 4] = [0; 4];
    vlen_raw.copy_from_slice(&kv[1..5]);

    // & 0x0F to remove the first 4 bits of the second byte
    let vlen = u32::from_be_bytes([vlen_raw[0] & 0x0F, vlen_raw[1], vlen_raw[2], vlen_raw[3]]);

    (klen, vlen)
}

pub fn encode_kv_length(key_length: u32, value_length: u32) -> RawKvLength {
    let mut kv = [0; 5];
    // key length is 12 bits, so the first byte is the first 8 bits of the key length
    kv[0] = (key_length >> 4) as u8;
    // the last 4 bits of the first byte and the first 4 bits of the second byte are the last 8 bits of the key length
    kv[1] = ((key_length & 0x0F) << 4) as u8 | ((value_length >> 24) & 0x0F) as u8;
    kv[2] = (value_length >> 16) as u8;
    kv[3] = (value_length >> 8) as u8;
    kv[4] = value_length as u8;
    kv
}

pub fn read_kv<'a>(buf: &mut Cursor<&'a [u8]>) -> Option<(&'a [u8], &'a [u8])> {
    let mut kv = [0; 5];
    buf.read_exact(&mut kv).unwrap();
    let (klen, vlen) = decode_kv_length(&kv);

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

pub fn write_kv<W: io::Write>(mut buf: W, key: &[u8], value: &[u8]) {
    let kv = encode_kv_length(key.len() as u32, value.len() as u32);
    buf.write_all(&kv).unwrap();
    buf.write_all(key).unwrap();
    buf.write_all(value).unwrap();
}

impl DocInfo {
    pub fn encode_id_index_value<W: io::Write>(&self, mut buf: W) {
        buf.write_u48::<BigEndian>(self.db_seq).unwrap();
        buf.write_u32::<BigEndian>(self.physical_size).unwrap();
        buf.write_u48::<BigEndian>(
            self.bp | {
                // set the first bit of the first byte to 1 if deleted
                if self.deleted {
                    1 << 47
                } else {
                    0
                }
            },
        )
        .unwrap();
        buf.write_u8(self.content_meta.bits()).unwrap();
        buf.write_u48::<BigEndian>(self.rev_seq).unwrap();
        buf.write_all(&self.rev_meta).unwrap();
    }

    pub fn encode_seq_index_value<W: io::Write>(&self, mut buf: W) {
        let sizes = encode_kv_length(self.id.len() as u32, self.physical_size);
        buf.write_all(&sizes).unwrap();
        buf.write_u48::<BigEndian>(self.bp).unwrap();
        buf.write_u8(self.content_meta.bits()).unwrap();
        buf.write_u48::<BigEndian>(self.rev_seq).unwrap();
        buf.write_all(&self.id).unwrap();
        buf.write_all(&self.rev_meta).unwrap();
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_kv_length_roundtrip() {
        let kv = encode_kv_length(1234, 5678);
        let (klen, vlen) = decode_kv_length(&kv);
        assert_eq!(klen, 1234);
        assert_eq!(vlen, 5678);
    }
}
