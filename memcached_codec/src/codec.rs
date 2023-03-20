use crate::{
    message::Specific, Cas, DataType, Magic, McbpDecodeError, McbpMessage, Opcode, Status,
};
use bytes::{Buf, BufMut, BytesMut};
use std::convert::TryFrom;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Clone, Copy, Default)]
pub struct McbpCodec;

/// Codec that implements encoding and decoding of the Couchbase adapted Memcached Binary Protocol
impl McbpCodec {
    pub fn new() -> Self {
        Self
    }
}

const HEADER_LEN: usize = 24;

impl Decoder for McbpCodec {
    type Item = McbpMessage;

    type Error = McbpDecodeError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < HEADER_LEN {
            // Not enough data to read header.
            return Ok(None);
        }

        let total_body_length = (&src[8..12]).get_u32() as usize;

        if src.len() < HEADER_LEN + total_body_length {
            // The full frame has not yet arrived
            //
            // We reserve more space in the buffer. This is not strictly
            // necessary, but is a good idea performance-wise.
            src.reserve(HEADER_LEN + total_body_length - src.len());

            return Ok(None);
        }

        let magic = Magic::try_from(src.get_u8())?;
        let opcode = Opcode::from_u8(src.get_u8(), magic)?;
        let (key_length, framing_extras_length) = {
            if magic.is_alternative_encoding() {
                let framing_extras_length = src.get_u8() as usize;
                let key_length = src.get_u8() as usize;
                (key_length, framing_extras_length)
            } else {
                let key_length = src.get_u16() as usize;
                let framing_extras_length = 0;
                (key_length, framing_extras_length)
            }
        };
        let extras_length = src.get_u8() as usize;
        let data_type = DataType::try_from(src.get_u8())?;
        let vbucket_or_status = src.get_u16();
        let total_body_length = src.get_u32() as usize;
        let opaque = src.get_u32();
        let cas = Cas(src.get_u64());
        let extras = src.copy_to_bytes(extras_length);
        let framing_extras = src.copy_to_bytes(framing_extras_length);
        let key = src.copy_to_bytes(key_length);
        let value = src
            .copy_to_bytes(total_body_length - extras_length - framing_extras_length - key_length);

        let specific = if magic.is_request() {
            Specific::Vbucket(vbucket_or_status)
        } else {
            Specific::Status(Status::from(vbucket_or_status))
        };

        Ok(Some(McbpMessage {
            magic,
            opcode,
            data_type,
            specific,
            opaque,
            cas,
            extras,
            framing_extras,
            key,
            value,
        }))
    }
}

impl Encoder<McbpMessage> for McbpCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: McbpMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let total_body_length =
            item.framing_extras.len() + item.key.len() + item.value.len() + item.extras.len();
        let len = HEADER_LEN + total_body_length;

        dst.reserve(len);

        dst.put_u8(item.magic.into());
        dst.put_u8(item.opcode.into());

        if item.magic.is_alternative_encoding() {
            dst.put_u8(item.framing_extras.len() as u8);
            dst.put_u8(item.key.len() as u8);
        } else {
            assert!(item.framing_extras.is_empty());
            dst.put_u16(item.key.len() as u16);
        }

        dst.put_u8(item.extras.len() as u8);
        dst.put_u8(item.data_type.into());

        match item.specific {
            Specific::Vbucket(vbucket) => {
                dst.put_u16(vbucket);
            }
            Specific::Status(status) => {
                dst.put_u16(status.into());
            }
        }

        dst.put_u32(total_body_length as u32);
        dst.put_u32(item.opaque);
        dst.put_u64(item.cas.0);
        dst.put(item.extras);
        dst.put(item.framing_extras);
        dst.put(item.key);
        dst.put(item.value);

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use bytes::Bytes;
    use std::iter::FromIterator;

    #[test]
    fn test_roundtrip() {
        let mut codec = McbpCodec::new();
        let mut buf = BytesMut::new();

        let message = McbpMessage {
            magic: Magic::AltClientRequest,
            opcode: Opcode::Insert,
            data_type: DataType::JSON | DataType::SNAPPY,
            specific: Specific::Vbucket(14),
            opaque: 1,
            cas: Cas(345),
            extras: Bytes::from(vec![0x00, 0x01, 0x02, 0x03, 0x04, 0x05]),
            framing_extras: Bytes::from(vec![0x06, 0x07, 0x08, 0x09]),
            key: Bytes::from(vec![0x0a, 0x0b, 0x0c]),
            value: Bytes::from(vec![0x0d, 0x0e, 0x0f]),
        };

        codec.encode(message.clone(), &mut buf).unwrap();
        let decoded_message = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(message, decoded_message);
    }

    #[test]
    fn test_unknown_status() {
        let mut codec = McbpCodec::new();
        let mut buf = BytesMut::from_iter(vec![
            0x81, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ]);
        let decoded_message = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(
            decoded_message.specific,
            Specific::Status(Status::Unknown(0xffff))
        );
    }

    #[test]
    fn test_invalid_magic() {
        let mut codec = McbpCodec::new();
        let mut buf = BytesMut::from_iter(vec![
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ]);
        let error = codec.decode(&mut buf).unwrap_err();
        assert!(matches!(error, McbpDecodeError::InvalidMagic(0x00)));
    }

    #[test]
    fn test_invalid_opcode() {
        let mut codec = McbpCodec::new();
        let mut buf = BytesMut::from_iter(vec![
            0x81, 0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ]);
        let error = codec.decode(&mut buf).unwrap_err();
        assert!(matches!(error, McbpDecodeError::InvalidOpcode(0xff)));
    }

    #[test]
    fn test_invalid_data_type() {
        let mut codec = McbpCodec::new();
        let mut buf = BytesMut::from_iter(vec![
            0x81, 0x00, 0x00, 0x00, 0x00, 0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ]);
        let error = codec.decode(&mut buf).unwrap_err();
        assert!(matches!(error, McbpDecodeError::InvalidDataType(0xff)));
    }
}
