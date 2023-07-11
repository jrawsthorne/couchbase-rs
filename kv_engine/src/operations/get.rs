use bytes::{Buf, Bytes};

use memcached_codec::{
    Cas, DataType, McbpDecodeError, McbpMessage, McbpMessageBuilder, Opcode, Status,
};

use super::v_bucket_hash;

#[derive(Debug)]
pub struct GetRequest {
    pub key: Bytes,
    pub vbucket: u16,
}

#[derive(Debug, Clone)]
pub struct GetResponse {
    pub value: Option<Bytes>,
    pub flags: u32,
    pub cas: Cas,
    pub data_type: DataType,
}

impl GetRequest {
    pub fn encode(&self) -> McbpMessage {
        McbpMessageBuilder::new(Opcode::Get)
            .key(self.key.clone())
            .vbucket(v_bucket_hash(&self.key, 1024))
            .build()
    }

    pub fn decode(resp: &McbpMessage) -> Result<GetRequest, McbpDecodeError> {
        Ok(GetRequest {
            key: resp.key.clone(),
            vbucket: resp.try_vbucket().unwrap(),
        })
    }
}

impl GetResponse {
    pub fn encode(&self) -> McbpMessage {
        let (value, status) = if let Some(value) = &self.value {
            (value.clone(), Status::Success)
        } else {
            (Bytes::new(), Status::KeyNotFound)
        };
        McbpMessageBuilder::new(Opcode::Get)
            .status(status)
            .cas(self.cas)
            .data_type(self.data_type)
            .value(value)
            .extras(self.flags.to_be_bytes().to_vec())
            .build()
    }

    pub fn decode(resp: &McbpMessage) -> Result<GetResponse, McbpDecodeError> {
        let mut extras = &resp.extras[..];
        let mut flags = 0;
        if extras.len() == 4 {
            flags = extras.get_u32();
        }
        Ok(GetResponse {
            value: Some(resp.value.clone()),
            flags,
            cas: resp.cas,
            data_type: resp.data_type,
        })
    }
}
