use bytes::Bytes;

use memcached_codec::{
    Cas, DataType, McbpDecodeError, McbpMessage, McbpMessageBuilder, Opcode, Status,
};

use super::v_bucket_hash;

#[derive(Debug)]
pub struct SetRequest {
    pub key: Bytes,
    pub value: Bytes,
    pub vbucket: u16,
}

#[derive(Debug, Clone)]
pub struct SetResponse {
    pub cas: Cas,
    pub data_type: DataType,
}

impl SetRequest {
    pub fn encode(&self) -> McbpMessage {
        McbpMessageBuilder::new(Opcode::Upsert)
            .key(self.key.clone())
            .vbucket(v_bucket_hash(&self.key, 1024))
            .build()
    }

    pub fn decode(resp: &McbpMessage) -> Result<SetRequest, McbpDecodeError> {
        Ok(SetRequest {
            vbucket: resp.try_vbucket().unwrap(),
            key: resp.key.clone(),
            value: resp.value.clone(),
        })
    }
}

impl SetResponse {
    pub fn encode(&self) -> McbpMessage {
        McbpMessageBuilder::new(Opcode::Upsert)
            .status(Status::Success)
            .cas(self.cas)
            .data_type(self.data_type)
            .build()
    }

    pub fn decode(resp: &McbpMessage) -> Result<SetResponse, McbpDecodeError> {
        Ok(SetResponse {
            cas: resp.cas,
            data_type: resp.data_type,
        })
    }
}
