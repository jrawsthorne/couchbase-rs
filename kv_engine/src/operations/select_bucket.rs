use memcached_codec::{McbpDecodeError, McbpMessage, McbpMessageBuilder, Opcode, Status};

#[derive(Debug)]
pub struct SelectBucketRequest {
    pub bucket: String,
}

#[derive(Debug, Clone, Copy)]
pub struct SelectBucketResponse {}

impl SelectBucketRequest {
    pub fn encode(&self) -> McbpMessage {
        McbpMessageBuilder::new(Opcode::SelectBucket)
            .key(self.bucket.clone())
            .build()
    }

    pub fn decode(message: &McbpMessage) -> Result<SelectBucketRequest, McbpDecodeError> {
        Ok(SelectBucketRequest {
            bucket: String::from_utf8(message.key.to_vec()).unwrap(),
        })
    }
}

impl SelectBucketResponse {
    pub fn encode(&self) -> McbpMessage {
        McbpMessageBuilder::new(Opcode::SelectBucket)
            .status(Status::Success)
            .build()
    }

    pub fn decode() -> Result<SelectBucketResponse, McbpDecodeError> {
        Ok(SelectBucketResponse {})
    }
}
