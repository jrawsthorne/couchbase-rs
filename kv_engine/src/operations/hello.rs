use bytes::{Buf, BufMut, BytesMut};
use tracing::warn;

use memcached_codec::{
    feature::Feature, McbpDecodeError, McbpMessage, McbpMessageBuilder, Opcode, Status,
};

#[derive(Debug)]
pub struct HelloRequest {
    pub features: Vec<Feature>,
    pub user_agent: String,
}

impl HelloRequest {
    pub fn encode(&self) -> McbpMessage {
        let mut key = BytesMut::with_capacity(self.user_agent.len());
        let mut value = BytesMut::with_capacity(self.features.len() * 2);

        key.put_slice(self.user_agent.as_bytes());

        for &feature in &self.features {
            value.put_u16(feature.into());
        }

        McbpMessageBuilder::new(Opcode::Hello)
            .key(key)
            .value(value)
            .build()
    }

    pub fn decode(resp: &McbpMessage) -> Result<HelloRequest, McbpDecodeError> {
        let mut value = &resp.value[..];
        let mut features = Vec::with_capacity(value.len() / 2);
        for _ in 0..value.len() / 2 {
            let feature = value.get_u16();
            if let Ok(feature) = Feature::try_from(feature) {
                features.push(feature);
            } else {
                warn!("unknown feature ({})", feature);
            }
        }
        let user_agent = String::from_utf8(resp.key.to_vec()).unwrap();
        Ok(HelloRequest {
            features,
            user_agent,
        })
    }
}

#[derive(Debug)]
pub struct HelloResponse {
    pub supported_features: Vec<Feature>,
}

impl HelloResponse {
    pub fn decode(resp: &McbpMessage) -> Result<HelloResponse, McbpDecodeError> {
        let num_features = resp.value.len() / 2;
        let mut supported_features = Vec::with_capacity(num_features);
        let mut value = &resp.value[..];
        for _ in 0..num_features {
            let feature = value.get_u16();
            if let Ok(feature) = Feature::try_from(feature) {
                supported_features.push(feature);
            } else {
                warn!("unknown feature ({})", feature);
            }
        }
        Ok(HelloResponse { supported_features })
    }

    pub fn encode(&self) -> McbpMessage {
        let mut value = BytesMut::with_capacity(self.supported_features.len() * 2);
        for &feature in &self.supported_features {
            value.put_u16(feature.into());
        }
        McbpMessageBuilder::new(Opcode::Hello)
            .status(Status::Success)
            .value(value)
            .build()
    }
}
