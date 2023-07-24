use bytes::{BufMut, BytesMut};
use memcached_codec::{McbpDecodeError, McbpMessage, McbpMessageBuilder, Opcode};

pub enum SaslAuthRequest {
    Plain { username: String, password: String },
}

#[derive(Debug, Clone, Copy)]
pub struct SaslAuthResponse {}

impl SaslAuthRequest {
    pub fn encode(&self) -> McbpMessage {
        let mut builder = McbpMessageBuilder::new(Opcode::SaslAuth);
        match self {
            SaslAuthRequest::Plain { username, password } => {
                let value = {
                    let mut bytes = BytesMut::with_capacity(2 + username.len() + password.len());
                    bytes.put_u8(0);
                    bytes.put(username.as_bytes());
                    bytes.put_u8(0);
                    bytes.put(password.as_bytes());
                    bytes.freeze()
                };
                builder = builder.key("PLAIN").value(value);
            }
        }
        builder.build()
    }
}

impl SaslAuthResponse {
    pub fn decode(_message: &McbpMessage) -> Result<SaslAuthResponse, McbpDecodeError> {
        Ok(SaslAuthResponse {})
    }
}
