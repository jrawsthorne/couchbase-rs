use std::{
    io::{Read, Write},
    net::TcpStream,
};

use bytes::BytesMut;
use memcached_codec::{McbpCodec, McbpMessage};
use tokio_util::codec::{Decoder, Encoder};
use tracing::info;

use crate::operations::{
    dcp::DcpControlRequest,
    hello::{HelloRequest, HelloResponse},
    sasl_auth::{SaslAuthRequest, SaslAuthResponse},
};

pub struct Connection {
    stream: TcpStream,
    read_buffer: BytesMut,
    write_buffer: BytesMut,
    mcbp_codec: McbpCodec,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Connection {
        Connection {
            stream,
            read_buffer: BytesMut::new(),
            write_buffer: BytesMut::new(),
            mcbp_codec: McbpCodec::new(),
        }
    }

    pub fn send(&mut self, message: McbpMessage) {
        info!("Sent message: {:?}", message);
        self.mcbp_codec
            .encode(message, &mut self.write_buffer)
            .unwrap();
        self.stream.write_all(&self.write_buffer).unwrap();
        self.write_buffer.clear();
    }

    pub fn recv(&mut self) -> McbpMessage {
        loop {
            match self.mcbp_codec.decode(&mut self.read_buffer) {
                Ok(Some(message)) => {
                    info!("Received message: {:?}", message);
                    return message;
                }
                Ok(None) => {
                    let mut buf = [0; 1024];
                    let n = self.stream.read(&mut buf).unwrap();
                    self.read_buffer.extend_from_slice(&buf[..n]);
                }
                Err(e) => panic!("Error: {:?}", e),
            }
        }
    }

    pub fn hello(&mut self) -> HelloResponse {
        let req = HelloRequest {
            features: HelloRequest::default_features(),
            user_agent: "couchbase-rs".to_string(),
        };
        self.send(req.encode());
        let resp = self.recv();
        HelloResponse::decode(&resp).unwrap()
    }

    pub fn auth(&mut self, username: String, password: String) -> SaslAuthResponse {
        let req = SaslAuthRequest::Plain { username, password };
        self.send(req.encode());
        let resp = self.recv();
        SaslAuthResponse::decode(&resp).unwrap()
    }

    pub fn send_dcp_control(&mut self, key: String, value: String) {
        self.send(DcpControlRequest { key, value }.encode());
    }
}
