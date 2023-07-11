use std::collections::HashMap;

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterConfig {
    pub rev: u32,
    pub rev_epoch: u32,
    pub bucket_capabilities_ver: Option<String>,
    pub bucket_capabilities: Option<Vec<String>>,
    pub name: Option<String>,
    pub uri: Option<String>,
    pub streaming_uri: Option<String>,
    pub nodes: Option<Vec<Node>>,
    pub node_locator: Option<String>,
    pub uuid: Option<String>,
    pub ddocs: Option<HashMap<String, String>>,
    pub v_bucket_server_map: Option<VBucketServerMap>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeExt {
    services: HashMap<String, u16>,
    #[serde(default)]
    this_node: bool,
    hostname: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VBucketServerMap {
    pub hash_algorithm: String,
    pub num_replicas: u32,
    pub server_list: Vec<String>,
    pub v_bucket_map: Vec<Vec<i32>>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Node {
    pub couch_api_base: String,
    pub hostname: Option<String>,
    pub ports: HashMap<String, u16>,
}

use memcached_codec::{McbpDecodeError, McbpMessage, McbpMessageBuilder, Opcode, Status};

#[derive(Debug)]
pub struct GetClusterConfigRequest {}

#[derive(Debug, Clone)]
pub struct GetClusterConfigResponse {
    pub config: ClusterConfig,
}

impl GetClusterConfigRequest {
    pub fn encode(&self) -> McbpMessage {
        McbpMessageBuilder::new(Opcode::GetClusterConfig).build()
    }

    pub fn decode() -> Result<GetClusterConfigRequest, McbpDecodeError> {
        Ok(GetClusterConfigRequest {})
    }
}

impl GetClusterConfigResponse {
    pub fn encode(&self) -> McbpMessage {
        McbpMessageBuilder::new(Opcode::GetClusterConfig)
            .status(Status::Success)
            .value(serde_json::to_vec(&self.config).unwrap())
            .build()
    }

    pub fn decode(resp: &McbpMessage) -> Result<GetClusterConfigResponse, McbpDecodeError> {
        let config = serde_json::from_slice(&resp.value).unwrap();
        Ok(GetClusterConfigResponse { config })
    }
}
