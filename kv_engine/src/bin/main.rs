use anyhow::Result;
use bytes::Bytes;
use couchstore::{DBOpenOptions, Db, OpenOptions};
use kv_engine::{
    connection::Connection,
    operations::{
        cluster_config::{ClusterConfig, GetClusterConfigResponse, Node, VBucketServerMap},
        get::{GetRequest, GetResponse},
        hello::HelloResponse,
        select_bucket::{SelectBucketRequest, SelectBucketResponse},
        set::{SetRequest, SetResponse},
    },
};
use memcached_codec::{
    feature::Feature, Cas, DataType, Magic, McbpMessage, McbpMessageBuilder, Opcode, Status,
};
use std::{fs::File, net::TcpListener, thread, time::Duration};
use tracing::{error, info, level_filters::LevelFilter};
use tracing_subscriber::{prelude::*, Registry};

const DATA_PATH: &str = "./data";

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() < 3 {
        // TODO: Actually parse args rather than assume third arg is config
        println!("Usage: {} -C <config>", args[0]);
        std::process::exit(1);
    }

    let config: kv_engine::Config =
        serde_json::from_str(&std::fs::read_to_string(&args[2]).unwrap())
            .expect("Failed to parse config");

    setup_logger(&config);

    let mut jhs = Vec::new();

    for interface in config.interfaces {
        if interface.host != "*" {
            continue;
        }
        let jh = std::thread::spawn(move || start_listener(interface.port));
        jhs.push(jh);
    }

    for jh in jhs {
        jh.join().unwrap();
    }
}

fn start_listener(port: u16) {
    let listener = TcpListener::bind(format!("127.0.0.1:{port}")).unwrap();
    info!("Listening on port {port}");

    for stream in listener.incoming() {
        std::thread::spawn(|| {
            let stream = stream.unwrap();
            let connection = Connection::new(stream);
            if let Err(err) = handle_connection(connection) {
                error!("Error: {:?}", err);
                thread::sleep(Duration::from_secs(5));
            }
        });
    }
}

fn setup_logger(config: &kv_engine::Config) {
    let logger_file = File::create(config.logger.filename.clone()).unwrap();
    let layer = tracing_subscriber::fmt::layer()
        .with_writer(logger_file)
        .with_filter(LevelFilter::TRACE);
    let subscriber = Registry::default().with(layer);
    tracing::subscriber::set_global_default(subscriber).unwrap();
}

#[derive(Default)]
struct State {
    bucket: Option<String>,
}

fn handle_connection(mut connection: Connection) -> Result<()> {
    let mut state = State::default();

    loop {
        let req = connection.recv()?.unwrap();

        info!("Received message: {:?}", req);
        let to_send = handle_message(&mut state, &req);
        if let Some(mut resp) = to_send {
            resp.opaque = req.opaque;
            resp.magic = Magic::ClientResponse;

            info!("Sending message: {:?}", resp);

            connection.send(resp);
        }
    }
}

fn handle_message(state: &mut State, message: &McbpMessage) -> Option<McbpMessage> {
    match message.opcode {
        Opcode::Get => {
            let req = GetRequest::decode(message).unwrap();
            let vbucket = req.vbucket;
            let key = req.key;
            let bucket = state.bucket.as_ref().unwrap();
            let mut db = Db::open(
                format!("{DATA_PATH}/{bucket}/{vbucket}.couch.1"),
                DBOpenOptions::default(),
            );
            if let Some(docinfo) = db.docinfo_by_id(key.to_vec()) {
                let value = db
                    .open_doc_with_docinfo(&docinfo, OpenOptions::DECOMPRESS_DOC_BODIES)
                    .unwrap();

                let resp = GetResponse {
                    value: Some(Bytes::from(value.data)),
                    flags: 0,
                    cas: Cas::default(),
                    data_type: DataType::JSON,
                };

                Some(resp.encode())
            } else {
                let resp = GetResponse {
                    value: None,
                    flags: 0,
                    cas: Cas::default(),
                    data_type: DataType::RAW,
                };
                Some(resp.encode())
            }
        }
        Opcode::Upsert => {
            let req = SetRequest::decode(message).unwrap();
            let vbucket = req.vbucket;
            let key = req.key;
            let value = req.value;
            let bucket = state.bucket.as_ref().unwrap();
            let mut db = Db::open(
                format!("{DATA_PATH}/{bucket}/{vbucket}.couch.1"),
                DBOpenOptions::default(),
            );
            db.set(key.to_vec(), value.to_vec());
            db.commit();
            let resp = SetResponse {
                cas: Cas::default(),
                data_type: DataType::RAW,
            };
            Some(resp.encode())
        }
        Opcode::Hello => {
            let res = HelloResponse {
                supported_features: vec![Feature::SelectBucket, Feature::Json],
            }
            .encode();
            Some(res)
        }
        Opcode::SelectBucket => {
            let req: SelectBucketRequest = SelectBucketRequest::decode(message).unwrap();
            let bucket = req.bucket;

            std::fs::create_dir_all(format!("{DATA_PATH}/{bucket}")).unwrap();

            state.bucket = Some(bucket);

            let resp = SelectBucketResponse {}.encode();
            Some(resp)
        }
        Opcode::GetClusterConfig => {
            let config = if state.bucket.is_some() {
                default_bucket_config(state)
            } else {
                default_cluster_config()
            };
            let resp = GetClusterConfigResponse { config }.encode();
            Some(resp)
        }
        Opcode::SaslListMechs => {
            let resp = McbpMessageBuilder::new(Opcode::SaslListMechs)
                .value("PLAIN")
                .build();
            Some(resp)
        }
        Opcode::SaslAuth => {
            let resp = McbpMessageBuilder::new(Opcode::SaslAuth)
                .status(Status::Success)
                .build();
            Some(resp)
        }
        Opcode::GetErrorMap => {
            let resp = McbpMessageBuilder::new(Opcode::GetErrorMap)
                .status(Status::KeyNotFound)
                .build();
            Some(resp)
        }
        _ => {
            info!("Unknown opcode: {:?}", message.opcode);
            let resp = McbpMessageBuilder::new(message.opcode)
                .status(Status::Success)
                .build();
            std::thread::sleep(Duration::from_secs(5));
            Some(resp)
        }
    }
}

fn default_bucket_config(state: &State) -> ClusterConfig {
    let bucket = state.bucket.clone().unwrap();
    ClusterConfig {
        rev: 1,
        rev_epoch: 1,
        bucket_capabilities_ver: Some(String::new()),
        bucket_capabilities: Some(
            vec![
                "durableWrite",
                "tombstonedUserXAttrs",
                "couchapi",
                "dcp.IgnorePurgedTombstones",
                "dcp",
                "cbhello",
                "touch",
                "cccp",
                "xdcrCheckpointing",
                "nodesExt",
                "xattr",
            ]
            .into_iter()
            .map(|s| s.to_string())
            .collect(),
        ),
        name: Some(bucket.clone()),
        uri: Some(format!(
            "/pools/default/buckets/{bucket}?bucket_uuid=c4730ffcb639bd2c54d11944c80ffb31"
        )),
        streaming_uri: Some(format!(
            "/pools/default/bucketsStreaming/{bucket}?bucket_uuid=c4730ffcb639bd2c54d11944c80ffb31"
        )),
        nodes: Some(vec![Node {
            couch_api_base: format!(
                "http://127.0.0.1:8092/{bucket}%2Bc4730ffcb639bd2c54d11944c80ffb31"
            ),
            hostname: Some("127.0.0.1:8091".to_string()),
            ports: maplit::hashmap! {
                "direct".to_string() =>11210,
            },
        }]),
        node_locator: Some("vbucket".to_string()),
        uuid: Some("c4730ffcb639bd2c54d11944c80ffb31".to_string()),
        ddocs: None,
        v_bucket_server_map: Some(VBucketServerMap {
            hash_algorithm: "CRC".to_string(),
            num_replicas: 0,
            server_list: vec!["127.0.0.1:11210".to_string()],
            v_bucket_map: vec![vec![0]; 1024],
        }),
    }
}

fn default_cluster_config() -> ClusterConfig {
    ClusterConfig {
        rev: 1,
        rev_epoch: 1,
        bucket_capabilities_ver: Some(String::new()),
        bucket_capabilities: Some(
            vec![
                "durableWrite",
                "tombstonedUserXAttrs",
                "couchapi",
                "dcp.IgnorePurgedTombstones",
                "dcp",
                "cbhello",
                "touch",
                "cccp",
                "xdcrCheckpointing",
                "nodesExt",
                "xattr",
            ]
            .into_iter()
            .map(|s| s.to_string())
            .collect(),
        ),
        name: None,
        uri: None,
        streaming_uri: None,
        nodes: Some(vec![Node {
            couch_api_base: "".to_string(),
            hostname: Some("127.0.0.1:8091".to_string()),
            ports: maplit::hashmap! {
                "direct".to_string() =>11210,
            },
        }]),
        node_locator: Some("vbucket".to_string()),
        uuid: Some("c4730ffcb639bd2c54d11944c80ffb31".to_string()),
        ddocs: None,
        v_bucket_server_map: Some(VBucketServerMap {
            hash_algorithm: "CRC".to_string(),
            num_replicas: 0,
            server_list: vec!["127.0.0.1:11210".to_string()],
            v_bucket_map: vec![vec![0]; 1024],
        }),
    }
}
