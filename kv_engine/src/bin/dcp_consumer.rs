use std::net::TcpStream;

use kv_engine::{
    connection::Connection,
    operations::{
        dcp::{DcpOpenConnectionRequest, DcpOpenFlag, DcpStreamAddFlag, DcpStreamRequest},
        select_bucket::SelectBucketRequest,
    },
};

fn main() {
    tracing_subscriber::fmt::init();
    let stream = TcpStream::connect("127.0.0.1:11210").unwrap();
    let mut client = Connection::new(stream);
    client.hello();
    client.auth("Administrator".to_string(), "password".to_string());
    client.send(
        SelectBucketRequest {
            bucket: "travel-sample".to_string(),
        }
        .encode(),
    );
    client.send(
        DcpOpenConnectionRequest {
            stream_name: "test".to_string(),
            flags: DcpOpenFlag::PRODUCER,
        }
        .encode(),
    );
    client.send_dcp_control("enable_noop".to_string(), "true".to_string());
    client.send_dcp_control("set_noop_interval".to_string(), "180".to_string());
    client.send(
        DcpStreamRequest {
            vbucket: 0,
            flags: DcpStreamAddFlag::empty(),
            start_seqno: 0,
            end_seqno: u64::MAX,
            vb_uuid: 0,
            snap_start_seqno: 0,
            snap_end_seqno: 0,
        }
        .encode(),
    );
    loop {
        client.recv();
    }
}
