use bitflags::bitflags;
use bytes::{BufMut, BytesMut};
use memcached_codec::{McbpMessage, McbpMessageBuilder, Opcode};

pub type VbUuid = u64;

pub struct DcpStreamRequest {
    pub vbucket: u16,
    pub flags: DcpStreamAddFlag,
    pub start_seqno: u64,
    pub end_seqno: u64,
    pub vb_uuid: VbUuid,
    pub snap_start_seqno: u64,
    pub snap_end_seqno: u64,
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct DcpStreamAddFlag: u32 {
        const DISK_ONLY = 0x02;
        const LATEST = 0x04;
        const ACTIVE_ONLY = 0x10;
        const StrictVBUUID = 0x20;
    }
}

impl DcpStreamRequest {
    pub fn encode(self) -> McbpMessage {
        let mut extras = BytesMut::with_capacity(48);
        extras.put_u32(self.flags.bits());
        extras.put_u32(0);
        extras.put_u64(self.start_seqno);
        extras.put_u64(self.end_seqno);
        extras.put_u64(self.vb_uuid);
        extras.put_u64(self.snap_start_seqno);
        extras.put_u64(self.snap_end_seqno);
        assert_eq!(extras.len(), 48);
        McbpMessageBuilder::new(Opcode::DcpStreamRequest)
            .extras(extras)
            .vbucket(self.vbucket)
            .build()
    }
}

pub struct DcpOpenConnectionRequest {
    pub stream_name: String,
    pub flags: DcpOpenFlag,
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct DcpOpenFlag: u32 {
        const PRODUCER = 0x01;
        const INCLUDE_XATTRS = 0x04;
        const NO_VALUE = 0x08;
        const INCLUDE_DELETE_TIMES = 0x20;
        const PITR = 0x80;
    }
}

impl DcpOpenConnectionRequest {
    pub fn encode(self) -> McbpMessage {
        let mut extras = BytesMut::with_capacity(8);
        extras.put_u32(0);
        extras.put_u32(self.flags.bits());
        McbpMessageBuilder::new(Opcode::DcpOpenConnection)
            .key(self.stream_name)
            .extras(extras)
            .build()
    }
}

pub struct DcpControlRequest {
    pub key: String,
    pub value: String,
}

impl DcpControlRequest {
    pub fn encode(self) -> McbpMessage {
        McbpMessageBuilder::new(Opcode::DcpControl)
            .key(self.key)
            .value(self.value)
            .build()
    }
}
