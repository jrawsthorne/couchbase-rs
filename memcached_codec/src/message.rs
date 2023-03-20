use crate::{Cas, DataType, Magic, McbpDecodeError, Opcode, Status};
use bytes::Bytes;

/// McbpMessage defines the fields contained in a full message sent between client and server
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct McbpMessage {
    pub magic: Magic,
    pub opcode: Opcode,
    pub data_type: DataType,
    pub specific: Specific,
    pub opaque: u32,
    pub cas: Cas,
    pub extras: Bytes,
    pub framing_extras: Bytes,
    pub key: Bytes,
    pub value: Bytes,
}

/// Specific defines the requests or response specific value
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Specific {
    Status(Status),
    Vbucket(u16),
}

impl McbpMessage {
    /// Unwrap the status of a response message
    pub fn try_status(&self) -> Result<Status, McbpDecodeError> {
        match self.specific {
            Specific::Status(status) => Ok(status),
            _ => Err(McbpDecodeError::MissingStatus),
        }
    }

    /// Unwrap the vbucket of a request message
    pub fn try_vbucket(&self) -> Result<u16, McbpDecodeError> {
        match self.specific {
            Specific::Vbucket(vbucket) => Ok(vbucket),
            _ => Err(McbpDecodeError::MissingVbucket),
        }
    }
}

/// McbpMessageBuilder can be used to build an [McbpMessage]
pub struct McbpMessageBuilder {
    pub magic: Magic,
    pub opcode: Opcode,
    pub data_type: DataType,
    pub specific: Specific,
    pub opaque: u32,
    pub cas: Cas,
    pub extras: Bytes,
    pub framing_extras: Bytes,
    pub key: Bytes,
    pub value: Bytes,
}

impl McbpMessageBuilder {
    pub fn new(opcode: Opcode) -> McbpMessageBuilder {
        McbpMessageBuilder {
            opcode,
            magic: Default::default(),
            data_type: Default::default(),
            specific: Specific::Vbucket(Default::default()),
            opaque: Default::default(),
            cas: Default::default(),
            extras: Default::default(),
            framing_extras: Default::default(),
            key: Default::default(),
            value: Default::default(),
        }
    }

    pub fn magic(mut self, magic: Magic) -> Self {
        self.magic = magic;
        self
    }

    pub fn data_type(mut self, data_type: DataType) -> Self {
        self.data_type = data_type;
        self
    }

    pub fn vbucket(mut self, vbucket: u16) -> Self {
        self.specific = Specific::Vbucket(vbucket);
        self
    }

    pub fn status(mut self, status: Status) -> Self {
        self.specific = Specific::Status(status);
        self
    }

    pub fn opaque(mut self, opaque: u32) -> Self {
        self.opaque = opaque;
        self
    }

    pub fn cas(mut self, cas: Cas) -> Self {
        self.cas = cas;
        self
    }

    pub fn extras(mut self, extras: impl Into<Bytes>) -> Self {
        self.extras = extras.into();
        self
    }

    pub fn framing_extras(mut self, framing_extras: impl Into<Bytes>) -> Self {
        self.framing_extras = framing_extras.into();
        self
    }

    pub fn key(mut self, key: impl Into<Bytes>) -> Self {
        self.key = key.into();
        self
    }

    pub fn value(mut self, value: impl Into<Bytes>) -> Self {
        self.value = value.into();
        self
    }

    pub fn build(self) -> McbpMessage {
        McbpMessage {
            magic: self.magic,
            opcode: self.opcode,
            data_type: self.data_type,
            specific: self.specific,
            opaque: self.opaque,
            cas: self.cas,
            extras: self.extras,
            framing_extras: self.framing_extras,
            key: self.key,
            value: self.value,
        }
    }
}
