use std::convert::TryFrom;

use crate::error::McbpDecodeError;

/// Magic defines the type of message
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Magic {
    /// Request packet from client to server
    ClientRequest,
    /// The alternative request packet containing frame extras
    AltClientRequest,
    /// Response packet from server to client
    ClientResponse,
    /// The alternative response packet containing frame extras
    AltClientResponse,
    /// Request packet from server to client
    ServerRequest,
    /// Response packet from client to server
    ServerResponse,
}

impl Default for Magic {
    fn default() -> Self {
        Magic::ClientRequest
    }
}

impl Magic {
    // Test to check if the magic represents one of the available request
    // magics
    pub fn is_request(&self) -> bool {
        matches!(
            self,
            Magic::ClientRequest | Magic::AltClientRequest | Magic::ServerRequest
        )
    }

    /// Test to check if the magic represents one of the available response
    /// magics
    pub fn is_response(&self) -> bool {
        !self.is_request()
    }

    /// Return true if the magic represents one of the client magics.
    pub fn is_client_magic(&self) -> bool {
        matches!(
            self,
            Magic::ClientRequest
                | Magic::AltClientRequest
                | Magic::ClientResponse
                | Magic::AltClientResponse
        )
    }

    /// Return true if the magic represents one of the server magics.
    pub fn is_server_magic(&self) -> bool {
        !self.is_client_magic()
    }

    /// Is the packet using the alternative packet encoding form
    pub fn is_alternative_encoding(&self) -> bool {
        matches!(self, Magic::AltClientRequest | Magic::AltClientResponse)
    }
}

impl From<Magic> for u8 {
    fn from(magic: Magic) -> Self {
        match magic {
            Magic::ClientRequest => 0x80,
            Magic::AltClientRequest => 0x08,
            Magic::ClientResponse => 0x81,
            Magic::AltClientResponse => 0x18,
            Magic::ServerRequest => 0x82,
            Magic::ServerResponse => 0x83,
        }
    }
}

impl TryFrom<u8> for Magic {
    type Error = McbpDecodeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(match value {
            0x80 => Magic::ClientRequest,
            0x08 => Magic::AltClientRequest,
            0x81 => Magic::ClientResponse,
            0x18 => Magic::AltClientResponse,
            0x82 => Magic::ServerRequest,
            0x83 => Magic::ServerResponse,
            _ => return Err(McbpDecodeError::InvalidMagic(value)),
        })
    }
}
