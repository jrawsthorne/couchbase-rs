use crate::{error::McbpDecodeError, magic::Magic};

/// Different types of operations that can be sent
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum Opcode {
    // Client
    Get,
    Upsert,
    Insert,
    Replace,
    Remove,
    Hello,
    SaslListMechs,
    SaslAuth,
    SaslStep,
    SelectBucket,
    GetCollectionsManifest,
    GetCollectionId,
    GetScopeId,
    GetClusterConfig,
    GetErrorMap,

    // Server
    ClusterMapChangeNotification,
}

impl From<Opcode> for u8 {
    fn from(opcode: Opcode) -> Self {
        match opcode {
            // Client
            Opcode::Get => 0x00,
            Opcode::Upsert => 0x01,
            Opcode::Insert => 0x02,
            Opcode::Replace => 0x03,
            Opcode::Remove => 0x04,
            Opcode::Hello => 0x1f,
            Opcode::SaslListMechs => 0x20,
            Opcode::SaslAuth => 0x21,
            Opcode::SaslStep => 0x22,
            Opcode::GetCollectionsManifest => 0xba,
            Opcode::GetCollectionId => 0xbb,
            Opcode::GetScopeId => 0xbc,
            Opcode::GetErrorMap => 0xfe,
            Opcode::SelectBucket => 0x89,
            Opcode::GetClusterConfig => 0xb5,

            // Server
            Opcode::ClusterMapChangeNotification => 0x01,
        }
    }
}

impl Opcode {
    /// Decode the opcode from the raw u8. The magic is requried to differentiate between
    /// client and server opcodes that have the same raw value
    pub fn from_u8(value: u8, magic: Magic) -> Result<Opcode, McbpDecodeError> {
        Ok(if magic.is_client_magic() {
            match value {
                0x00 => Opcode::Get,
                0x01 => Opcode::Upsert,
                0x02 => Opcode::Insert,
                0x03 => Opcode::Replace,
                0x04 => Opcode::Remove,
                0x1f => Opcode::Hello,
                0x20 => Opcode::SaslListMechs,
                0x21 => Opcode::SaslAuth,
                0x22 => Opcode::SaslStep,
                0x89 => Opcode::SelectBucket,
                0xba => Opcode::GetCollectionsManifest,
                0xbb => Opcode::GetCollectionId,
                0xbc => Opcode::GetScopeId,
                0xb5 => Opcode::GetClusterConfig,
                0xfe => Opcode::GetErrorMap,
                _ => return Err(McbpDecodeError::InvalidOpcode(value)),
            }
        } else {
            match value {
                0x01 => Opcode::ClusterMapChangeNotification,
                _ => return Err(McbpDecodeError::InvalidOpcode(value)),
            }
        })
    }

    /// Does the opcode support snappy compression
    pub fn is_compressible(&self) -> bool {
        matches!(self, Opcode::Get | Opcode::Upsert | Opcode::Insert)
    }

    /// Does the provided opcode support durability or not
    pub fn is_durability_supported(&self) -> bool {
        matches!(
            self,
            Opcode::Upsert | Opcode::Insert | Opcode::Replace | Opcode::Remove
        )
    }

    /// Does the provided opcode support reordering
    pub fn is_reorder_supported(&self) -> bool {
        matches!(
            self,
            Opcode::Get | Opcode::Upsert | Opcode::Insert | Opcode::Replace | Opcode::Remove
        )
    }

    /// Does the command carry a key which contains a collection identifier
    pub fn is_collection_command(&self) -> bool {
        matches!(
            self,
            Opcode::Get | Opcode::Upsert | Opcode::Insert | Opcode::Replace | Opcode::Remove
        )
    }

    /// Does the provided opcode support preserving TTL
    pub fn is_preserve_ttl_supported(&self) -> bool {
        matches!(self, Opcode::Upsert | Opcode::Replace)
    }

    /// Does the client write data with this opcode
    pub fn is_client_writing_data(&self) -> bool {
        matches!(self, Opcode::Upsert | Opcode::Insert | Opcode::Replace)
    }
}
