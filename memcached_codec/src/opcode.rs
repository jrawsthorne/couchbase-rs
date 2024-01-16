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

    Stat,

    // Audit
    AuditPut,
    AuditConfigReload,

    // DCP
    DcpOpenConnection,
    DcpAddStream,
    DcpCloseStream,
    DcpStreamRequest,
    DcpGetFailoverLog,
    DcpStreamEnd,
    DcpSnapshotMarker,
    DcpMutation,
    DcpDeletion,
    DcpExpiration,
    DcpSetVbucketState,
    DcpNoop,
    DcpBufferAcknowledgement,
    DcpControl,
    DcpSystemEvent,
    DcpPrepare,
    DcpSeqnoAcknowledged,
    DcpCommit,
    DcpAbort,
    DcpSeqnoAdvanced,
    DcpOsoSnapshot,

    SetClusterConfig,
    RbacRefresh,
    IsaslRefresh,

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

            Opcode::Stat => 0x10,

            // Audit
            Opcode::AuditPut => 0x27,
            Opcode::AuditConfigReload => 0x28,

            // DCP
            Opcode::DcpOpenConnection => 0x50,
            Opcode::DcpAddStream => 0x51,
            Opcode::DcpCloseStream => 0x52,
            Opcode::DcpStreamRequest => 0x53,
            Opcode::DcpGetFailoverLog => 0x54,
            Opcode::DcpStreamEnd => 0x55,
            Opcode::DcpSnapshotMarker => 0x56,
            Opcode::DcpMutation => 0x57,
            Opcode::DcpDeletion => 0x58,
            Opcode::DcpExpiration => 0x59,
            Opcode::DcpSetVbucketState => 0x5b,
            Opcode::DcpNoop => 0x5c,
            Opcode::DcpBufferAcknowledgement => 0x5d,
            Opcode::DcpControl => 0x5e,
            Opcode::DcpSystemEvent => 0x5f,
            Opcode::DcpPrepare => 0x60,
            Opcode::DcpSeqnoAcknowledged => 0x61,
            Opcode::DcpCommit => 0x62,
            Opcode::DcpAbort => 0x63,
            Opcode::DcpSeqnoAdvanced => 0x64,
            Opcode::DcpOsoSnapshot => 0x65,

            Opcode::SetClusterConfig => 0xb4,
            Opcode::RbacRefresh => 0xf7,
            Opcode::IsaslRefresh => 0xf1,

            // Server
            Opcode::ClusterMapChangeNotification => 0x01,
        }
    }
}

impl Opcode {
    /// Decode the opcode from the raw u8. The magic is requried to differentiate between
    /// client and server opcodes that have the same raw value
    pub fn from_u8(value: u8, magic: Magic) -> Result<Opcode, McbpDecodeError> {
        Ok(match value {
            0x00 => Opcode::Get,
            0x01 => {
                if magic.is_client_magic() {
                    Opcode::Upsert
                } else {
                    Opcode::ClusterMapChangeNotification
                }
            }
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

            0x10 => Opcode::Stat,

            0x27 => Opcode::AuditPut,
            0x28 => Opcode::AuditConfigReload,

            // DCP
            0x50 => Opcode::DcpOpenConnection,
            0x51 => Opcode::DcpAddStream,
            0x52 => Opcode::DcpCloseStream,
            0x53 => Opcode::DcpStreamRequest,
            0x54 => Opcode::DcpGetFailoverLog,
            0x55 => Opcode::DcpStreamEnd,
            0x56 => Opcode::DcpSnapshotMarker,
            0x57 => Opcode::DcpMutation,
            0x58 => Opcode::DcpDeletion,
            0x59 => Opcode::DcpExpiration,
            0x5b => Opcode::DcpSetVbucketState,
            0x5c => Opcode::DcpNoop,
            0x5d => Opcode::DcpBufferAcknowledgement,
            0x5e => Opcode::DcpControl,
            0x5f => Opcode::DcpSystemEvent,
            0x60 => Opcode::DcpPrepare,
            0x61 => Opcode::DcpSeqnoAcknowledged,
            0x62 => Opcode::DcpCommit,
            0x63 => Opcode::DcpAbort,
            0x64 => Opcode::DcpSeqnoAdvanced,
            0x65 => Opcode::DcpOsoSnapshot,

            0xb4 => Opcode::SetClusterConfig,
            0xf7 => Opcode::RbacRefresh,
            0xf1 => Opcode::IsaslRefresh,

            _ => return Err(McbpDecodeError::InvalidOpcode(value)),
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
