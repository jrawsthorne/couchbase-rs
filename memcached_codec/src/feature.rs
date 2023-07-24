use crate::error::McbpDecodeError;

/// Features negotiated during the hello process of bootstrap
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Feature {
    Datatype,

    Tls,

    TcpNodelay,

    MutationSeqno,

    TcpDelay,

    Xattr,

    Xerror,

    /// The server supports selecting between multiple buckets
    SelectBucket,

    /// The server supports snappy compression
    Snappy,

    /// The server support JSON
    Json,

    /// The server supports sending commands to the client
    Duplex,

    ClustermapChangeNotification,

    UnorderedExecution,

    Tracing,

    /// Does the server support alternative request packets
    AltRequestSupport,

    SyncReplication,

    /// Supports scopes and collections
    Collections,

    PreserveTtl,

    VAttr,

    PiTR,

    SubdocCreateAsDeleted,

    SubdocDocumentMacroSupport,

    SubdocReplaceBodyWithXattr,

    ReportUnitUsage,

    SubdocReplicaRead,
}

impl From<Feature> for u16 {
    fn from(feature: Feature) -> Self {
        match feature {
            Feature::Datatype => 0x01,
            Feature::Tls => 0x02,
            Feature::TcpNodelay => 0x03,
            Feature::MutationSeqno => 0x04,
            Feature::TcpDelay => 0x05,
            Feature::Xattr => 0x06,
            Feature::Xerror => 0x07,
            Feature::SelectBucket => 0x08,
            Feature::Snappy => 0x0a,
            Feature::Json => 0x0b,
            Feature::Duplex => 0x0c,
            Feature::ClustermapChangeNotification => 0x0d,
            Feature::UnorderedExecution => 0x0e,
            Feature::Tracing => 0x0f,
            Feature::AltRequestSupport => 0x10,
            Feature::SyncReplication => 0x11,
            Feature::Collections => 0x12,
            Feature::PreserveTtl => 0x14,
            Feature::VAttr => 0x15,
            Feature::PiTR => 0x16,
            Feature::SubdocCreateAsDeleted => 0x17,
            Feature::SubdocDocumentMacroSupport => 0x18,
            Feature::SubdocReplaceBodyWithXattr => 0x19,
            Feature::ReportUnitUsage => 0x1a,
            Feature::SubdocReplicaRead => 0x1c,
        }
    }
}

impl TryFrom<u16> for Feature {
    type Error = McbpDecodeError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(Feature::Datatype),
            0x02 => Ok(Feature::Tls),
            0x03 => Ok(Feature::TcpNodelay),
            0x04 => Ok(Feature::MutationSeqno),
            0x05 => Ok(Feature::TcpDelay),
            0x06 => Ok(Feature::Xattr),
            0x07 => Ok(Feature::Xerror),
            0x08 => Ok(Feature::SelectBucket),
            0x0a => Ok(Feature::Snappy),
            0x0b => Ok(Feature::Json),
            0x0c => Ok(Feature::Duplex),
            0x0d => Ok(Feature::ClustermapChangeNotification),
            0x0e => Ok(Feature::UnorderedExecution),
            0x0f => Ok(Feature::Tracing),
            0x10 => Ok(Feature::AltRequestSupport),
            0x11 => Ok(Feature::SyncReplication),
            0x12 => Ok(Feature::Collections),
            0x14 => Ok(Feature::PreserveTtl),
            0x15 => Ok(Feature::VAttr),
            0x16 => Ok(Feature::PiTR),
            0x17 => Ok(Feature::SubdocCreateAsDeleted),
            0x18 => Ok(Feature::SubdocDocumentMacroSupport),
            0x19 => Ok(Feature::SubdocReplaceBodyWithXattr),
            0x1a => Ok(Feature::ReportUnitUsage),
            0x1c => Ok(Feature::SubdocReplicaRead),
            _ => Err(McbpDecodeError::InvalidFeature(value)),
        }
    }
}
