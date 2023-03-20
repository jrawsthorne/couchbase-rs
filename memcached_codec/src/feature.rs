use crate::error::McbpDecodeError;

/// Features negotiated during the hello process of bootstrap
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Feature {
    /// The server supports selecting between multiple buckets
    SelectBucket,

    /// The server supports snappy compression
    Snappy,

    /// The server support JSON
    Json,

    /// The server supports sending commands to the client
    Duplex,

    /// Does the server support alternative request packets
    AltRequestSupport,

    /// Supports scopes and collections
    Collections,
}

impl From<Feature> for u16 {
    fn from(feature: Feature) -> Self {
        match feature {
            Feature::SelectBucket => 0x08,
            Feature::Snappy => 0x0a,
            Feature::Json => 0x0b,
            Feature::Duplex => 0x0c,
            Feature::AltRequestSupport => 0x10,
            Feature::Collections => 0x12,
        }
    }
}

impl TryFrom<u16> for Feature {
    type Error = McbpDecodeError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        Ok(match value {
            0x08 => Feature::SelectBucket,
            0x0a => Feature::Snappy,
            0x0b => Feature::Json,
            0x0c => Feature::Duplex,
            0x10 => Feature::AltRequestSupport,
            0x12 => Feature::Collections,
            _ => return Err(McbpDecodeError::InvalidFeature(value)),
        })
    }
}
