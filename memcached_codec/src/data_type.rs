use crate::error::McbpDecodeError;
use bitflags::bitflags;
use std::convert::TryFrom;

bitflags! {
    /// DataType is used to communicate how the client and server should encode and decode a value
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct DataType: u8 {
        const RAW = 0x00;
        const JSON = 0x01;
        const SNAPPY = 0x02;
        const XATTR = 0x04;
    }
}

impl Default for DataType {
    fn default() -> Self {
        DataType::RAW
    }
}

impl TryFrom<u8> for DataType {
    type Error = McbpDecodeError;

    fn try_from(bits: u8) -> Result<Self, Self::Error> {
        DataType::from_bits(bits).ok_or(McbpDecodeError::InvalidDataType(bits))
    }
}

impl From<DataType> for u8 {
    fn from(data_type: DataType) -> Self {
        data_type.bits()
    }
}
