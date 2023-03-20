use std::io;

use thiserror::Error;

/// Errors that can be produced by the encoder or decoder
#[derive(Error, Debug)]
pub enum McbpDecodeError {
    #[error("invalid magic ({0})")]
    InvalidMagic(u8),
    #[error("invalid opcode ({0})")]
    InvalidOpcode(u8),
    #[error("invalid data type ({0})")]
    InvalidDataType(u8),
    #[error("invalid feature ({0})")]
    InvalidFeature(u16),
    #[error("missing status")]
    MissingStatus,
    #[error("missing vbucket")]
    MissingVbucket,
    #[error(transparent)]
    Io {
        #[from]
        source: io::Error,
    },
}
