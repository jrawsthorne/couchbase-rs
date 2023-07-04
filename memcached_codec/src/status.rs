/// Definition of the valid response status numbers.
#[derive(Debug, Clone, PartialEq, Eq, Copy, Default)]
pub enum Status {
    /// The operation completed successfully
    #[default]
    Success,

    /// The key does not exist
    KeyNotFound,

    /// The key exists in the cluster (with another CAS value)
    KeyExists,

    /// Invalid request
    InvalidArguments,

    /// The server is not responsible for the requested vbucket
    NotMyVBucket,

    /// Could not authenticate successfully
    AuthenticationError,

    /// An error we don't know about. Use the error map returned from the server to decode the status
    Unknown(u16),
}

impl From<Status> for u16 {
    fn from(status: Status) -> Self {
        match status {
            Status::Success => 0x0000,
            Status::KeyNotFound => 0x0001,
            Status::KeyExists => 0x0002,
            Status::InvalidArguments => 0x0004,
            Status::NotMyVBucket => 0x0007,
            Status::AuthenticationError => 0x0020,
            Status::Unknown(status) => status,
        }
    }
}

impl From<u16> for Status {
    fn from(status: u16) -> Self {
        match status {
            0x0000 => Status::Success,
            0x0001 => Status::KeyNotFound,
            0x0002 => Status::KeyExists,
            0x0004 => Status::InvalidArguments,
            0x0007 => Status::NotMyVBucket,
            0x0020 => Status::AuthenticationError,
            _ => Status::Unknown(status),
        }
    }
}
