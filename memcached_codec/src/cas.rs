/// A unique value created per mutation of a document.
/// Is not guaranteed to be unique across documents
/// Can be used in a compare and swap loop to safely mutate a document concurrently
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Cas(pub(crate) u64);
