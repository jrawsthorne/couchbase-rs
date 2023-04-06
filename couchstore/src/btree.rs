use crate::TreeFile;

pub struct CouchfileLookupRequest<'a> {
    pub file: &'a mut TreeFile,
    pub key: Vec<u8>,
}
