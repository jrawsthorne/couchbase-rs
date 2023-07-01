use std::{
    fs::{File, OpenOptions},
    io::{self, Cursor, Seek, SeekFrom, Write},
    path::Path,
};
mod btree;
mod btree_modify;
mod btree_read;
mod constants;
mod file_read;
mod file_write;
mod node_types;
mod save;
mod utils;

use btree_modify::{CouchfileModifyAction, CouchfileModifyActionType, CouchfileModifyRequest};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use constants::COUCH_BLOCK_SIZE;
use node_types::RawFileHeaderV13;
use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::{btree::CouchfileLookupRequest, constants::MAX_DB_HEADER_SIZE};

#[derive(Debug, Clone, Copy, IntoPrimitive, TryFromPrimitive, PartialEq, Eq)]
#[repr(u8)]
pub enum DiskBlockType {
    Data = 0x00,
    Header = 0x01,
}

#[derive(Debug)]
pub struct Db {
    read_only: bool,
    file: TreeFile,
    header: Header,
    opts: DBOpenOptions,
}

pub struct TreeFileOptions {}

#[derive(Debug, Clone, Default)]
pub struct Header {
    disk_version: DiskVersion,
    update_seq: u64,
    by_id_root: Option<NodePointer>,
    by_seq_root: Option<NodePointer>,
    local_docs_root: Option<NodePointer>,
    purge_seq: u64,
    purge_ptr: u64,
    position: u64,
    timestamp: u64,
}

impl Header {
    fn reset(&mut self) {
        self.by_id_root = None;
        self.by_seq_root = None;
        self.local_docs_root = None;
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
pub enum DiskVersion {
    Eleven = 11,
    Twelve = 12,
    #[default]
    Thirteen = 13,
}

#[derive(Debug, Clone)]
pub struct NodePointer {
    key: Option<Vec<u8>>,
    pointer: u64,
    reduce_value: Vec<u8>,
    subtree_size: u64,
}

impl NodePointer {
    fn read_root(mut buf: impl io::Read, root_size: usize) -> Option<NodePointer> {
        if root_size == 0 {
            return None;
        }
        let position = buf.read_u48::<BigEndian>().unwrap();
        let subtree_size = buf.read_u48::<BigEndian>().unwrap();

        let redsize = if root_size > 0 { root_size - 12 } else { 0 };

        let mut reduce_value = vec![0; redsize];
        buf.read_exact(&mut reduce_value).unwrap();

        Some(NodePointer {
            key: None,
            pointer: position,
            reduce_value,
            subtree_size,
        })
    }

    fn read_pointer(key: &[u8], mut buf: impl io::Read) -> NodePointer {
        let pointer = buf.read_u48::<BigEndian>().unwrap();
        let subtree_size = buf.read_u48::<BigEndian>().unwrap();
        let reduce_value_len = buf.read_u16::<BigEndian>().unwrap() as usize;
        let mut reduce_value = vec![0; reduce_value_len];
        buf.read_exact(&mut reduce_value).unwrap();

        NodePointer {
            key: Some(key.to_vec()),
            pointer,
            reduce_value,
            subtree_size,
        }
    }

    fn encode_root(&self, mut buf: impl io::Write) -> io::Result<()> {
        buf.write_u48::<BigEndian>(self.pointer)?;
        buf.write_u48::<BigEndian>(self.subtree_size)?;
        buf.write_all(&self.reduce_value)?;
        Ok(())
    }

    fn encode_pointer(&self, mut buf: impl io::Write) -> io::Result<()> {
        buf.write_u48::<BigEndian>(self.pointer)?;
        buf.write_u48::<BigEndian>(self.subtree_size)?;
        buf.write_u16::<BigEndian>(self.reduce_value.len() as u16)?;
        buf.write_all(&self.reduce_value)?;
        Ok(())
    }
}

pub struct Doc {
    pub id: Vec<u8>,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct DocInfo {
    /// Document ID (key)
    pub id: Vec<u8>,

    /// Sequence number in database
    pub db_seq: u64,

    /// Revision number of document
    pub rev_seq: u64,

    /// Revision metadata; uninterpreted by CouchStore.
    /// Needs to be kept small enough to fit in a B-tree index.
    pub rev_meta: Vec<u8>,

    /// Is this a deleted revision?
    pub deleted: bool,

    /// Content metadata flags
    pub content_meta: ContentMetaFlag,

    /// Byte offset of document data in file
    pub bp: u64,

    /// Physical space occupied by data (*not* its length)
    pub physical_size: u32,
}

bitflags! {
    /// DataType is used to communicate how the client and server should encode and decode a value
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct SaveOptions: u64 {
         /// Snappy compress document data if the high bit of the
    /// content_meta field of the DocInfo is set. This is NOT the
    /// default, and if this is not set the data field of the Doc will
    /// be written to disk as-is, regardless of the content_meta flags.
        const COMPRESS_DOC_BODIES = 1;

         /// Store the DocInfo's passed in db_seq as is.
    ///
    /// Couchstore will *not* assign it a new sequence number, but store the
    /// sequence number as given. The update_seq for the DB will be set to
    /// at least this sequence.
        const SEQUENCE_AS_IS = 2;
    }
}

use bitflags::bitflags;
use std::convert::TryFrom;

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct ContentMetaFlag: u8 {
        /// Document contents compressed via Snappy
        const IS_COMPRESSED = 128;

        /// Document is valid JSON data
        const IS_JSON = 0;

        /// Document was checked, and was not valid JSON
        const INVALID_JSON = 1;

        /// Document was checked, and contained reserved keys,
        /// was not inserted as JSON.
        const INVALID_JSON_KEY = 2;

         /// Document was not checked (DB running in non-JSON mode)
        const NON_JSON_MODE = 3;
    }
}

#[derive(Debug)]
pub struct TreeFile {
    pos: usize,
    file: File,
    options: DBOpenOptions,
}

impl TreeFile {
    pub fn new(file: File, options: DBOpenOptions) -> TreeFile {
        TreeFile {
            pos: 0,
            file,
            options,
        }
    }
}

const ROOT_BASE_SIZE: usize = 12;

impl Db {
    pub fn open(filename: impl AsRef<Path>, opts: DBOpenOptions) -> Db {
        let file = OpenOptions::new()
            .read(true)
            .write(!opts.read_only)
            .create(opts.create)
            .open(filename)
            .unwrap();

        let mut tree_file = TreeFile::new(file, opts);

        tree_file.pos = tree_file.file.seek(SeekFrom::End(0)).unwrap() as usize;

        let mut db = Db {
            read_only: opts.read_only,
            file: tree_file,
            header: Header::default(),
            opts,
        };

        if db.file.pos == 0 {
            db.create_header();
        } else {
            db.find_header(db.file.pos - 2);
        }

        db
    }

    pub fn set(&mut self, key: impl AsRef<str>, value: Vec<u8>) {
        let key = key.as_ref();

        let mut key_b = Vec::new();
        key_b.write_all(&[0]).unwrap(); // default collection prefix
        key_b.write_all(key.as_bytes()).unwrap();

        let doc = Doc {
            id: key_b.clone(),
            data: value.clone(),
        };

        let physical_size = value.len() as u32;

        let doc_info = DocInfo {
            id: key_b,
            db_seq: 0,
            rev_seq: 0,
            rev_meta: vec![],
            deleted: false,
            content_meta: ContentMetaFlag::IS_JSON | ContentMetaFlag::IS_COMPRESSED,
            bp: 0,
            physical_size,
        };

        self.couchstore_save_document(Some(doc), doc_info, SaveOptions::COMPRESS_DOC_BODIES);

        // TODO: separate commit, this is just for testing
        self.write_header();
    }

    pub fn get(&mut self, key: impl AsRef<str>) -> Option<Vec<u8>> {
        let key = key.as_ref();

        let pos = self.header.by_id_root.as_ref()?.pointer as usize;

        let mut key_b = Vec::new();
        key_b.write_all(&[0]).unwrap(); // default collection prefix
        key_b.write_all(key.as_bytes()).unwrap();

        return self
            .file
            .btree_lookup(CouchfileLookupRequest { key: key_b }, pos);
    }

    fn find_header(&mut self, start_pos: usize) {
        let mut pos = start_pos;

        pos -= pos % COUCH_BLOCK_SIZE;

        loop {
            self.find_header_at_pos(pos);
            // pos -= COUCH_BLOCK_SIZE;
            break; // error handling
        }
    }

    fn find_header_at_pos(&mut self, pos: usize) {
        self.file.file.seek(SeekFrom::Start(pos as u64)).unwrap();
        let disk_block_type = DiskBlockType::try_from(self.file.file.read_u8().unwrap()).unwrap();

        assert_eq!(disk_block_type, DiskBlockType::Header);

        let header_buf = self.file.read_header(pos, MAX_DB_HEADER_SIZE);

        let mut cursor = Cursor::new(&header_buf[..]);

        let header = RawFileHeaderV13::decode(&mut cursor);

        assert!(header.purge_ptr <= pos as u64);
        assert_eq!(
            header_buf.len(),
            RawFileHeaderV13::ON_DISK_SIZE
                + (header.seqrootsize as usize)
                + (header.idrootsize as usize)
                + (header.localrootsize as usize)
        );

        let by_seq_root = NodePointer::read_root(&mut cursor, header.seqrootsize as usize);
        let by_id_root = NodePointer::read_root(&mut cursor, header.idrootsize as usize);
        let local_docs_root = NodePointer::read_root(&mut cursor, header.localrootsize as usize);

        self.header.update_seq = header.update_seq;
        self.header.by_id_root = by_id_root;
        self.header.by_seq_root = by_seq_root;
        self.header.local_docs_root = local_docs_root;
        self.header.purge_seq = header.purge_seq;
        self.header.purge_ptr = header.purge_ptr;
        self.header.position = pos as u64;
        self.header.timestamp = header.timestamp;
    }

    fn create_header(&mut self) {
        self.header.disk_version = DiskVersion::Thirteen;
        self.header.update_seq = 0;
        self.header.by_id_root = None;
        self.header.by_seq_root = None;
        self.header.local_docs_root = None;
        self.header.purge_seq = 0;
        self.header.purge_ptr = 0;
        self.header.position = 0;
        self.header.timestamp = 0;

        self.write_header();
    }

    fn write_header(&mut self) {
        let (totalsize, seqrootsize, idrootsize, localrootsize) = self.calculate_header_size();

        let mut b = Vec::with_capacity(totalsize);

        b.write_u8(self.header.disk_version.into()).unwrap();
        b.write_u48::<BigEndian>(self.header.update_seq).unwrap();
        b.write_u48::<BigEndian>(self.header.purge_seq).unwrap();
        b.write_u48::<BigEndian>(self.header.purge_ptr).unwrap();
        b.write_u16::<BigEndian>(seqrootsize as u16).unwrap();
        b.write_u16::<BigEndian>(idrootsize as u16).unwrap();
        b.write_u16::<BigEndian>(localrootsize as u16).unwrap();
        b.write_u64::<BigEndian>(self.header.timestamp).unwrap();
        if let Some(by_seq_root) = &self.header.by_seq_root {
            by_seq_root.encode_root(&mut b).unwrap();
        }
        if let Some(by_id_root) = &self.header.by_id_root {
            by_id_root.encode_root(&mut b).unwrap();
        }
        if let Some(local_docs_root) = &self.header.local_docs_root {
            local_docs_root.encode_root(&mut b).unwrap();
        }

        let header_pos = self.file.write_header(&b);
        self.header.position = header_pos as u64;
    }

    fn calculate_header_size(&self) -> (usize, usize, usize, usize) {
        let mut seqrootsize = 0;
        let mut idrootsize = 0;
        let mut localrootsize = 0;

        if let Some(by_seq_root) = &self.header.by_seq_root {
            seqrootsize = ROOT_BASE_SIZE + by_seq_root.reduce_value.len();
        }

        if let Some(by_id_root) = &self.header.by_id_root {
            idrootsize = ROOT_BASE_SIZE + by_id_root.reduce_value.len();
        }
        if let Some(local_docs_root) = &self.header.local_docs_root {
            localrootsize = ROOT_BASE_SIZE + local_docs_root.reduce_value.len();
        }

        let total = RawFileHeaderV13::ON_DISK_SIZE + seqrootsize + idrootsize + localrootsize;

        (total, seqrootsize, idrootsize, localrootsize)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct DBOpenOptions {
    /// Create a new empty .couch file if file doesn't exist.
    create: bool,

    /// Open the database in read only mode
    read_only: bool,

    kv_chunk_threshold: usize,

    kp_chunk_threshold: usize,
}

impl Default for DBOpenOptions {
    fn default() -> Self {
        Self {
            create: true,
            read_only: false,
            kv_chunk_threshold: 1279,
            kp_chunk_threshold: 1279,
        }
    }
}
