use std::{
    fs::{File, OpenOptions},
    io::{self, Cursor, Seek, SeekFrom, Write},
    path::Path,
};
mod btree;
mod btree_read;
mod constants;
mod file_read;
mod file_write;
mod node_types;
mod utils;

use byteorder::{BigEndian, ReadBytesExt};
use constants::COUCH_BLOCK_SIZE;
use file_read::pread_header;
use node_types::RawFileHeaderV13;
use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::{
    btree::CouchfileLookupRequest, btree_read::btree_lookup, constants::MAX_DB_HEADER_SIZE,
};

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
}

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
    fn decode(mut buf: impl io::Read, root_size: usize) -> Option<NodePointer> {
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
}

#[derive(Debug)]
pub struct TreeFile {
    pos: usize,
    file: File,
}

impl TreeFile {
    pub fn new(file: File) -> TreeFile {
        TreeFile { pos: 0, file }
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

        let mut tree_file = TreeFile::new(file);

        tree_file.pos = tree_file.file.seek(SeekFrom::End(0)).unwrap() as usize;

        let mut db = Db {
            read_only: opts.read_only,
            file: tree_file,
            header: Header::default(),
        };

        if db.file.pos == 0 {
            db.create_header();
        } else {
            db.find_header(db.file.pos - 2);
        }

        db
    }

    pub fn get(&mut self, key: impl AsRef<str>) -> Option<Vec<u8>> {
        let key = key.as_ref();

        let pos = self.header.by_id_root.as_ref().unwrap().pointer as usize;

        let mut key_b = Vec::new();
        key_b.write_all(&[0]).unwrap(); // default collection prefix
        key_b.write_all(key.as_bytes()).unwrap();

        return btree_lookup(
            CouchfileLookupRequest {
                file: &mut self.file,
                key: key_b,
            },
            pos,
        );
    }

    fn find_header(&mut self, start_pos: usize) {
        let mut pos = start_pos;

        pos -= pos % COUCH_BLOCK_SIZE;

        loop {
            self.find_header_at_pos(pos);
            pos -= COUCH_BLOCK_SIZE;
            break; // error handling
        }
    }

    fn find_header_at_pos(&mut self, pos: usize) {
        self.file.file.seek(SeekFrom::Start(pos as u64)).unwrap();
        let disk_block_type = DiskBlockType::try_from(self.file.file.read_u8().unwrap()).unwrap();

        assert_eq!(disk_block_type, DiskBlockType::Header);

        let header_buf = pread_header(&mut self.file, pos, Some(MAX_DB_HEADER_SIZE));

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

        let by_seq_root = NodePointer::decode(&mut cursor, header.seqrootsize as usize);
        let by_id_root = NodePointer::decode(&mut cursor, header.idrootsize as usize);
        let local_docs_root = NodePointer::decode(&mut cursor, header.localrootsize as usize);

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
        let buf = &[];

        let header_pos = file_write::write_header(&mut self.file, buf);
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

pub struct DBOpenOptions {
    /// Create a new empty .couch file if file doesn't exist.
    create: bool,

    /// Open the database in read only mode
    read_only: bool,
}

impl Default for DBOpenOptions {
    fn default() -> Self {
        Self {
            create: true,
            read_only: false,
        }
    }
}
