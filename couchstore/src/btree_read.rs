use std::io::Cursor;

use byteorder::ReadBytesExt;
use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::{btree::CouchfileLookupRequest, file_read::pread_compressed, node_types::read_kv};

#[derive(Debug, PartialEq, Eq, Clone, Copy, TryFromPrimitive, IntoPrimitive, Default)]
#[repr(u8)]
pub enum NodeType {
    #[default]
    KPNode = 0,
    KVNode = 1,
}

// TODO: support multiple keys
pub fn btree_lookup_inner(req: &mut CouchfileLookupRequest, diskpos: usize) -> Option<Vec<u8>> {
    let node = pread_compressed(req.file, diskpos);

    let mut cursor = Cursor::new(node.as_ref());

    let node_type = NodeType::try_from_primitive(cursor.read_u8().unwrap()).unwrap();

    match node_type {
        NodeType::KPNode => {
            while (cursor.position() as usize) < node.len() {
                let (cmp_key, value) = read_kv(&mut cursor).unwrap();

                let pointer = (&value[..]).read_u48::<byteorder::BigEndian>().unwrap();

                if &req.key[..] <= cmp_key {
                    // In interior nodes the Value parts of these pairs are pointers to another
                    // B-tree node, where keys less than or equal to that pair's Key will be.
                    return btree_lookup_inner(req, pointer as usize);
                }
            }
            return None;
        }
        NodeType::KVNode => {
            let mut ret = None;
            while (cursor.position() as usize) < node.len() {
                // KV Node
                let (cmp_key, value) = read_kv(&mut cursor).unwrap();

                if &req.key[..] <= cmp_key {
                    if req.key == cmp_key {
                        let pointer = (&value[10..16]).read_u48::<byteorder::BigEndian>().unwrap();

                        let val = pread_compressed(req.file, pointer as usize);
                        ret = Some(val);
                    }
                }
            }
            return ret;
        }
    }
}

pub fn btree_lookup(mut req: CouchfileLookupRequest, root_pointer: usize) -> Option<Vec<u8>> {
    btree_lookup_inner(&mut req, root_pointer)
}
