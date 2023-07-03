use std::io::Cursor;

use byteorder::ReadBytesExt;
use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::{btree::CouchfileLookupRequest, node_types::read_kv, TreeFile};

#[derive(Debug, PartialEq, Eq, Clone, Copy, TryFromPrimitive, IntoPrimitive, Default)]
#[repr(u8)]
pub enum NodeType {
    #[default]
    KPNode = 0,
    KVNode = 1,
}

impl TreeFile {
    // TODO: support multiple keys
    pub fn btree_lookup_inner<F>(
        &mut self,
        req: &mut CouchfileLookupRequest,
        mut on_fetch: F,
        diskpos: usize,
    ) where
        F: FnMut(&mut Self, &mut CouchfileLookupRequest, Option<&[u8]>),
    {
        let node = self.read_compressed(diskpos);

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
                        return self.btree_lookup_inner(req, on_fetch, pointer as usize);
                    }
                }
                on_fetch(self, req, None);
            }
            NodeType::KVNode => {
                while (cursor.position() as usize) < node.len() {
                    // KV Node
                    let (cmp_key, value) = read_kv(&mut cursor).unwrap();

                    if &req.key[..] <= cmp_key {
                        if req.key == cmp_key {
                            on_fetch(self, req, Some(value));
                            return;
                        }
                    }
                }
                on_fetch(self, req, None);
            }
        }
    }

    pub fn btree_lookup<F>(
        &mut self,
        req: &mut CouchfileLookupRequest,
        on_fetch: F,
        root_pointer: usize,
    ) where
        F: Sized + FnMut(&mut Self, &mut CouchfileLookupRequest, Option<&[u8]>),
    {
        self.btree_lookup_inner(req, on_fetch, root_pointer);
    }
}
