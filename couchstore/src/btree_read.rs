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
        on_fetch: &mut F,
        diskpos: usize,
        mut current: usize,
        end: usize,
    ) where
        F: FnMut(&mut Self, &[u8], Option<&[u8]>),
    {
        if current == end {
            return;
        }

        let node = self.read_compressed(diskpos);

        let mut cursor = Cursor::new(node.as_ref());

        let node_type = NodeType::try_from_primitive(cursor.read_u8().unwrap()).unwrap();

        match node_type {
            NodeType::KPNode => {
                while (cursor.position() as usize) < node.len() && current < end {
                    let (cmp_key, value) = read_kv(&mut cursor).unwrap();

                    if &req.keys[current][..] <= cmp_key {
                        let mut last_item = current;

                        loop {
                            last_item += 1;

                            if last_item >= end {
                                break;
                            }

                            if &req.keys[last_item][..] > cmp_key {
                                break;
                            }
                        }

                        let pointer =
                            (&value[..]).read_u48::<byteorder::BigEndian>().unwrap() as usize;

                        // In interior nodes the Value parts of these pairs are pointers to another
                        // B-tree node, where keys less than or equal to that pair's Key will be.
                        self.btree_lookup_inner(req, on_fetch, pointer, current, last_item);

                        current = last_item;
                    }
                }
            }
            NodeType::KVNode => {
                let mut cmp_key: &[u8] = &[];
                let mut value: &[u8] = &[];
                let mut next_key = true;
                // Try iterating whilst we have 'input' keys, i.e. keys we're looking up
                while current < end {
                    // Only try and read the next-key if requested and we're still in
                    // the node length
                    if next_key && (cursor.position() as usize) < node.len() {
                        (cmp_key, value) = read_kv(&mut cursor).unwrap();
                    } else if next_key {
                        // else if next_key is true and we're out of buf space, break
                        break;
                    }

                    let key = &req.keys[current][..];

                    if key > cmp_key {
                        next_key = true;
                        continue;
                    }

                    if key == cmp_key {
                        on_fetch(self, key, Some(value));
                        next_key = true;
                    } else {
                        on_fetch(self, key, None);
                    }

                    current += 1;
                }
            }
        }

        while current < end {
            on_fetch(self, &req.keys[current], None);
            current += 1;
        }
    }

    pub fn btree_lookup<F>(
        &mut self,
        req: &mut CouchfileLookupRequest,
        mut on_fetch: F,
        root_pointer: usize,
    ) where
        F: Sized + FnMut(&mut Self, &[u8], Option<&[u8]>),
    {
        self.btree_lookup_inner(req, &mut on_fetch, root_pointer, 0, req.keys.len());
    }
}
