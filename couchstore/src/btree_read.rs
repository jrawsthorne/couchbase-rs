use crate::{btree::CouchfileLookupRequest, node_types::read_kv, Db};
use byteorder::ReadBytesExt;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use std::{cmp::Ordering, io::Cursor};

#[derive(Debug, PartialEq, Eq, Clone, Copy, TryFromPrimitive, IntoPrimitive, Default)]
#[repr(u8)]
pub enum NodeType {
    #[default]
    KPNode = 0,
    KVNode = 1,
}

impl Db {
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

        let node = self.file.read_compressed(diskpos);

        let mut cursor = Cursor::new(node.as_ref());

        let node_type = NodeType::try_from_primitive(cursor.read_u8().unwrap()).unwrap();

        match node_type {
            NodeType::KPNode => {
                while (cursor.position() as usize) < node.len() && current < end {
                    let (cmp_key, value) = read_kv(&mut cursor).unwrap();

                    let key = &req.keys[current][..];

                    if (req.compare)(key, cmp_key) == Ordering::Greater {
                        continue;
                    }

                    if req.fold {
                        req.in_fold = true;
                    }

                    let mut last_item = current;

                    loop {
                        last_item += 1;

                        if last_item >= end {
                            break;
                        }

                        if (req.compare)(&req.keys[last_item][..], cmp_key) == Ordering::Greater {
                            break;
                        }
                    }

                    let pointer = (&value[..]).read_u48::<byteorder::BigEndian>().unwrap() as usize;

                    // In interior nodes the Value parts of these pairs are pointers to another
                    // B-tree node, where keys less than or equal to that pair's Key will be.
                    self.btree_lookup_inner(req, on_fetch, pointer, current, last_item);

                    if !req.in_fold {
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

                    let cmp_val = (req.compare)(key, cmp_key);

                    if matches!(cmp_val, Ordering::Less | Ordering::Equal)
                        && req.fold
                        && !req.in_fold
                    {
                        req.in_fold = true;
                    }

                    // in_fold (>= start), requires a compare against end
                    if req.in_fold
                        && current + 1 < end
                        && (req.compare)(&req.keys[current + 1][..], cmp_key) == Ordering::Less
                    {
                        //We've hit a key past the end of our range.
                        req.in_fold = false;
                        req.fold = false;
                        current = end;
                        break;
                    }

                    if cmp_val == Ordering::Greater {
                        next_key = true;
                        continue;
                    }

                    if cmp_val == Ordering::Equal || req.in_fold {
                        on_fetch(self, cmp_key, Some(value));
                    } else {
                        on_fetch(self, key, None);
                    }

                    if !req.in_fold {
                        current += 1;
                        next_key = cmp_val == Ordering::Equal;
                    } else {
                        next_key = true;
                    }
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
        req.in_fold = false;
        self.btree_lookup_inner(req, &mut on_fetch, root_pointer, 0, req.keys.len());
    }
}
