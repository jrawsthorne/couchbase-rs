use std::{fmt::Debug, io::Cursor};

use byteorder::WriteBytesExt;

use crate::{btree_read::NodeType, node_types::read_kv, NodePointer, TreeFile};

#[derive(Debug)]
pub struct CouchfileModifyResult<'a, Ctx> {
    pub node_type: NodeType,
    pub req: &'a CouchfileModifyRequest<Ctx>,
    pub values: Vec<Node>,
    pub node_length: usize,
    pub count: usize,
    pub pointers: Vec<Node>,
    pub modified: bool,
    pub compacting: bool,
}

impl<'a, Ctx> CouchfileModifyResult<'a, Ctx> {
    fn new(req: &'a CouchfileModifyRequest<Ctx>) -> Self {
        Self {
            node_type: NodeType::default(),
            req,
            values: Vec::new(),
            node_length: 0,
            count: 0,
            pointers: Vec::new(),
            modified: false,
            compacting: false,
        }
    }
}

trait Modifier: Sized {
    fn on_fetch(&mut self, req: CouchfileModifyRequest<Self>, key: &[u8], value: &[u8]);
}

#[derive(Debug)]
pub struct UpdateIdContext {
    pub seq_actions: Vec<CouchfileModifyAction>,
}
pub struct UpdateSeqContext {}

impl Modifier for UpdateIdContext {
    fn on_fetch(&mut self, req: CouchfileModifyRequest<Self>, key: &[u8], value: &[u8]) {
        let old_seq = value[0..6].to_vec();

        self.seq_actions.push(CouchfileModifyAction {
            key: old_seq,
            data: None,
            action_type: CouchfileModifyActionType::Remove,
        });
    }
}

#[derive(Default, Debug)]
pub struct CouchfileModifyRequest<Ctx> {
    pub actions: Vec<CouchfileModifyAction>,
    pub context: Ctx,
    pub kv_chunk_threshold: usize,
    pub kp_chunk_threshold: usize,
}

#[derive(Debug)]
pub struct Node {
    pub data: Vec<u8>,
    pub key: Vec<u8>,
    pub pointer: Option<NodePointer>,
}

#[derive(Debug)]
pub struct CouchfileModifyAction {
    pub key: Vec<u8>,
    pub data: Option<Vec<u8>>,
    pub action_type: CouchfileModifyActionType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CouchfileModifyActionType {
    Fetch,
    Remove,
    Insert,
    FetchInsert,
}

impl TreeFile {
    pub fn modify_btree<Ctx: Debug>(
        &mut self,
        req: CouchfileModifyRequest<Ctx>,
        root: Option<NodePointer>,
    ) -> Option<NodePointer> {
        let num_actions = req.actions.len();
        let root_result = self.modify_node(&req, root.clone(), 0, num_actions);

        let mut ret = root;

        dbg!(&root_result);

        if root_result.modified {
            if root_result.count > 1 || !root_result.pointers.is_empty() {
                //The root was split
                //Write it to disk and return the pointer to it.
            } else {
                ret = root_result.pointers.last().unwrap().pointer.clone();
            }
        }

        return ret;
    }

    pub fn modify_node<'a, Ctx: Debug>(
        &mut self,
        req: &'a CouchfileModifyRequest<Ctx>,
        mut node_pointer: Option<NodePointer>,
        mut start: usize,
        end: usize,
    ) -> CouchfileModifyResult<'a, Ctx> {
        let mut node_buf = Vec::new();

        if let Some(node_pointer) = &node_pointer {
            node_buf = self.pread_compressed(node_pointer.pointer as usize);
        }

        let mut cursor = Cursor::new(node_buf.as_ref());

        let mut local_result = CouchfileModifyResult::new(req);

        if node_pointer.is_none() || node_buf[0] == 1 {
            // KV Node
            local_result.node_type = NodeType::KVNode;

            while (cursor.position() as usize) < node_buf.len() {
                let (key, value) = read_kv(&mut cursor).unwrap();

                let advance = 1;

                // let pointer = (&value[10..16]).read_u48::<byteorder::BigEndian>().unwrap();

                if &req.actions[start].key[..] < key { //Key less than action key
                } else if &req.actions[start].key[..] > key { //Key greater than action key
                } else { //Node key is equal to action key
                }
            }
            while start < end {
                let action_type = req.actions[start].action_type;
                if matches!(
                    action_type,
                    CouchfileModifyActionType::Fetch | CouchfileModifyActionType::FetchInsert
                ) {
                    // not found to fetch callback
                }
                match req.actions[start].action_type {
                    CouchfileModifyActionType::Remove => {
                        local_result.modified = true;
                    }
                    CouchfileModifyActionType::Insert | CouchfileModifyActionType::FetchInsert => {
                        local_result.modified = true;
                        mr_push_item(
                            &req.actions[start].key,
                            &req.actions[start].data.as_ref().unwrap(),
                            &mut local_result,
                        );
                    }
                    _ => {}
                }
                start += 1;
            }
        } else if node_buf[0] == 0 { // KP Node
        } else {
            panic!("Invalid node type");
        }

        dbg!(&local_result);

        self.flush_mr(&mut local_result);

        local_result
    }
}

pub fn maybe_pure_kv<Ctx>(
    req: &mut CouchfileModifyRequest<Ctx>,
    key: &[u8],
    value: &[u8],
    result: &mut CouchfileModifyResult<Ctx>,
) {
    // TODO: Support purging???

    mr_push_item(key, value, result)
}

pub fn mr_push_item<Ctx>(key: &[u8], value: &[u8], result: &mut CouchfileModifyResult<Ctx>) {
    result.values.push(Node {
        data: value.to_vec(),
        key: key.to_vec(),
        pointer: None,
    });
    result.count += 1;
    result.node_length += key.len() + value.len() + 5; // key + value + 48 bit packed key + value length
}

impl TreeFile {
    pub fn maybe_flush<Ctx>(&mut self, result: &mut CouchfileModifyResult<Ctx>) {
        if result.compacting {
            todo!()
        } else if result.modified && result.count > 3 {
            let threshold = match result.node_type {
                NodeType::KVNode => result.req.kv_chunk_threshold,
                NodeType::KPNode => result.req.kp_chunk_threshold,
            };
            if result.node_length > threshold {
                let quota = threshold * 2 / 3;
                self.flush_mr_partial(result, quota);
            }
        }
    }

    /// Write the current contents of the values list to disk as a node
    /// and add the resulting pointer to the pointers list.
    pub fn flush_mr<Ctx>(&mut self, result: &mut CouchfileModifyResult<Ctx>) {
        self.flush_mr_partial(result, result.node_length)
    }

    /// Write a node using enough items from the values list to create a node
    /// with uncompressed size of at least mr_quota
    pub fn flush_mr_partial<Ctx>(
        &mut self,
        result: &mut CouchfileModifyResult<Ctx>,
        mut mr_quota: usize,
    ) {
        if result.values.is_empty() || !result.modified {
            return;
        }

        let mut nodebuf = Vec::with_capacity(result.node_length + 1);

        nodebuf.write_u8(result.node_type.into()).unwrap();

        let mut diskpos = 0;
        let mut disksize = 0;
        let mut item_count = 0;
        let mut final_key = Vec::new();

        let mut values_iter = result.values.iter();

        while let Some(value) = values_iter.next() {}

        for value in result.values.iter() {
            if mr_quota > 0 || (item_count >= 2 && result.node_type == NodeType::KPNode) {
                mr_quota -= value.key.len() + value.data.len() + 5;
                final_key = value.key.clone();
                result.count -= 1;
                item_count += 1;
            }
            break;
        }

        self.db_write_buf_compressed(&nodebuf, &mut diskpos, &mut disksize);
    }
}
