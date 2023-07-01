use std::{cmp::Ordering, collections::VecDeque, fmt::Debug, io::Cursor};

use byteorder::WriteBytesExt;

use crate::{
    btree_read::NodeType,
    node_types::{read_kv, write_kv},
    NodePointer, TreeFile,
};

#[derive(Debug)]
pub struct CouchfileModifyResult<'a, Ctx> {
    pub node_type: NodeType,
    pub req: &'a CouchfileModifyRequest<Ctx>,
    pub values: VecDeque<Node>,
    pub node_length: usize,
    pub pointers: VecDeque<Node>,
    pub modified: bool,
    pub compacting: bool,
}

impl<'a, Ctx> CouchfileModifyResult<'a, Ctx> {
    fn new(req: &'a CouchfileModifyRequest<Ctx>) -> Self {
        Self {
            node_type: NodeType::default(),
            req,
            values: VecDeque::new(),
            node_length: 0,
            pointers: VecDeque::new(),
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
        mut root: Option<NodePointer>,
    ) -> Option<NodePointer> {
        let num_actions = req.actions.len();
        let mut root_result = CouchfileModifyResult::new(&req);
        root_result.node_type = NodeType::KPNode;
        self.modify_node(&req, root.as_mut(), 0, num_actions, &mut root_result);

        let mut ret = root;

        if root_result.modified {
            if root_result.values.len() > 1 || !root_result.pointers.is_empty() {
                // The root was split
                // Write it to disk and return the pointer to it.
                ret = self.finish_root(&req, &mut root_result);
            } else {
                ret = root_result.values.back().unwrap().pointer.clone();
            }
        }

        return ret;
    }

    fn finish_root<'a, Ctx: Debug>(
        &mut self,
        req: &'a CouchfileModifyRequest<Ctx>,
        root_result: &'a mut CouchfileModifyResult<'a, Ctx>,
    ) -> Option<NodePointer> {
        let ret;

        let mut collector = CouchfileModifyResult::new(req);

        collector.modified = true;
        collector.node_type = NodeType::KPNode;

        self.flush_mr(root_result);

        loop {
            if root_result.pointers.len() == 1 {
                // The root result split into exactly one kp_node.
                // Return the pointer to it.
                ret = root_result.pointers.back().unwrap().pointer.clone();
                break;
            } else {
                // The root result split into more than one kp_node.
                // Move the pointer list to the value list and write out the new node.
                self.mr_move_pointers(root_result, &mut collector);

                self.flush_mr(&mut collector);

                std::mem::swap(root_result, &mut collector);
            }
        }

        return ret;
    }

    pub fn modify_node<'a, Ctx: Debug>(
        &mut self,
        req: &'a CouchfileModifyRequest<Ctx>,
        node_pointer: Option<&mut NodePointer>,
        mut start: usize,
        end: usize,
        dst: &mut CouchfileModifyResult<'a, Ctx>,
    ) {
        let mut node_buf = Vec::new();

        if let Some(node_pointer) = &node_pointer {
            node_buf = self.read_compressed(node_pointer.pointer as usize);
        }

        let mut cursor = Cursor::new(node_buf.as_ref());

        let mut local_result = CouchfileModifyResult::new(req);

        if node_pointer.is_none() || node_buf[0] == NodeType::KVNode as u8 {
            cursor.set_position(1);

            // KV Node
            local_result.node_type = NodeType::KVNode;

            while (cursor.position() as usize) < node_buf.len() {
                let (cmp_key, value) = read_kv(&mut cursor).unwrap();

                let mut advance = false;

                while !advance && start < end {
                    advance = true;
                    if &req.actions[start].key[..] < &cmp_key[..] {
                        //Key less than action key
                        self.maybe_purge_kv(&req, &cmp_key, &value, &mut local_result);
                    } else if &req.actions[start].key[..] > &cmp_key[..] {
                        //Key greater than action key
                        local_result.modified = true;
                        self.mr_push_item(
                            &req.actions[start].key[..],
                            &req.actions[start].data.as_ref().unwrap()[..],
                            &mut local_result,
                        );

                        start += 1;
                        advance = false;
                    } else {
                        //Node key is equal to action key
                        local_result.modified = true;
                        self.mr_push_item(
                            &req.actions[start].key[..],
                            &req.actions[start].data.as_ref().unwrap()[..],
                            &mut local_result,
                        );
                        start += 1;
                    }
                }
                if start == end && !advance {
                    self.maybe_purge_kv(req, &cmp_key, &value, &mut local_result)
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
                        self.mr_push_item(
                            &req.actions[start].key,
                            &req.actions[start].data.as_ref().unwrap(),
                            &mut local_result,
                        );
                    }
                    _ => {}
                }
                start += 1;
            }
        } else if node_buf[0] == 0 {
            cursor.set_position(1);

            // KP Node
            local_result.node_type = NodeType::KPNode;
            while (cursor.position() as usize) < node_buf.len() && start < end {
                let (cmp_key, value) = read_kv(&mut cursor).unwrap();
                if cursor.position() as usize == node_buf.len() {
                    //We're at the last item in the kpnode, must apply all our
                    //actions here.
                    let mut desc = NodePointer::read_pointer(cmp_key, value);

                    self.modify_node(req, Some(&mut desc), start, end, &mut local_result);

                    break;
                }
                match cmp_key.cmp(&req.actions[start].key) {
                    Ordering::Less => {
                        //Key in node item less than action item and not at end
                        //position, so just add it and continue.
                        let add = NodePointer::read_pointer(cmp_key, value);

                        self.maybe_purge_kp(req, add, &mut local_result);
                    }
                    Ordering::Equal | Ordering::Greater => {
                        let mut range_end = start;

                        while range_end < end && &req.actions[start].key[..] <= cmp_key {
                            range_end += 1;
                        }

                        let mut desc = NodePointer::read_pointer(cmp_key, value);

                        self.modify_node(req, Some(&mut desc), start, range_end, &mut local_result);
                    }
                }
            }
            while (cursor.position() as usize) < node_buf.len() {
                let (cmp_key, value) = read_kv(&mut cursor).unwrap();
                let add = NodePointer::read_pointer(cmp_key, value);

                self.maybe_purge_kp(req, add, &mut local_result);
            }
        } else {
            panic!("Invalid node type");
        }

        self.flush_mr(&mut local_result);

        if !local_result.modified && node_pointer.is_some() {
            self.mr_push_pointerinfo(node_pointer.cloned().unwrap(), dst);
        } else {
            dst.modified = true;
            self.mr_move_pointers(&mut local_result, dst)
        }
    }

    fn mr_push_pointerinfo<Ctx: Debug>(
        &mut self,
        ptr: NodePointer,
        dst: &mut CouchfileModifyResult<Ctx>,
    ) {
        let mut data = Vec::new();
        ptr.encode_pointer(&mut data).unwrap();

        let raw_ptr = Node {
            data,
            key: ptr.key.as_ref().cloned().unwrap_or_default(),
            pointer: Some(ptr),
        };

        dst.node_length += raw_ptr.key.len() + raw_ptr.data.len() + 5;
        dst.values.push_back(raw_ptr);

        self.maybe_flush(dst);
    }

    fn mr_move_pointers<Ctx: Debug>(
        &mut self,
        src: &mut CouchfileModifyResult<Ctx>,
        dst: &mut CouchfileModifyResult<Ctx>,
    ) {
        while let Some(val) = src.pointers.pop_back() {
            dst.node_length += val.data.len() + val.key.len() + 5;
            dst.values.push_back(val);
            self.maybe_flush(dst);
        }
    }

    pub fn mr_push_item<Ctx: Debug>(
        &mut self,
        key: &[u8],
        value: &[u8],
        result: &mut CouchfileModifyResult<Ctx>,
    ) {
        result.values.push_back(Node {
            data: value.to_vec(),
            key: key.to_vec(),
            pointer: None,
        });
        result.node_length += key.len() + value.len() + 5; // key + value + 48 bit packed key + value length
        self.maybe_flush(result);
    }

    pub fn maybe_purge_kv<Ctx: Debug>(
        &mut self,
        req: &CouchfileModifyRequest<Ctx>,
        key: &[u8],
        value: &[u8],
        result: &mut CouchfileModifyResult<Ctx>,
    ) {
        // TODO: Support purging???

        self.mr_push_item(key, value, result)
    }

    pub fn maybe_purge_kp<Ctx: Debug>(
        &mut self,
        req: &CouchfileModifyRequest<Ctx>,
        node: NodePointer,
        result: &mut CouchfileModifyResult<Ctx>,
    ) {
        // TODO: Support purging???

        self.mr_push_pointerinfo(node, result);
    }
}

impl TreeFile {
    pub fn maybe_flush<Ctx: Debug>(&mut self, result: &mut CouchfileModifyResult<Ctx>) {
        if result.compacting {
            todo!()
        } else if result.modified && result.values.len() > 3 {
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
    pub fn flush_mr<Ctx: Debug>(&mut self, result: &mut CouchfileModifyResult<Ctx>) {
        self.flush_mr_partial(result, result.node_length)
    }

    /// Write a node using enough items from the values list to create a node
    /// with uncompressed size of at least mr_quota
    pub fn flush_mr_partial<Ctx: Debug>(
        &mut self,
        result: &mut CouchfileModifyResult<Ctx>,
        mr_quota: usize,
    ) {
        if result.values.is_empty() || !result.modified {
            return;
        }

        let mut nodebuf = Vec::with_capacity(result.node_length + 1);

        nodebuf.write_u8(result.node_type.into()).unwrap();

        let mut diskpos = 0;
        let mut subtreesize = 0;
        let mut disksize = 0;
        let mut item_count = 0;
        let mut final_key = Vec::new();

        let mut mr_quota = mr_quota as isize;

        while let Some(value) = result.values.pop_front() {
            if mr_quota > 0 || (item_count >= 2 && result.node_type == NodeType::KPNode) {
                write_kv(&mut nodebuf, &value.key, &value.data);

                if let Some(pointer) = &value.pointer {
                    subtreesize += pointer.subtree_size
                }

                mr_quota -= (value.key.len() + value.data.len() + 5) as isize;
                final_key = value.key.clone();
                item_count += 1;
            } else {
                break;
            }
        }

        self.db_write_buf_compressed(&nodebuf, &mut diskpos, &mut disksize);

        let ptr = NodePointer {
            pointer: diskpos as u64,
            subtree_size: u64::from(disksize) + subtreesize,
            key: Some(final_key.clone()),
            reduce_value: vec![],
        };

        let mut data = Vec::new();

        ptr.encode_pointer(&mut data).unwrap();

        let raw_ptr = Node {
            data,
            key: final_key,
            pointer: Some(ptr),
        };

        result.node_length -= nodebuf.len() - 1;
        result.pointers.push_back(raw_ptr);
    }
}
