use std::io::Cursor;

use crate::{
    btree_modify::{
        CouchfileModifyAction, CouchfileModifyActionType, CouchfileModifyRequest, UpdateIdContext,
    },
    file_write::{db_write_buf, db_write_buf_compressed},
    ContentMetaFlag, Db, Doc, DocInfo, SaveOptions,
};

// const RAW_SEQ_SIZE: usize = 6;

// // raw_kv_length + bp + rev_seq + content_meta
// const RAW_SEQ_INDEX_VALUE_SIZE: usize = 5 + 6 + 6 + 1;

// // db_seq + pythsical_size + bp + rev_seq + content_meta
// const RAW_ID_INDEX_VALUE_SIZE: usize = 6 + 4 + 6 + 6 + 1;

pub struct ModifyResult {}

impl Db {
    fn couchstore_save_document(&mut self, doc: Option<Doc>, info: DocInfo, options: SaveOptions) {
        self.save_documents_and_callback(doc.map(|doc| vec![doc]), vec![info], options);
    }

    fn save_documents_and_callback(
        &mut self,
        mut docs: Option<Vec<Doc>>,
        mut infos: Vec<DocInfo>,
        options: SaveOptions,
    ) {
        // let mut term_meta_size = 0;

        // for info in &infos {
        //     term_meta_size += RAW_SEQ_SIZE;
        //     term_meta_size += seq_index_raw_value_size(info);
        //     term_meta_size += id_index_raw_value_size(info);
        // }

        // TODO: Reduce allocations, couchstore uses 1 buffer for all the data
        let mut ids: Vec<Vec<u8>> = Vec::new();
        let mut seqs: Vec<u64> = Vec::new();
        let mut id_idx: Vec<Vec<u8>> = Vec::new();
        let mut seq_idx: Vec<Vec<u8>> = Vec::new();

        let mut seq = self.header.update_seq;

        for i in 0..infos.len() {
            let info = &mut infos[i];
            let mut doc = None;

            if options.contains(SaveOptions::SEQUENCE_AS_IS) {
                seq = info.db_seq;
            } else {
                seq += 1;
                info.db_seq = seq;
            }

            if let Some(docs) = &docs {
                doc = Some(&docs[i])
            }

            self.add_doc_to_update_list(
                doc,
                info,
                &mut seqs,
                &mut ids,
                &mut seq_idx,
                &mut id_idx,
                options,
            );
        }

        self.update_indexes(seqs, ids, seq_idx, id_idx, infos.len());

        self.header.update_seq = seq;
    }

    fn add_doc_to_update_list(
        &mut self,
        doc: Option<&Doc>,
        info: &DocInfo,
        seqs: &mut Vec<u64>,
        ids: &mut Vec<Vec<u8>>,
        seq_idx: &mut Vec<Vec<u8>>,
        id_idx: &mut Vec<Vec<u8>>,
        mut options: SaveOptions,
    ) {
        let mut updated = info.clone();

        seqs.push(updated.db_seq);

        if let Some(doc) = doc {
            let mut disk_size = 0;

            // Don't compress a doc unless the meta flag is set
            if !info.content_meta.contains(ContentMetaFlag::IS_COMPRESSED) {
                options.remove(SaveOptions::COMPRESS_DOC_BODIES);
            }

            self.write_doc(doc, &mut updated.bp, &mut disk_size, options);

            updated.physical_size = disk_size;
        } else {
            updated.deleted = true;
            updated.bp = 0;
            updated.physical_size = 0;
        }

        ids.push(updated.id.clone());

        let mut seq_index_value_vec = Vec::new();
        let mut id_index_value_vec = Vec::new();

        let mut seq_index_value = Cursor::new(&mut seq_index_value_vec[..]);
        let mut id_index_value = Cursor::new(&mut id_index_value_vec[..]);

        updated.encode_id_index_value(&mut id_index_value);
        updated.encode_seq_index_value(&mut seq_index_value);

        id_idx.push(id_index_value_vec);
        seq_idx.push(seq_index_value_vec);
    }

    fn update_indexes(
        &mut self,
        seqs: Vec<u64>,
        ids: Vec<Vec<u8>>,
        seq_idx: Vec<Vec<u8>>,
        id_idx: Vec<Vec<u8>>,
        num_docs: usize,
    ) {
        let mut id_keys_and_data = ids.into_iter().zip(id_idx.into_iter()).collect::<Vec<_>>();
        id_keys_and_data.sort_unstable_by(|(key_a, _), (key_b, _)| key_a.cmp(key_b));

        let id_actions = id_keys_and_data
            .into_iter()
            .map(|(key, data)| CouchfileModifyAction {
                key,
                data: Some(data),
                action_type: CouchfileModifyActionType::FetchInsert,
            })
            .collect::<Vec<_>>();

        let id_req = CouchfileModifyRequest {
            actions: id_actions,
            context: UpdateIdContext {
                seq_actions: vec![],
            },
        };

        self.file
            .modify_btree(id_req, self.header.by_id_root.clone());
    }

    fn write_doc(&mut self, doc: &Doc, bp: &mut u64, disk_size: &mut u32, options: SaveOptions) {
        if options.contains(SaveOptions::COMPRESS_DOC_BODIES) {
            db_write_buf_compressed(&self.file, &doc.data, bp, disk_size)
        } else {
            db_write_buf(&self.file, &doc.data, bp, disk_size)
        }
    }
}

// fn seq_index_raw_value_size(doc_info: &DocInfo) -> usize {
//     RAW_SEQ_INDEX_VALUE_SIZE + doc_info.id.len() + doc_info.rev_meta.len()
// }

// fn id_index_raw_value_size(doc_info: &DocInfo) -> usize {
//     RAW_ID_INDEX_VALUE_SIZE + doc_info.rev_meta.len()
// }
