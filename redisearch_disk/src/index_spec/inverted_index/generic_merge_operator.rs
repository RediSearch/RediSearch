use std::marker::PhantomData;

use speedb::{
    MergeOperands,
    merge_operator::{MergeFn, MergeResult},
};

use crate::{
    compaction::{MergeCallbacks, NoOpCallbacks},
    debug,
    index_spec::deleted_ids::DeletedIdsStore,
    key_traits::AsKeyExt,
};

use super::{
    InvertedIndexKey,
    block_traits::{ArchivedBlock, ArchivedDocument, IndexConfig, SerializableBlock},
    generic_index,
};

/// A generic merge operator that works with any block type implementing IndexConfig.
/// This eliminates code duplication between term and tag merge operators.
///
/// The merge operator filters out deleted document IDs during compaction,
/// merging multiple blocks into a single block while respecting the deleted IDs store.
///
/// The `Callback` type parameter allows customizing behavior during merge:
/// - Use `NoOpCallbacks` for simple merging without delta tracking
/// - Use `CompactionDeltaCollector` to record removals for compaction delta building
pub struct GenericMergeOperator<Config, Callbacks = NoOpCallbacks> {
    _phantom: PhantomData<(Config, Callbacks)>,
}

impl<Config, Callbacks> GenericMergeOperator<Config, Callbacks>
where
    Config: IndexConfig,
    Callbacks: MergeCallbacks,
    for<'a> <Config::ArchivedBlock as ArchivedBlock>::Document<'a>:
        Into<<Config::SerializableBlock as SerializableBlock>::Document>,
{
    /// Create a function that performs a full merge given the passed [`DeletedIdsStore`]
    /// and callback for tracking merge events.
    pub fn full_merge_fn(
        deleted_ids: DeletedIdsStore,
        callbacks: Callbacks,
    ) -> impl MergeFn + Clone {
        move |key: &[u8],
              existing_value: Option<&[u8]>,
              operand_list: &MergeOperands|
              -> Option<MergeResult> {
            // SpeedB's DoMergeValues passes the full user key (prefix + delimiter + doc_id).
            // Strip the fixed-size doc_id suffix to recover just the prefix.
            let prefix = generic_index::strip_doc_id_suffix(key);
            let merge_inner = Self::full_merge_inner(deleted_ids.clone(), &callbacks);
            merge_inner(prefix, existing_value, operand_list.into_iter())
                .map(|(data, key)| (data, Some(key)))
        }
    }

    /// Merge function that operates on a slice iterator rather than [`MergeOperands`],
    /// so it can be called from tests without having to instantiate a [`MergeOperands`].
    ///
    /// The implementation builds upon the following assumptions:
    /// - The last document ID of the existing block is greater than that of each operand;
    /// - That the operands are ordered ascendingly by their last document ID;
    /// - No document ID exists in more than one operand, or in any operand and the existing block.
    #[inline]
    #[allow(clippy::type_complexity)]
    pub fn full_merge_inner<'operand, TOperandsIter>(
        deleted_ids: DeletedIdsStore,
        callbacks: &Callbacks,
    ) -> impl Fn(&[u8], Option<&[u8]>, TOperandsIter) -> Option<(Vec<u8>, Vec<u8>)>
    where
        TOperandsIter: Iterator<Item = &'operand [u8]>,
    {
        move |key: &[u8],
              existing_value: Option<&[u8]>,
              operand_list: TOperandsIter|
              -> Option<(Vec<u8>, Vec<u8>)> {
            // Validate UTF-8 - terms must be valid UTF-8 for InvertedIndexKey construction (Keys come without the docID suffix, have been transformed by the caller)
            let term = match std::str::from_utf8(key) {
                Ok(s) => s,
                Err(_) => {
                    debug!("Term is not valid UTF-8, cannot perform merge");
                    return None;
                }
            };

            let mut merged_block =
                Config::SerializableBlock::with_capacity(operand_list.size_hint().0);

            let mut max_doc_id = None;
            let mut block_last_doc_id = None;

            for operand in operand_list {
                let op_archived_block = Config::ArchivedBlock::from_bytes(operand.into());
                let Some(last_doc_id) = op_archived_block.last().map(|d| d.doc_id()) else {
                    continue; // Block empty, no need to push
                };
                if let Some(max_doc_id) = max_doc_id
                    && last_doc_id <= max_doc_id
                {
                    debug!(
                        "Operand's last doc id is not greater than that of the previous operands"
                    );
                    return None;
                }
                max_doc_id = Some(last_doc_id);
                if let Some(last_pushed_doc_id) = Self::push_from_archived_block(
                    &mut merged_block,
                    &deleted_ids,
                    &op_archived_block,
                    term,
                    callbacks,
                ) {
                    block_last_doc_id = Some(last_pushed_doc_id);
                }
            }

            if let Some(existing_value) = existing_value {
                let existing_archived_block =
                    Config::ArchivedBlock::from_bytes(existing_value.into());
                if let Some(last_doc_id) = existing_archived_block.last().map(|d| d.doc_id()) {
                    if let Some(max_doc_id) = max_doc_id
                        && last_doc_id <= max_doc_id
                    {
                        debug!(
                            "Existing value's last doc id is not greater than that of the last operand"
                        );
                        return None;
                    }
                    if let Some(last_pushed_doc_id) = Self::push_from_archived_block(
                        &mut merged_block,
                        &deleted_ids,
                        &existing_archived_block,
                        term,
                        callbacks,
                    ) {
                        block_last_doc_id = Some(last_pushed_doc_id);
                    }
                }
            }

            if merged_block.is_empty() {
                debug!("Merged block is empty after filtering out deleted IDs");
                // Note: We don't track term emptiness here because a term can span
                // multiple blocks. The merge operator only sees individual blocks,
                // so emptying one block doesn't mean the term is empty. Term deletion
                // is detected when applying the compaction delta to the trie - if a
                // term's doc count reaches 0, the trie returns TRIE_DECR_DELETED.
                return None;
            }

            let new_key = InvertedIndexKey::new(term, block_last_doc_id);

            let data = merged_block.serialize();
            Some((data, new_key.as_key()))
        }
    }

    #[inline]
    fn push_from_archived_block(
        block: &mut Config::SerializableBlock,
        deleted_ids: &DeletedIdsStore,
        archived_block: &Config::ArchivedBlock,
        term: &str,
        callbacks: &Callbacks,
    ) -> Option<u64> {
        let mut last_pushed_doc_id = None;

        for archived_doc in archived_block.iter() {
            let doc_id = archived_doc.doc_id();
            if deleted_ids.is_deleted(doc_id) {
                // Notify callback about the removal
                callbacks.on_doc_removed(term, doc_id);
                continue;
            }
            block.push(archived_doc.into());
            last_pushed_doc_id = Some(doc_id);
        }

        last_pushed_doc_id
    }
}

#[cfg(test)]
mod tests {
    use ffi::t_docId;
    use pretty_assertions::assert_eq;

    use super::{GenericMergeOperator, NoOpCallbacks};
    use crate::index_spec::{
        deleted_ids::{DeletedIds, DeletedIdsStore},
        inverted_index::{
            TagIndexConfig, TermIndexConfig,
            block_traits::{ArchivedBlock, ArchivedDocument, IndexConfig, SerializableBlock},
            tag::TagDocument,
            term,
        },
    };

    /// Trait for creating test documents for different block types.
    /// This allows us to write generic tests that work for both term and tag blocks.
    trait TestDocumentFactory: IndexConfig {
        /// Additional data associated with each document (e.g., field_mask for terms)
        type ExtraData: Copy + Default + PartialEq + std::fmt::Debug;

        /// Create a document with the given doc_id and extra data
        fn create_document(
            doc_id: t_docId,
            extra: Self::ExtraData,
        ) -> <Self::SerializableBlock as SerializableBlock>::Document;

        /// Extract extra data from an archived document for verification
        fn extract_extra(
            archived: &<Self::ArchivedBlock as ArchivedBlock>::Document<'_>,
        ) -> Self::ExtraData;
    }

    impl TestDocumentFactory for TermIndexConfig {
        type ExtraData = u128; // field_mask

        fn create_document(doc_id: t_docId, field_mask: Self::ExtraData) -> term::Document {
            term::Document {
                doc_id,
                metadata: term::Metadata {
                    field_mask,
                    frequency: 123,
                },
            }
        }

        fn extract_extra(archived: &term::archive::ArchivedDocument<'_>) -> Self::ExtraData {
            archived.field_mask()
        }
    }

    impl TestDocumentFactory for TagIndexConfig {
        type ExtraData = (); // Tags have no extra data

        fn create_document(doc_id: t_docId, _extra: Self::ExtraData) -> TagDocument {
            TagDocument::new(doc_id)
        }

        fn extract_extra(
            _archived: &crate::index_spec::inverted_index::tag::archive::ArchivedTagDocument<'_>,
        ) -> Self::ExtraData {
        }
    }

    /// Create a block from doc_ids and extra data
    fn create_block<Config: TestDocumentFactory>(
        docs: impl IntoIterator<Item = (t_docId, Config::ExtraData)>,
    ) -> Vec<u8> {
        let block = docs
            .into_iter()
            .map(|(doc_id, extra)| Config::create_document(doc_id, extra))
            .fold(Config::SerializableBlock::new(), |mut builder, doc| {
                builder.push(doc);
                builder
            });
        block.serialize()
    }

    /// Verify all documents are present in the merged result
    fn verify_merge_result<Config: TestDocumentFactory>(
        expected: impl IntoIterator<Item = (t_docId, Config::ExtraData)>,
        merged_result: &[u8],
    ) where
        <Config::ArchivedBlock as ArchivedBlock>::Index: TryFrom<usize>,
    {
        let archived_block = Config::ArchivedBlock::from_bytes(merged_result.into());

        let mut i: usize = 0;
        for (expected_id, expected_extra) in expected {
            let index = i.try_into().ok().expect("Index conversion failed");
            let archived_doc = archived_block.get(index).expect("Missing document");
            assert_eq!(archived_doc.doc_id(), expected_id);
            assert_eq!(Config::extract_extra(&archived_doc), expected_extra);
            i += 1;
        }

        assert_eq!(i, archived_block.num_docs().into());
    }

    /// Generic merge test helper
    fn do_assert_merge<Config: TestDocumentFactory>(
        key: &[u8],
        operands: impl IntoIterator<Item = impl IntoIterator<Item = (t_docId, Config::ExtraData)>>,
        existing: Option<impl IntoIterator<Item = (t_docId, Config::ExtraData)>>,
        expected: impl IntoIterator<Item = (t_docId, Config::ExtraData)>,
        deleted_ids: DeletedIdsStore,
        expected_new_key: Option<&[u8]>,
    ) where
        for<'a> <Config::ArchivedBlock as ArchivedBlock>::Document<'a>:
            Into<<Config::SerializableBlock as SerializableBlock>::Document>,
        <Config::ArchivedBlock as ArchivedBlock>::Index: TryFrom<usize>,
    {
        let operand_blocks: Vec<Vec<u8>> = operands
            .into_iter()
            .map(|docs| create_block::<Config>(docs))
            .collect();
        let operand_refs: Vec<&[u8]> = operand_blocks.iter().map(|b| b.as_slice()).collect();

        let existing_block = existing.map(|docs| create_block::<Config>(docs));

        let full_merge = GenericMergeOperator::<Config, NoOpCallbacks>::full_merge_inner(
            deleted_ids,
            &NoOpCallbacks,
        );
        let merged_result =
            full_merge(key, existing_block.as_deref(), operand_refs.iter().copied());

        let expected: Vec<_> = expected.into_iter().collect();

        match (merged_result, expected_new_key) {
            (None, None) => {
                assert!(
                    expected.is_empty(),
                    "Expected empty result but had expected docs"
                );
            }
            (Some((result, new_key)), Some(expected_key)) => {
                verify_merge_result::<Config>(expected, &result);
                assert_eq!(new_key, expected_key);
            }
            (None, Some(_)) => panic!("Expected merge to produce a result, but got None"),
            (Some(_), None) => panic!("Expected merge to return None, but got a result"),
        }
    }

    // ==================== Term Block Tests ====================

    // SpeedB calls the merge operator with keys without the doc_id suffix (prefix-only).
    // The key format is: [term/tag bytes] (just the term or tag, no delimiter or doc_id).

    #[test]
    fn test_term_basic_merge() {
        do_assert_merge::<TermIndexConfig>(
            b"test_term",
            [[(1, 0x01), (2, 0x02)]],
            Some([(3, 0x03), (4, 0x04)]),
            [(1, 0x01), (2, 0x02), (3, 0x03), (4, 0x04)],
            DeletedIdsStore::default(),
            Some(b"test_term\x00\x00\x00\x00\x00\x00\x00\x00\x04"),
        );
    }

    #[test]
    fn test_term_merge_multiple_operands() {
        do_assert_merge::<TermIndexConfig>(
            b"test_term",
            [[(1, 0x01)], [(2, 0x02)], [(3, 0x03)]],
            Some([(4, 0x04)]),
            [(1, 0x01), (2, 0x02), (3, 0x03), (4, 0x04)],
            DeletedIdsStore::default(),
            Some(b"test_term\x00\x00\x00\x00\x00\x00\x00\x00\x04"),
        );
    }

    #[test]
    fn test_term_with_deleted_ids() {
        let mut deleted_ids = DeletedIds::default();
        deleted_ids.mark_deleted(2);
        let deleted_ids = DeletedIdsStore::with_deleted_ids(deleted_ids);

        do_assert_merge::<TermIndexConfig>(
            b"test_term",
            [[(1, 0x01), (2, 0x02)]],
            Some([(3, 0x03), (4, 0x04)]),
            [(1, 0x01), (3, 0x03), (4, 0x04)],
            deleted_ids,
            Some(b"test_term\x00\x00\x00\x00\x00\x00\x00\x00\x04"),
        );
    }

    #[test]
    fn test_term_all_ids_deleted() {
        let mut deleted_ids = DeletedIds::default();
        for id in 1..=4 {
            deleted_ids.mark_deleted(id);
        }
        let deleted_ids = DeletedIdsStore::with_deleted_ids(deleted_ids);

        do_assert_merge::<TermIndexConfig>(
            b"test_term",
            [[(1, 0x01), (2, 0x02)]],
            Some([(3, 0x03), (4, 0x04)]),
            [],
            deleted_ids,
            None,
        );
    }

    // ==================== Tag Block Tests ====================

    #[test]
    fn test_tag_basic_merge() {
        do_assert_merge::<TagIndexConfig>(
            b"test_tag",
            [[(1, ()), (2, ())]],
            Some([(3, ()), (4, ())]),
            [(1, ()), (2, ()), (3, ()), (4, ())],
            DeletedIdsStore::default(),
            Some(b"test_tag\x00\x00\x00\x00\x00\x00\x00\x00\x04"),
        );
    }

    #[test]
    fn test_tag_merge_multiple_operands() {
        do_assert_merge::<TagIndexConfig>(
            b"test_tag",
            [[(1, ())], [(2, ())], [(3, ())]],
            Some([(4, ())]),
            [(1, ()), (2, ()), (3, ()), (4, ())],
            DeletedIdsStore::default(),
            Some(b"test_tag\x00\x00\x00\x00\x00\x00\x00\x00\x04"),
        );
    }

    #[test]
    fn test_tag_with_deleted_ids() {
        let mut deleted_ids = DeletedIds::default();
        deleted_ids.mark_deleted(2);
        let deleted_ids = DeletedIdsStore::with_deleted_ids(deleted_ids);

        do_assert_merge::<TagIndexConfig>(
            b"test_tag",
            [[(1, ()), (2, ())]],
            Some([(3, ()), (4, ())]),
            [(1, ()), (3, ()), (4, ())],
            deleted_ids,
            Some(b"test_tag\x00\x00\x00\x00\x00\x00\x00\x00\x04"),
        );
    }

    #[test]
    fn test_tag_all_ids_deleted() {
        let mut deleted_ids = DeletedIds::default();
        for id in 1..=4 {
            deleted_ids.mark_deleted(id);
        }
        let deleted_ids = DeletedIdsStore::with_deleted_ids(deleted_ids);

        do_assert_merge::<TagIndexConfig>(
            b"test_tag",
            [[(1, ()), (2, ())]],
            Some([(3, ()), (4, ())]),
            [],
            deleted_ids,
            None,
        );
    }
}
