use speedb::{MergeOperands, merge_operator::MergeFn};

use crate::{
    debug,
    index_spec::{
        deleted_ids::DeletedIdsStore,
        inverted_index::{
            FullTermDocument, FullTermMetadata, PostingsListBlock,
            full_term_block::ArchivedFullTermBlock,
        },
    },
};

#[non_exhaustive]
pub struct DeletedIdsMergeOperator {}

impl DeletedIdsMergeOperator {
    /// Create a function that performs a full merge given the passed
    /// [`DeletedIdsStore`]
    pub fn full_merge_fn(deleted_ids: DeletedIdsStore) -> impl MergeFn {
        move |key: &[u8],
              existing_value: Option<&[u8]>, // single block
              operand_list: &MergeOperands| // list of blocks
              -> Option<Vec<u8>> {
            let merge_inner = Self::full_merge_inner(deleted_ids.clone());
            merge_inner(key, existing_value, operand_list.into_iter())
        }
    }

    /// Create a partial merge function. Delegates to [`Self::full_merge_fn`] for now.
    pub fn partial_merge_fn(deleted_ids: DeletedIdsStore) -> impl MergeFn {
        Self::full_merge_fn(deleted_ids)
    }

    /// Merge function that operates on a slice iterator
    /// rather than [`MergeOperands`], so it can be called
    /// from tests without having to instantiate a [`MergeOperands`].
    ///
    /// The implementation builds upon the following assumptions:
    /// - The last document ID of the existing block is greater than that of each operand;
    /// - That the operands are ordered ascendingly by their last document ID;
    /// - No document ID extists in more than one operand, or in any operand
    ///   and the existing block.
    ///
    /// This function is generic over the operands iterator (`TOperandsIter`),
    /// as well as the lifetime of the operands themselves (`'operand`).
    /// It takes the key bytes, the existing block if any, as well
    /// as the operands iterator as parameters. The type of `TOperandsIter`
    /// is restricted to iterators over byte slices that live for `'operand` (`&'operand [u8]`).
    /// The return value is the resulting block, or `None` if the resulting block
    /// is empty.
    ///
    /// Given these restrictions, it is compatible with [`MergeOperands`] as well
    /// as other iterators that iterate over byte slices, making it suitable
    /// for unit testing.
    #[inline]
    fn full_merge_inner<'operand, TOperandsIter>(
        deleted_ids: DeletedIdsStore,
    ) -> impl Fn(&[u8], Option<&[u8]>, TOperandsIter) -> Option<Vec<u8>>
    where
        TOperandsIter: Iterator<Item = &'operand [u8]>,
    {
        #[inline]
        fn push_from_archived_block(
            block: &mut PostingsListBlock,
            deleted_ids: &DeletedIdsStore,
            archived_block: &ArchivedFullTermBlock,
        ) {
            for archived_doc in archived_block.iter() {
                let doc_id = archived_doc.doc_id();
                if deleted_ids.is_deleted(doc_id) {
                    continue;
                };
                let metadata = FullTermMetadata {
                    field_mask: archived_doc.field_mask(),
                    frequency: archived_doc.frequency(),
                };
                block.push(FullTermDocument { doc_id, metadata });
            }
        }

        // Return a closure that takes the key, existing value and operand iterator
        // as parameters, and that moves any outer values it refers to inside the closure.
        // In this case, we need the `DeletedIdsStore` (i.e. `deleted_ids`) to be part
        // of the closure's state, so that it can be inspected whenever this closure is invoked.
        #[inline]
        move |_key: &[u8],
              existing_value: Option<&[u8]>,
              operand_list: TOperandsIter|
              -> Option<Vec<u8>> {
            let mut merged_block = PostingsListBlock::with_capacity(operand_list.size_hint().0);

            let mut max_doc_id = None;
            for operand in operand_list {
                let op_archived_block = ArchivedFullTermBlock::from_bytes(operand.into());
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
                push_from_archived_block(&mut merged_block, &deleted_ids, &op_archived_block);
            }

            if let Some(existing_value) = existing_value {
                let existing_archived_block =
                    ArchivedFullTermBlock::from_bytes(existing_value.into());
                if let Some(last_doc_id) = existing_archived_block.last().map(|d| d.doc_id()) {
                    if let Some(max_doc_id) = max_doc_id
                        && last_doc_id <= max_doc_id
                    {
                        debug!(
                            "Existing value's last doc id is not greater than that of the last operand"
                        );
                        return None;
                    }
                    push_from_archived_block(
                        &mut merged_block,
                        &deleted_ids,
                        &existing_archived_block,
                    );
                } // else there's nothing in the block
            }

            // TODO somehow update the block key based on the last doc ID in the merged block
            let data = merged_block.serialize();
            Some(data)
        }
    }
}

#[cfg(test)]
mod tests {
    use ffi::{t_docId, t_fieldMask};

    use crate::{
        index_spec::{
            deleted_ids::{DeletedIds, DeletedIdsStore},
            inverted_index::{
                FullTermDocument, FullTermMetadata, PostingsListBlock,
                full_term_block::ArchivedFullTermBlock,
            },
        },
        merge_op::DeletedIdsMergeOperator,
    };

    /// Verify all documents are present in the merged result, and
    /// that the result contains no excess documents.
    /// Note: The merge operator processes operands first, then existing value
    fn verify_merge_result(
        expected_id_and_masks: impl IntoIterator<Item = (t_docId, t_fieldMask)>,
        merged_result: Vec<u8>,
    ) {
        let archived_block = ArchivedFullTermBlock::from_bytes(merged_result.into());

        let mut i = 0;
        for (expected_id, expected_mask) in expected_id_and_masks {
            let Some(archivec_doc) = archived_block.get(i) else {
                break;
            };
            assert_eq!(archivec_doc.doc_id(), expected_id);
            assert_eq!(archivec_doc.field_mask(), expected_mask);
            i += 1;
        }

        assert_eq!(i, archived_block.num_terms());
    }

    /// Create a block given the document IDs and field masks
    fn create_block(docs: impl IntoIterator<Item = (t_docId, t_fieldMask)>) -> Vec<u8> {
        docs.into_iter()
            .map(|(doc_id, field_mask)| FullTermDocument {
                doc_id,
                metadata: FullTermMetadata {
                    field_mask,
                    frequency: 123,
                },
            })
            .fold(PostingsListBlock::new(), |mut builder, doc| {
                builder.push(doc);
                builder
            })
            .serialize()
    }

    /// Assert that merging existing documents with
    /// operand documents with the given ids and masks with results
    /// in the a block containing documents with the expected ids and masks,
    /// given the deleted ids.
    ///
    /// Note: to avoid having to specify a concrete iterator type
    /// in case there should not be an existing block, use [`assert_merge_no_existing`].
    fn do_assert_merge(
        operands_id_and_masks: impl IntoIterator<
            Item = impl IntoIterator<Item = (t_docId, t_fieldMask)>,
        >,
        existing_id_and_masks: Option<impl IntoIterator<Item = (t_docId, t_fieldMask)>>,
        expected_id_and_masks: impl IntoIterator<Item = (t_docId, t_fieldMask)>,
        deleted_ids: DeletedIdsStore,
    ) {
        // Create operand documents and serialize them into a block per operand
        // We need to store the serialized operand blocks in a Vec...
        let operand_blocks: Vec<Vec<u8>> = operands_id_and_masks
            .into_iter()
            .map(create_block)
            .collect();
        // ...so we can obtain references to the vecs it holds here
        let operand_blocks: Vec<&[u8]> = operand_blocks
            .iter()
            .map(|block| block.as_slice())
            .collect();

        // Create existing documents and serialize them into a block
        let existing_block = existing_id_and_masks.map(create_block);

        // Do the merge
        let full_merge = DeletedIdsMergeOperator::full_merge_inner(deleted_ids);
        let merged_result = full_merge(
            b"test_term",
            existing_block.as_deref(),
            operand_blocks.iter().copied(),
        );

        // Verify result
        let merged_result = merged_result.expect("Merged result should be Some");
        verify_merge_result(expected_id_and_masks, merged_result);
    }

    #[test]
    fn test_basic_merge() {
        let deleted_ids = DeletedIdsStore::default();
        do_assert_merge(
            [[(1, 0x01), (2, 0x02)]],
            Some([(3, 0x03), (4, 0x04)]),
            [(1, 0x01), (2, 0x02), (3, 0x03), (4, 0x04)],
            deleted_ids,
        );
    }

    #[test]
    fn test_merge_without_existing_value() {
        let deleted_ids = DeletedIdsStore::default();
        do_assert_merge(
            [[(1, 0x01), (2, 0x02)]],
            None::<Option<_>>,
            [(1, 0x01), (2, 0x02)],
            deleted_ids,
        );
    }

    #[test]
    fn test_merge_multiple_operands() {
        let deleted_ids = DeletedIdsStore::default();
        do_assert_merge(
            [[(1, 0x01)], [(2, 0x02)], [(3, 0x03)]],
            Some([(4, 0x04)]),
            [(1, 0x01), (2, 0x02), (3, 0x03), (4, 0x04)],
            deleted_ids,
        );
    }

    #[test]
    fn test_basic_with_deleted_ids() {
        let mut deleted_ids = DeletedIds::default();
        deleted_ids.mark_deleted(2);
        let deleted_ids = DeletedIdsStore::with_deleted_ids(deleted_ids);

        do_assert_merge(
            [[(1, 0x01), (2, 0x02)]],
            Some([(3, 0x03), (4, 0x04)]),
            [(1, 0x01), (3, 0x03), (4, 0x04)],
            deleted_ids,
        );
    }

    #[test]
    fn test_basic_all_ids_deleted() {
        let mut deleted_ids = DeletedIds::default();
        deleted_ids.mark_deleted(1);
        deleted_ids.mark_deleted(2);
        deleted_ids.mark_deleted(3);
        deleted_ids.mark_deleted(4);
        let deleted_ids = DeletedIdsStore::with_deleted_ids(deleted_ids);

        do_assert_merge(
            [[(1, 0x01), (2, 0x02)]],
            Some([(3, 0x03), (4, 0x04)]),
            [],
            deleted_ids,
        );
    }
}
