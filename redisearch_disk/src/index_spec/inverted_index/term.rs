use ffi::{t_docId, t_fieldMask};
use inverted_index::{FilterMaskReader, RSIndexResult};
use rqe_iterators::inverted_index::InvIndIterator;
use speedb::{
    BlockBasedOptions, ColumnFamilyDescriptor, Options as SpeedbDbOptions, SliceTransform,
};
use std::mem::size_of;

use super::{
    DeletedIdsStore, InvertedIndexKey, PostingsListReader, block_traits, generic_index,
    generic_reader,
};
use crate::database::{Speedb, SpeedbMultithreadedDatabase};
use crate::key_traits::AsKeyExt;
use crate::merge_op::DeletedIdsMergeOperator;

pub mod archive;
pub mod block;

// Re-export PostingsListBlock for convenience
pub use block::PostingsListBlock;

/// A document in a postings list, including its ID and associated metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Document {
    pub doc_id: t_docId,
    pub metadata: Metadata,
}

/// Metadata associated with a term in a document, including the fields it appears in and its frequency.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Metadata {
    pub field_mask: u128,
    pub frequency: u64,
}

impl Metadata {
    pub const SIZE: usize = Self::FIELD_MASK_SIZE + Self::FREQUENCY_SIZE;
    pub const FIELD_MASK_SIZE: usize = size_of::<u128>();
    pub const FREQUENCY_SIZE: usize = size_of::<u64>();
}

/// Configuration for term-based inverted indexes.
///
/// Term indexes store full-text search data with document IDs, field masks, and frequencies.
/// They use the "fulltext" column family and require a merge operator for handling deleted IDs.
pub struct TermIndexConfig;

impl TermIndexConfig {
    const PREFIX_EXTRACTOR_NAME: &'static str = "fulltext_prefix_extractor";
    const MERGE_OPERATOR_NAME: &'static str = "deleted_ids_merge_operator";
}

impl block_traits::IndexConfig for TermIndexConfig {
    type SerializableBlock = PostingsListBlock;
    type ArchivedBlock = archive::ArchivedBlock;

    const COLUMN_FAMILY_NAME: &'static str = "fulltext";

    fn cf_descriptor(deleted_ids: Option<DeletedIdsStore>) -> ColumnFamilyDescriptor {
        let prefix_extractor = SliceTransform::create(
            Self::PREFIX_EXTRACTOR_NAME,
            generic_index::GenericInvertedIndex::<Self>::strip_doc_id_suffix,
            Some(generic_index::GenericInvertedIndex::<Self>::has_doc_id_suffix),
        );

        let mut cf_options = SpeedbDbOptions::default();

        // Term indexes require a merge operator for handling deleted IDs
        let deleted_ids =
            deleted_ids.expect("Term index requires DeletedIdsStore for merge operator");
        cf_options.set_merge_operator(
            Self::MERGE_OPERATOR_NAME,
            DeletedIdsMergeOperator::full_merge_fn(deleted_ids.clone()),
            DeletedIdsMergeOperator::partial_merge_fn(deleted_ids),
        );

        cf_options.set_prefix_extractor(prefix_extractor);
        cf_options.set_block_based_table_factory(&BlockBasedOptions::default());

        ColumnFamilyDescriptor::new(Self::COLUMN_FAMILY_NAME, cf_options)
    }
}

/// An inverted index maps terms to the documents which contain the term.
pub struct InvertedIndex {
    /// The generic inverted index implementation
    inner: generic_index::GenericInvertedIndex<TermIndexConfig>,
}

impl InvertedIndex {
    /// Creates a new inverted index with the given Speedb database.
    pub fn new(database: SpeedbMultithreadedDatabase) -> Self {
        InvertedIndex {
            inner: generic_index::GenericInvertedIndex::new(database),
        }
    }

    /// Creates a column family descriptor for the inverted index.
    pub fn cf_descriptor(deleted_ids: DeletedIdsStore) -> ColumnFamilyDescriptor {
        generic_index::GenericInvertedIndex::<TermIndexConfig>::cf_descriptor(Some(deleted_ids))
    }

    /// Strip the doc_id suffix from the key, leaving just the term.
    /// Delegates to the generic implementation.
    pub fn strip_doc_id_suffix(src: &[u8]) -> &[u8] {
        generic_index::GenericInvertedIndex::<TermIndexConfig>::strip_doc_id_suffix(src)
    }

    /// Returns whether `src` is long enough to contain a doc_id suffix.
    /// Delegates to the generic implementation.
    pub fn has_doc_id_suffix(src: &[u8]) -> bool {
        generic_index::GenericInvertedIndex::<TermIndexConfig>::has_doc_id_suffix(src)
    }

    /// Inserts a document ID into the postings list for the given term and for
    /// which fields the term appears in.
    pub fn insert(
        &self,
        term: String,
        doc_id: t_docId,
        field_mask: t_fieldMask,
        frequency: u64,
    ) -> Result<(), speedb::Error> {
        let doc = Document {
            doc_id,
            metadata: Metadata {
                field_mask,
                frequency,
            },
        };
        self.inner.insert(term, doc_id, doc)
    }

    /// Returns an iterator over the document IDs for the given term.
    pub fn term_iterator(
        &self,
        term: &str,
        field_mask: t_fieldMask,
        weight: f64,
    ) -> Result<
        InvIndIterator<'_, FilterMaskReader<PostingsListReader<'_, Speedb>>>,
        generic_reader::ReaderCreateError,
    > {
        let key = InvertedIndexKey {
            prefix: term,
            last_doc_id: None,
        }
        .as_key();

        let iterator = self.inner.database().iterator_cf(
            &self.inner.cf_handle(),
            speedb::IteratorMode::From(&key, speedb::Direction::Forward),
        );
        let reader = PostingsListReader::new(iterator, term.to_string())?;
        let reader = FilterMaskReader::new(field_mask, reader);

        let iter = InvIndIterator::new(reader, RSIndexResult::virt().weight(weight), None);

        Ok(iter)
    }

    /// Collect metrics for the text inverted index column family.
    pub fn collect_metrics(&self) -> crate::metrics::CFMetrics {
        let cf = self.inner.cf_handle();
        crate::metrics::CFMetrics::collect(self.inner.database(), &cf)
    }
}
