use ffi::{t_docId, t_fieldMask};
use inverted_index::{FilterMaskReader, RSIndexResult, RSOffsetSlice};
use query_term::RSQueryTerm;
use rqe_iterators::{NoOpChecker, inverted_index::InvIndIterator};
use speedb::{
    BlockBasedOptions, ColumnFamilyDescriptor, Options as SpeedbDbOptions, SliceTransform,
};
use std::mem::size_of;
use std::ops::Deref;

use super::{
    DeletedIdsStore, InvertedIndexKey, PostingsListReader, block_traits, block_traits::IndexConfig,
    generic_index, generic_merge_operator::GenericMergeOperator, generic_reader,
};
use crate::database::{Speedb, SpeedbMultithreadedDatabase};
use crate::key_traits::AsKeyExt;

pub mod archive;
pub mod block;

// Re-export PostingsListBlock for convenience
pub use block::PostingsListBlock;

// Re-export ArchivedBlock with an alias to avoid naming conflict with the trait
pub use archive::ArchivedBlock as TermArchivedBlock;

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
    pub frequency: u32,
}

impl Metadata {
    pub const FIELD_MASK_SIZE: usize = size_of::<u128>();
    pub const FREQUENCY_SIZE: usize = size_of::<u32>();
    pub const SIZE: usize = Self::FIELD_MASK_SIZE + Self::FREQUENCY_SIZE;
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
            generic_index::strip_doc_id_suffix,
            Some(generic_index::has_doc_id_suffix),
        );

        let mut cf_options = SpeedbDbOptions::default();

        cf_options.set_disable_auto_compactions(true);
        cf_options.set_merge_values(true);

        // Term indexes require a merge operator for handling deleted IDs
        let deleted_ids =
            deleted_ids.expect("Term index requires DeletedIdsStore for merge operator");
        cf_options.set_merge_operator_associative(
            Self::MERGE_OPERATOR_NAME,
            GenericMergeOperator::<Self>::full_merge_fn(deleted_ids.clone()),
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

impl Deref for InvertedIndex {
    type Target = generic_index::GenericInvertedIndex<TermIndexConfig>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl InvertedIndex {
    /// Creates a new inverted index with the given Speedb database.
    pub fn new(database: SpeedbMultithreadedDatabase) -> Self {
        InvertedIndex {
            inner: generic_index::GenericInvertedIndex::new(
                database,
                TermIndexConfig::COLUMN_FAMILY_NAME,
            ),
        }
    }

    /// Inserts a document ID into the postings list for the given term and for
    /// which fields the term appears in.
    pub fn insert(
        &self,
        term: &str,
        doc_id: t_docId,
        field_mask: t_fieldMask,
        frequency: u32,
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
        query_term: Box<RSQueryTerm>,
        field_mask: t_fieldMask,
        weight: f64,
    ) -> Result<
        InvIndIterator<'_, FilterMaskReader<PostingsListReader<'_, Speedb>>>,
        generic_reader::ReaderCreateError,
    > {
        let qt = query_term.as_ref();
        // SAFETY: qt.str_ and qt.len are valid as query_term is guaranteed valid by the caller.
        let term = unsafe { std::slice::from_raw_parts(qt.str_ as *const u8, qt.len) };
        // SAFETY: The caller in lib.rs already validated this is valid UTF-8
        let term_str =
            std::str::from_utf8(term).expect("term should be valid UTF-8, validated by caller");

        let key: Vec<u8> = InvertedIndexKey {
            prefix: term_str,
            last_doc_id: None,
        }
        .as_key();

        let iterator = self.inner.database().iterator_cf(
            &self.inner.cf_handle(),
            speedb::IteratorMode::From(&key, speedb::Direction::Forward),
        );
        let reader = PostingsListReader::new(iterator, term_str.to_string())?;
        let reader = FilterMaskReader::new(field_mask, reader);

        let result = RSIndexResult::with_term(Some(query_term), RSOffsetSlice::empty(), 0, 0, 0)
            .weight(weight);

        let iter = InvIndIterator::new(reader, result, NoOpChecker);
        Ok(iter)
    }

    /// Collect metrics for the text inverted index column family.
    pub fn collect_metrics(&self) -> crate::metrics::ColumnFamilyMetrics {
        let cf = self.inner.cf_handle();
        crate::metrics::ColumnFamilyMetrics::collect(self.inner.database(), &cf)
    }
}
