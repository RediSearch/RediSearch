use ffi::{t_docId, t_fieldIndex};
use inverted_index::RSIndexResult;
use rqe_iterators::{NoOpChecker, inverted_index::InvIndIterator};
use speedb::{
    BlockBasedOptions, ColumnFamilyDescriptor, Options as SpeedbDbOptions, SliceTransform,
};
use std::mem::size_of;
use std::ops::Deref;

use super::{
    DeletedIdsStore, TagPostingsListReader, block_traits, generic_index,
    generic_index::GenericInvertedIndex, generic_merge_operator::GenericMergeOperator,
    generic_reader,
};
use crate::compaction::NoOpCallbacks;
use crate::database::{Speedb, SpeedbMultithreadedDatabase};
use crate::key_traits::AsKeyExt;

pub mod archive;
pub mod block;

// Re-export TagPostingsListBlock for convenience
pub use block::TagPostingsListBlock;

/// A document in a tag postings list. Unlike term postings, tag postings only store
/// the document ID without any metadata (no frequency, no field mask).
/// This makes tag indexes much more compact and efficient for exact-match queries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TagDocument {
    pub doc_id: t_docId,
}

impl TagDocument {
    /// Size of a tag document entry in bytes (just the doc_id)
    pub const SIZE: usize = size_of::<t_docId>();

    /// Create a new tag document
    pub fn new(doc_id: t_docId) -> Self {
        Self { doc_id }
    }
}

/// Configuration for tag-based inverted indexes.
///
/// Tag indexes store only document IDs for exact-match queries, making them more compact
/// than term indexes. Each tag field has its own column family (e.g., "tag_0", "tag_1").
pub struct TagIndexConfig;

impl TagIndexConfig {
    const PREFIX_EXTRACTOR_NAME: &'static str = "tag_prefix_extractor";
    const MERGE_OPERATOR_NAME: &'static str = "tag_merge_operator";
    const CF_PREFIX: &'static str = "tag_";

    /// Returns the column family name for a given field index.
    pub fn cf_name(field_index: t_fieldIndex) -> String {
        format!("{}{}", Self::CF_PREFIX, field_index)
    }

    /// Parses a column family name and returns the field index if it's a tag CF.
    /// Returns None if the name doesn't match the tag CF pattern.
    pub fn parse_cf_name(cf_name: &str) -> Option<t_fieldIndex> {
        cf_name.strip_prefix(Self::CF_PREFIX)?.parse().ok()
    }

    /// Creates the column family options for a tag inverted index.
    ///
    /// # Arguments
    /// * `deleted_ids` - Store for tracking deleted document IDs
    pub fn cf_options(deleted_ids: DeletedIdsStore) -> SpeedbDbOptions {
        let prefix_extractor = SliceTransform::create(
            Self::PREFIX_EXTRACTOR_NAME,
            generic_index::strip_doc_id_suffix,
            Some(generic_index::has_potentially_doc_id_suffix),
        );

        let mut cf_options = SpeedbDbOptions::default();

        cf_options.set_disable_auto_compactions(true);
        cf_options.set_merge_accum_limit(super::MERGE_ACCUMULATION_SIZE_LIMIT);

        // Tag indexes use a merge operator for handling deleted IDs
        cf_options.set_merge_operator_associative(
            Self::MERGE_OPERATOR_NAME,
            GenericMergeOperator::<Self, NoOpCallbacks>::full_merge_fn(
                deleted_ids.clone(),
                NoOpCallbacks,
            ),
            true,
        );

        cf_options.set_prefix_extractor(prefix_extractor);
        cf_options.set_block_based_table_factory(&BlockBasedOptions::default());

        cf_options
    }

    /// Creates a column family descriptor for a tag inverted index.
    ///
    /// # Arguments
    /// * `field_index` - The field index for the tag field
    /// * `deleted_ids` - Store for tracking deleted document IDs
    pub fn cf_descriptor(
        field_index: t_fieldIndex,
        deleted_ids: DeletedIdsStore,
    ) -> ColumnFamilyDescriptor {
        ColumnFamilyDescriptor::new(Self::cf_name(field_index), Self::cf_options(deleted_ids))
    }
}

impl block_traits::IndexConfig for TagIndexConfig {
    type SerializableBlock = TagPostingsListBlock;
    type ArchivedBlock = archive::ArchivedTagBlock;
    type CfConfig = DeletedIdsStore;

    // Not used directly - each tag field has its own CF via TagInvertedIndex::cf_name()
    const COLUMN_FAMILY_NAME: &'static str = "tags";

    fn cf_descriptor(deleted_ids: Self::CfConfig) -> ColumnFamilyDescriptor {
        ColumnFamilyDescriptor::new(Self::COLUMN_FAMILY_NAME, Self::cf_options(deleted_ids))
    }
}

/// A tag inverted index maps tag values to the documents which contain the tag.
/// Unlike the full-text inverted index, tag indexes only store document IDs without
/// field masks or frequencies, making them much more compact and efficient.
///
/// Each tag field has its own TagInvertedIndex instance with a dedicated column family.
pub struct TagInvertedIndex {
    /// The generic inverted index implementation
    inner: GenericInvertedIndex<TagIndexConfig>,
}

impl Deref for TagInvertedIndex {
    type Target = GenericInvertedIndex<TagIndexConfig>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl TagInvertedIndex {
    /// Creates a new tag inverted index for the given field.
    ///
    /// The column family must already exist in the database.
    pub fn new(database: SpeedbMultithreadedDatabase, field_index: t_fieldIndex) -> Self {
        TagInvertedIndex {
            inner: GenericInvertedIndex::new(database, &TagIndexConfig::cf_name(field_index)),
        }
    }

    /// Inserts a document ID into the postings list for the given tag.
    ///
    /// # Arguments
    /// * `tag` - The tag value to index
    /// * `doc_id` - The document ID to associate with this tag
    pub fn insert(&self, tag: &str, doc_id: t_docId) -> Result<(), speedb::Error> {
        let doc = TagDocument::new(doc_id);
        self.inner.insert(tag, doc_id, doc)
    }

    /// Returns an iterator over the document IDs for the given tag.
    ///
    /// # Arguments
    /// * `tag` - The tag value to search for
    /// * `weight` - Weight for scoring
    pub fn tag_iterator(
        &self,
        tag: &str,
        weight: f64,
    ) -> Result<
        InvIndIterator<'_, TagPostingsListReader<'_, Speedb>>,
        generic_reader::ReaderCreateError,
    > {
        let key = super::InvertedIndexKey {
            prefix: tag,
            last_doc_id: None,
        }
        .as_key();

        let iterator = self.inner.database().full_iterator_cf(
            &self.inner.cf_handle(),
            speedb::IteratorMode::From(&key, speedb::Direction::Forward),
        );
        let reader = generic_reader::GenericReader::new(iterator, tag.to_string())?;

        let iter = InvIndIterator::new(reader, RSIndexResult::virt().weight(weight), NoOpChecker);

        Ok(iter)
    }
}
