use ffi::t_docId;
use inverted_index::RSIndexResult;
use rqe_iterators::inverted_index::InvIndIterator;
use speedb::{
    BlockBasedOptions, ColumnFamilyDescriptor, Options as SpeedbDbOptions, SliceTransform,
};
use std::mem::size_of;

use super::{
    InvertedIndexKey, Speedb, SpeedbMultithreadedDatabase, TagPostingsListReader, block_traits,
    generic_index, generic_reader,
};
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
/// than term indexes. They use the "tags" column family and do not require a merge operator.
pub struct TagIndexConfig;

impl TagIndexConfig {
    const PREFIX_EXTRACTOR_NAME: &'static str = "tags_prefix_extractor";
}

impl block_traits::IndexConfig for TagIndexConfig {
    type SerializableBlock = TagPostingsListBlock;
    type ArchivedBlock = archive::ArchivedTagBlock;

    const COLUMN_FAMILY_NAME: &'static str = "tags";

    fn cf_descriptor(_deleted_ids: Option<super::DeletedIdsStore>) -> ColumnFamilyDescriptor {
        let prefix_extractor = SliceTransform::create(
            Self::PREFIX_EXTRACTOR_NAME,
            generic_index::GenericInvertedIndex::<Self>::strip_doc_id_suffix,
            Some(generic_index::GenericInvertedIndex::<Self>::has_doc_id_suffix),
        );

        let mut cf_options = SpeedbDbOptions::default();

        // TODO: Implement a merge operator for tag indexes (separate Jira task - MOD-13362)
        cf_options.set_prefix_extractor(prefix_extractor);
        cf_options.set_block_based_table_factory(&BlockBasedOptions::default());

        ColumnFamilyDescriptor::new(Self::COLUMN_FAMILY_NAME, cf_options)
    }
}

/// A tag inverted index maps tag values to the documents which contain the tag.
/// Unlike the full-text inverted index, tag indexes only store document IDs without
/// field masks or frequencies, making them much more compact and efficient.
pub struct TagInvertedIndex {
    /// The generic inverted index implementation
    inner: generic_index::GenericInvertedIndex<TagIndexConfig>,
}

impl TagInvertedIndex {
    /// Creates a new tag inverted index with the given Speedb database.
    pub fn new(database: SpeedbMultithreadedDatabase) -> Self {
        TagInvertedIndex {
            inner: generic_index::GenericInvertedIndex::new(database),
        }
    }

    /// Creates a column family descriptor for the tag inverted index.
    pub fn cf_descriptor() -> ColumnFamilyDescriptor {
        generic_index::GenericInvertedIndex::<TagIndexConfig>::cf_descriptor(None)
    }

    /// Inserts a document ID into the postings list for the given tag.
    pub fn insert(&self, tag: String, doc_id: t_docId) -> Result<(), speedb::Error> {
        let doc = TagDocument::new(doc_id);
        self.inner.insert(tag, doc_id, doc)
    }

    /// Returns an iterator over the document IDs for the given tag.
    pub fn tag_iterator(
        &self,
        tag: &str,
        weight: f64,
    ) -> Result<
        InvIndIterator<'_, TagPostingsListReader<'_, Speedb>>,
        generic_reader::ReaderCreateError,
    > {
        let key = InvertedIndexKey {
            prefix: tag,
            last_doc_id: None,
        }
        .as_key();

        let iterator = self.inner.database().iterator_cf(
            &self.inner.cf_handle(),
            speedb::IteratorMode::From(&key, speedb::Direction::Forward),
        );
        let reader = TagPostingsListReader::new(iterator, tag.to_string())?;

        let iter = InvIndIterator::new(reader, RSIndexResult::virt().weight(weight), None);

        Ok(iter)
    }
}
