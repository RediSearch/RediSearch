pub mod term;
pub use term::reader::{
    Reader as PostingsListReader, ReaderCreateError as PostingReaderCreateError,
};

use std::{mem::size_of, sync::Arc};

use ffi::{t_docId, t_fieldMask};
use inverted_index::{FilterMaskReader, RSIndexResult};
use rqe_iterators::inverted_index::InvIndIterator;
use speedb::BoundColumnFamily;

use super::DeletedIdsStore;
use crate::merge_op::DeletedIdsMergeOperator;

use speedb::{
    BlockBasedOptions, ColumnFamilyDescriptor, Options as SpeedbDbOptions, SliceTransform,
};

use crate::{
    database::{Speedb, SpeedbMultithreadedDatabase},
    document_id_key::DocumentIdKey,
    key_traits::AsKeyExt,
};

/// Delimiter used in inverted index keys between term and last document ID
const KEY_DELIMETER_STR: &str = "_";

/// An inverted index maps terms to the documents which contain the term.
pub struct InvertedIndex {
    /// The Speedb database where we store the inverted index.
    database: SpeedbMultithreadedDatabase,

    /// The name of the column family where we store the inverted index.
    ///
    /// We can't currently store the column family handle directly because it has a lifetime
    /// tied to the database instance, which complicates ownership (does not compile).
    cf_name: String,
}

/// Key structure for inverted index entries
struct InvertedIndexKey<'term> {
    term: &'term str,
    last_doc_id: Option<t_docId>,
}

impl<'term> AsKeyExt for InvertedIndexKey<'term> {
    fn as_key(&self) -> Vec<u8> {
        let key = if let Some(last_doc_id) = self.last_doc_id {
            let last_doc_id: DocumentIdKey = last_doc_id.into();
            format!("{}{}{}", self.term, KEY_DELIMETER_STR, last_doc_id)
        } else {
            format!("{}{}", self.term, KEY_DELIMETER_STR)
        };

        key.as_bytes().to_vec()
    }
}

/// We use a block-based postings list to store document IDs of all documents containing the term and metadata for each document in the term context. This allows
/// us to quickly jump over large sections of the postings list when searching.
#[derive(Clone, Default)]
pub struct PostingsListBlock {
    /// Document IDs in this postings list block
    doc_ids: Vec<u8>,

    /// The metadata for each document in this postings list block
    metadata: Vec<u8>,
}

impl PostingsListBlock {
    const LATEST_VERSION: u8 = 0;

    /// Size of the version and the length
    const LENGTH_SIZE: usize = size_of::<u8>();
    const VERSION_SIZE: usize = size_of_val(&Self::LATEST_VERSION);
    const HEADER_SIZE: usize = Self::VERSION_SIZE + Self::LENGTH_SIZE;

    /// Amount of bytes needed to store a document in the block
    const DOCUMENT_SIZE: usize = Self::DOC_ID_SIZE + term::Metadata::SIZE;
    /// Amount of bytes needed to store a doc id in the block
    const DOC_ID_SIZE: usize = size_of::<t_docId>();

    /// Create a new block builder. Does not allocate until documents are pushed.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new block builder and reseve space for `cap` documents
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            doc_ids: Vec::with_capacity(cap * PostingsListBlock::DOC_ID_SIZE),
            metadata: Vec::with_capacity(cap * term::Metadata::SIZE),
        }
    }

    /// Add a full document to the block
    pub fn push(&mut self, term: term::Document) {
        self.doc_ids.extend_from_slice(&term.doc_id.to_le_bytes());

        self.metadata
            .extend_from_slice(&term.metadata.field_mask.to_le_bytes());
        self.metadata
            .extend_from_slice(&term.metadata.frequency.to_le_bytes());
    }

    /// Check if this block is empty
    pub fn is_empty(&self) -> bool {
        debug_assert!(
            self.doc_ids.len() / Self::DOC_ID_SIZE == self.metadata.len() / term::Metadata::SIZE,
            "Document IDs and metadata buffers must have the same number of entries"
        );

        self.doc_ids.is_empty()
    }

    /// Serialize a postings list block into bytes for storage.
    ///
    /// # Layout
    /// A block holding two documents is laid out as follows:
    ///
    /// ```txt
    /// | 0    | 1   | [2,9]    | [10,17]  | [18,33]      | [34,51] | [52,67]      | [68,75] |
    /// | ---- | --- | -------- | -------- | ------------ | ------- | ------------ | ------- |
    /// | VERS | LEN | doc_id_1 | doc_id_2 | field_mask_1 | freq_1  | field_mask_2 | freq_2  |
    /// ```
    /// I.e. first comes the header containing the version (1 byte) and the length (1 byte),
    /// then the document IDs, (8 bytes each), then the metadata for each document
    /// which each consist of a field mask (16 bytes each) and a frequency (8 bytes each).
    ///
    /// The output is deterministic: given the same input, the same byte vector will be produced.
    /// This method asserts that the internal buffers are consistent and correctly sized.
    pub fn serialize(&self) -> Vec<u8> {
        let Self { doc_ids, metadata } = self;

        // Assert that the lengths of the data buffers are correct:
        // 1. They must be a multiple of the length of the items they contain
        debug_assert!(doc_ids.len().is_multiple_of(Self::DOC_ID_SIZE));
        debug_assert!(metadata.len().is_multiple_of(term::Metadata::SIZE));
        // 2. They must contain an equal number of items
        debug_assert_eq!(
            doc_ids.len() / Self::DOC_ID_SIZE,
            metadata.len() / term::Metadata::SIZE
        );

        let num_docs = doc_ids.len() / Self::DOC_ID_SIZE;

        let data_size = num_docs * Self::DOCUMENT_SIZE + Self::HEADER_SIZE;

        // 3. The lengths of the buffer must equal the total size
        //    minus the reserved space for the version and length
        debug_assert_eq!(
            doc_ids.len() + metadata.len(),
            data_size - Self::HEADER_SIZE
        );

        let mut data = Vec::with_capacity(data_size);

        data.push(Self::LATEST_VERSION);
        data.push(num_docs.try_into().expect(
            "PostingsListBlock can only serialize up to 255 docs; increase length size if needed",
        ));
        data.extend(doc_ids);
        data.extend(metadata);

        data
    }
}

impl From<term::Document> for PostingsListBlock {
    fn from(doc: term::Document) -> Self {
        let doc_ids = doc.doc_id.to_le_bytes().to_vec();
        let mut metadata = Vec::with_capacity(term::Metadata::SIZE);

        metadata.extend_from_slice(&doc.metadata.field_mask.to_le_bytes());
        metadata.extend_from_slice(&doc.metadata.frequency.to_le_bytes());

        Self { doc_ids, metadata }
    }
}

impl InvertedIndex {
    const KEY_DELIMITER: u8 = b'_';
    const COLUMN_FAMILY_NAME: &str = "fulltext";
    const PREFIX_EXTRACTOR_NAME: &str = "fulltext_prefix_extractor";
    const MERGE_OPERATOR_NAME: &str = "fulltext_merge_operator";

    /// Creates a new inverted index with the given Speedb database.
    pub fn new(database: SpeedbMultithreadedDatabase) -> Self {
        // Verify the column family exists
        database
            .cf_handle(Self::COLUMN_FAMILY_NAME)
            .expect("Inverted index column family should exist");

        InvertedIndex {
            database,
            cf_name: Self::COLUMN_FAMILY_NAME.to_string(),
        }
    }

    /// Strip everything after the last [`KEY_DELIMITER`], which is the key
    /// without the last doc_id, but keeping the delimiter.
    fn strip_after_last_delimiter(src: &[u8]) -> &[u8] {
        let last_key_delimiter_idx = src
            .iter()
            .rposition(|byte| *byte == Self::KEY_DELIMITER)
            .expect("slice doesn't contain KEY_DELIMITER");

        src.split_at(last_key_delimiter_idx + 1).0
    }

    /// Returns whether `src` contains a [`KEY_DELIMITER`].
    fn contains_key_delimiter(src: &[u8]) -> bool {
        src.contains(&Self::KEY_DELIMITER)
    }

    pub fn cf_descriptor(deleted_ids: DeletedIdsStore) -> ColumnFamilyDescriptor {
        let prefix_extractor = SliceTransform::create(
            Self::PREFIX_EXTRACTOR_NAME,
            Self::strip_after_last_delimiter,
            Some(Self::contains_key_delimiter),
        );

        let mut cf_options = SpeedbDbOptions::default();
        cf_options.set_merge_operator(
            Self::MERGE_OPERATOR_NAME,
            DeletedIdsMergeOperator::full_merge_fn(deleted_ids.clone()),
            DeletedIdsMergeOperator::partial_merge_fn(deleted_ids),
        );
        cf_options.set_prefix_extractor(prefix_extractor);
        cf_options.set_block_based_table_factory(&BlockBasedOptions::default());

        ColumnFamilyDescriptor::new(Self::COLUMN_FAMILY_NAME, cf_options)
    }

    /// Returns the Speedb column family handle for the inverted index.
    fn cf_handle(&self) -> Arc<BoundColumnFamily<'_>> {
        // SAFETY: we verified the column family exists in `new()`
        self.database.cf_handle(&self.cf_name).unwrap()
    }

    fn term_and_doc_key(term: &str, last_doc_id: Option<t_docId>) -> InvertedIndexKey<'_> {
        InvertedIndexKey { term, last_doc_id }
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
        let key = Self::term_and_doc_key(&term, Some(doc_id));
        let block: PostingsListBlock = term::Document {
            doc_id,
            metadata: term::Metadata {
                field_mask,
                frequency,
            },
        }
        .into();
        let block = block.serialize();

        self.database
            .put_cf(&self.cf_handle(), key.as_key(), block)?;

        Ok(())
    }

    /// Returns an iterator over the document IDs for the given term.
    pub fn term_iterator(
        &self,
        term: &str,
        field_mask: t_fieldMask,
        weight: f64,
    ) -> Result<
        InvIndIterator<'_, FilterMaskReader<PostingsListReader<'_, Speedb>>>,
        PostingReaderCreateError,
    > {
        let key = Self::term_and_doc_key(term, None).as_key();

        let iterator = self.database.iterator_cf(
            &self.cf_handle(),
            speedb::IteratorMode::From(&key, speedb::Direction::Forward),
        );
        let reader = PostingsListReader::new(iterator, term.to_string())?;
        let reader = FilterMaskReader::new(field_mask, reader);

        let iter = InvIndIterator::new(reader, RSIndexResult::virt().weight(weight), None);

        Ok(iter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_after_last_delimiter_basic() {
        let key = b"foo_40";
        let expected = b"foo_";
        assert_eq!(InvertedIndex::strip_after_last_delimiter(key), expected);
    }

    #[test]
    fn test_strip_after_last_delimiter_multiple_delimiters() {
        let key = b"alpha_beta_gamma_123";
        let expected = b"alpha_beta_gamma_";
        assert_eq!(InvertedIndex::strip_after_last_delimiter(key), expected);
    }

    #[test]
    #[should_panic(expected = "slice doesn't contain KEY_DELIMITER")]
    fn test_strip_after_last_delimiter_panics_without_delimiter() {
        InvertedIndex::strip_after_last_delimiter(b"nodelimiterhere");
    }

    #[test]
    fn postings_list_block_roundtrip() {
        let mut block = PostingsListBlock::default();

        let doc1 = term::Document {
            doc_id: 1,
            metadata: term::Metadata {
                field_mask: 0xDEADBEEF,
                frequency: 42,
            },
        };

        let doc2 = term::Document {
            doc_id: 2,
            metadata: term::Metadata {
                field_mask: 0xCAFEBABE,
                frequency: 84,
            },
        };

        block.push(doc1.clone());
        block.push(doc2.clone());

        let block = term::block::ArchivedBlock::from_bytes(block.serialize().into());

        assert_eq!(term::Document::from(block.get(0).unwrap()), doc1);
        assert_eq!(term::Document::from(block.get(1).unwrap()), doc2);
        assert!(block.get(2).is_none());
    }
}
