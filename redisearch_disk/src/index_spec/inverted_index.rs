pub mod term;
pub use term::reader::{
    Reader as PostingsListReader, ReaderCreateError as PostingReaderCreateError,
};

use std::mem::size_of;

use ffi::{t_docId, t_fieldMask};
use inverted_index::{FilterMaskReader, RSIndexResult};
use rqe_iterators::inverted_index::InvIndIterator;

use super::DeletedIdsStore;
use crate::{merge_op::DeletedIdsMergeOperator, value_traits::ValueExt};

use speedb::{
    BlockBasedOptions, ColumnFamilyDescriptor, Options as SpeedbDbOptions, SliceTransform,
    WriteOptions,
};

use crate::{
    database::{ColumnFamilyGuard, Speedb, SpeedbMultithreadedDatabase},
    key_traits::AsKeyExt,
};

use crate::metrics::CFMetrics;

/// An inverted index maps terms to the documents which contain the term.
pub struct InvertedIndex {
    /// The column family handle for the inverted index.
    /// Must be declared before database handle
    cf: ColumnFamilyGuard,

    /// Write options for the inverted index
    write_options: WriteOptions,

    /// The Speedb database where we store the inverted index.
    database: SpeedbMultithreadedDatabase,
}

/// Key structure for inverted index entries.
///
/// Key format: `prefix + delimiter (0x00) + doc_id (8 bytes big-endian)`
///
/// Big-endian encoding is used for doc_id so that lexicographic ordering matches numeric ordering,
/// enabling efficient range scans and seeks in the database.
struct InvertedIndexKey<'term> {
    prefix: &'term str,
    last_doc_id: Option<t_docId>,
}

impl InvertedIndexKey<'_> {
    /// Delimiter byte between term and doc_id. Using \x00 is safe because:
    /// - UTF-8 never uses \x00 in multi-byte sequences (only for NUL character itself)
    /// - Ensures "term\x00..." < "term_...\x00..." so reverse seeks stay within term bounds
    const TERM_DELIMITER: u8 = 0x00;

    /// Size of the delimiter byte between term and doc_id
    pub(crate) const DELIMITER_SIZE: usize = std::mem::size_of_val(&Self::TERM_DELIMITER);

    /// Size of the binary-encoded document ID suffix in keys (delimiter + 8 bytes for u64)
    pub(crate) const DOC_ID_KEY_SIZE: usize = Self::DELIMITER_SIZE + std::mem::size_of::<t_docId>();
}

impl<'term> AsKeyExt for InvertedIndexKey<'term> {
    fn as_key(&self) -> Vec<u8> {
        // Pre-calculate capacity: term + optional (delimiter + 8-byte doc_id)
        let capacity = self.prefix.len()
            + if self.last_doc_id.is_some() {
                Self::DOC_ID_KEY_SIZE
            } else {
                0
            };
        let mut key = Vec::with_capacity(capacity);

        key.extend_from_slice(self.prefix.as_bytes());

        if let Some(doc_id) = self.last_doc_id {
            key.push(Self::TERM_DELIMITER);
            key.extend_from_slice(&doc_id.to_be_bytes());
        }

        key
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
}

impl ValueExt for PostingsListBlock {
    type ArchivedType<'a> = term::block::ArchivedBlock;

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
    fn as_speedb_value(&self) -> Vec<u8> {
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

    fn archive_from_speedb_value(value: &[u8]) -> Self::ArchivedType<'_> {
        term::block::ArchivedBlock::from_bytes(value.into())
    }
}

impl From<term::block::ArchivedBlock> for PostingsListBlock {
    fn from(archived: term::block::ArchivedBlock) -> Self {
        let num_docs = archived.num_docs() as usize;

        let mut doc_ids = Vec::with_capacity(num_docs * Self::DOC_ID_SIZE);
        let mut metadata = Vec::with_capacity(num_docs * term::Metadata::SIZE);

        for term in archived.iter() {
            doc_ids.extend_from_slice(&term.doc_id().to_le_bytes());
            metadata.extend_from_slice(&term.field_mask().to_le_bytes());
            metadata.extend_from_slice(&term.frequency().to_le_bytes());
        }

        Self { doc_ids, metadata }
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
    const COLUMN_FAMILY_NAME: &str = "fulltext";
    const PREFIX_EXTRACTOR_NAME: &str = "fulltext_prefix_extractor";
    const MERGE_OPERATOR_NAME: &str = "fulltext_merge_operator";

    /// Creates a new inverted index with the given Speedb database.
    pub fn new(database: SpeedbMultithreadedDatabase) -> Self {
        // SAFETY: The database field is declared after cf in the struct,
        // so cf will be dropped first, ensuring proper cleanup order.
        let cf = unsafe { database.cf_guard(Self::COLUMN_FAMILY_NAME) }
            .expect("Inverted index column family should exist");

        let mut write_options = WriteOptions::default();
        write_options.disable_wal(true);
        InvertedIndex {
            cf,
            write_options,
            database,
        }
    }

    /// Strip the 8-byte doc_id suffix from the key, leaving just the term.
    /// Keys are structured as: [term bytes][8-byte big-endian doc_id]
    fn strip_doc_id_suffix(src: &[u8]) -> &[u8] {
        debug_assert!(
            src.len() >= InvertedIndexKey::DOC_ID_KEY_SIZE,
            "key must be at least {} bytes to contain a doc_id",
            InvertedIndexKey::DOC_ID_KEY_SIZE
        );
        &src[..src.len() - InvertedIndexKey::DOC_ID_KEY_SIZE]
    }

    /// Returns whether `src` is long enough to contain a doc_id suffix.
    fn has_doc_id_suffix(src: &[u8]) -> bool {
        src.len() >= InvertedIndexKey::DOC_ID_KEY_SIZE
    }

    pub fn cf_descriptor(deleted_ids: DeletedIdsStore) -> ColumnFamilyDescriptor {
        let prefix_extractor = SliceTransform::create(
            Self::PREFIX_EXTRACTOR_NAME,
            Self::strip_doc_id_suffix,
            Some(Self::has_doc_id_suffix),
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

    fn term_and_doc_key(prefix: &str, last_doc_id: Option<t_docId>) -> InvertedIndexKey<'_> {
        InvertedIndexKey {
            prefix,
            last_doc_id,
        }
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

        self.database.put_cf_opt(
            &self.cf,
            key.as_key(),
            block.as_speedb_value(),
            &self.write_options,
        )?;

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
            &self.cf,
            speedb::IteratorMode::From(&key, speedb::Direction::Forward),
        );
        let reader = PostingsListReader::new(iterator, term.to_string())?;
        let reader = FilterMaskReader::new(field_mask, reader);

        let iter = InvIndIterator::new(reader, RSIndexResult::virt().weight(weight), None);

        Ok(iter)
    }

    /// Collect metrics for the text inverted index column family.
    pub fn collect_metrics(&self) -> crate::metrics::CFMetrics {
        CFMetrics::collect(&self.database, &self.cf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_doc_id_suffix_basic() {
        // "foo" + delimiter + 8 bytes of doc_id
        let key = b"foo\x00\x00\x00\x00\x00\x00\x00\x00\x28"; // doc_id = 40
        let expected = b"foo";
        assert_eq!(InvertedIndex::strip_doc_id_suffix(key), expected);
    }

    #[test]
    fn test_strip_doc_id_suffix_with_underscore_in_term() {
        // "alpha_beta" + delimiter + 8 bytes of doc_id
        let key = b"alpha_beta\x00\x00\x00\x00\x00\x00\x00\x00\x7B"; // doc_id = 123
        let expected = b"alpha_beta";
        assert_eq!(InvertedIndex::strip_doc_id_suffix(key), expected);
    }

    #[test]
    fn test_strip_doc_id_suffix_with_delimiter_byte_in_doc_id() {
        // Test that doc_id containing 0x5F ('_') doesn't break extraction
        // doc_id = 95 has 0x5F as its last byte
        let key = b"term\x00\x00\x00\x00\x00\x00\x00\x00\x5F"; // doc_id = 95
        let expected = b"term";
        assert_eq!(InvertedIndex::strip_doc_id_suffix(key), expected);
    }

    #[test]
    fn test_has_doc_id_suffix() {
        // With delimiter, DOC_ID_KEY_SIZE is 9 bytes (1 delimiter + 8 doc_id)
        assert!(InvertedIndex::has_doc_id_suffix(
            b"term\x00\x00\x00\x00\x00\x00\x00\x00\x01"
        ));
        assert!(InvertedIndex::has_doc_id_suffix(
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x01"
        )); // just 9 bytes (delimiter + doc_id)
        assert!(!InvertedIndex::has_doc_id_suffix(b"short")); // less than 9 bytes
        assert!(!InvertedIndex::has_doc_id_suffix(b"")); // empty
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

        let block = PostingsListBlock::archive_from_speedb_value(&block.as_speedb_value());

        assert_eq!(term::Document::from(block.get(0).unwrap()), doc1);
        assert_eq!(term::Document::from(block.get(1).unwrap()), doc2);
        assert!(block.get(2).is_none());
    }
}
