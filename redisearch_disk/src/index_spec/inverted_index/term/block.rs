use ffi::t_docId;
use std::mem::size_of;

use super::{Document, Metadata, archive::ArchivedBlock};
use crate::index_spec::inverted_index::block_traits;
use crate::value_traits::ValueExt;

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

    /// Amount of bytes needed to store a doc id in the block
    const DOC_ID_SIZE: usize = size_of::<t_docId>();
    /// Amount of bytes needed to store a document in the block
    const DOCUMENT_SIZE: usize = Self::DOC_ID_SIZE + Metadata::SIZE;

    /// Check if this block is empty
    pub fn is_empty(&self) -> bool {
        debug_assert!(
            self.doc_ids.len() / Self::DOC_ID_SIZE == self.metadata.len() / Metadata::SIZE,
            "Document IDs and metadata buffers must have the same number of entries"
        );

        self.doc_ids.is_empty()
    }
}

impl From<Document> for PostingsListBlock {
    fn from(doc: Document) -> Self {
        let doc_ids = doc.doc_id.to_le_bytes().to_vec();
        let mut metadata = Vec::with_capacity(Metadata::SIZE);

        metadata.extend_from_slice(&doc.metadata.field_mask.to_le_bytes());
        metadata.extend_from_slice(&doc.metadata.frequency.to_le_bytes());

        Self { doc_ids, metadata }
    }
}

impl From<ArchivedBlock> for PostingsListBlock {
    fn from(archived: ArchivedBlock) -> Self {
        let num_docs = archived.num_docs() as usize;

        let mut doc_ids = Vec::with_capacity(num_docs * Self::DOC_ID_SIZE);
        let mut metadata = Vec::with_capacity(num_docs * Metadata::SIZE);

        for doc in archived.iter() {
            doc_ids.extend_from_slice(&doc.doc_id().to_le_bytes());
            metadata.extend_from_slice(&doc.field_mask().to_le_bytes());
            metadata.extend_from_slice(&doc.frequency().to_le_bytes());
        }

        Self { doc_ids, metadata }
    }
}

// Implement the SerializableBlock trait for PostingsListBlock
impl block_traits::SerializableBlock for PostingsListBlock {
    type Document = Document;

    /// Create a new block builder. Does not allocate until documents are pushed.
    fn new() -> Self {
        Self::default()
    }

    /// Create a new block builder and reserve space for `cap` documents
    fn with_capacity(cap: usize) -> Self {
        Self {
            doc_ids: Vec::with_capacity(cap * Self::DOC_ID_SIZE),
            metadata: Vec::with_capacity(cap * Metadata::SIZE),
        }
    }

    /// Add a full document to the block
    fn push(&mut self, doc: Self::Document) {
        self.doc_ids.extend_from_slice(&doc.doc_id.to_le_bytes());

        self.metadata
            .extend_from_slice(&doc.metadata.field_mask.to_le_bytes());
        self.metadata
            .extend_from_slice(&doc.metadata.frequency.to_le_bytes());
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
    fn serialize(&self) -> Vec<u8> {
        let Self { doc_ids, metadata } = self;

        // Assert that the lengths of the data buffers are correct:
        // 1. They must be a multiple of the length of the items they contain
        debug_assert!(doc_ids.len().is_multiple_of(Self::DOC_ID_SIZE));
        debug_assert!(metadata.len().is_multiple_of(Metadata::SIZE));
        // 2. They must contain an equal number of items
        debug_assert_eq!(
            doc_ids.len() / Self::DOC_ID_SIZE,
            metadata.len() / Metadata::SIZE
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

impl ValueExt for PostingsListBlock {
    type ArchivedType<'a> = ArchivedBlock;

    fn as_speedb_value(&self) -> Vec<u8> {
        <Self as block_traits::SerializableBlock>::serialize(self)
    }

    fn archive_from_speedb_value(value: &[u8]) -> Self::ArchivedType<'_> {
        ArchivedBlock::from_bytes(value.into())
    }
}
