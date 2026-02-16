use std::mem::size_of;

use super::{TagDocument, archive::ArchivedTagBlock};
use crate::index_spec::inverted_index::block_traits::{self, ArchivedBlock};
use crate::value_traits::ValueExt;

/// A block-based postings list for tags that stores only document IDs.
/// This is much more compact than the full-text postings list which also stores
/// field masks and frequencies.
#[derive(Clone, Default)]
pub struct TagPostingsListBlock {
    /// Document IDs in this postings list block
    doc_ids: Vec<u8>,
}

impl TagPostingsListBlock {
    const LATEST_VERSION: u8 = 0;

    /// Size of the version and the length
    const LENGTH_SIZE: usize = size_of::<u16>();
    const VERSION_SIZE: usize = size_of_val(&Self::LATEST_VERSION);
    const HEADER_SIZE: usize = Self::VERSION_SIZE + Self::LENGTH_SIZE;

    /// Amount of bytes needed to store a document in the block (just the doc_id)
    const DOCUMENT_SIZE: usize = TagDocument::SIZE;
}

impl block_traits::SerializableBlock for TagPostingsListBlock {
    type Document = TagDocument;

    /// Create a new block builder. Does not allocate until documents are pushed.
    fn new() -> Self {
        Self::default()
    }

    /// Create a new block builder and reserve space for `cap` documents
    fn with_capacity(cap: usize) -> Self {
        Self {
            doc_ids: Vec::with_capacity(cap * Self::DOCUMENT_SIZE),
        }
    }

    /// Add a tag document to the block
    fn push(&mut self, doc: TagDocument) {
        self.doc_ids.extend_from_slice(&doc.doc_id.to_le_bytes());
    }

    fn is_empty(&self) -> bool {
        self.doc_ids.is_empty()
    }

    /// Serialize a tag postings list block into bytes for storage.
    ///
    /// # Layout
    /// A block holding two documents is laid out as follows:
    ///
    /// ```txt
    /// | 0    | 1-2  | [3,10]   | [11,18]  |
    /// | ---- | ---- | -------- | -------- |
    /// | VERS | LEN  | doc_id_1 | doc_id_2 |
    /// ```
    /// I.e. first comes the header containing the version (1 byte) and the length (2 bytes),
    /// then the document IDs (8 bytes each).
    ///
    /// # Returns
    /// A byte vector containing the serialized block
    fn serialize(&self) -> Vec<u8> {
        let Self { doc_ids } = self;

        let num_docs = doc_ids.len() / Self::DOCUMENT_SIZE;

        let data_size = num_docs * Self::DOCUMENT_SIZE + Self::HEADER_SIZE;

        // The length of the buffer must equal the total size minus the header
        debug_assert_eq!(doc_ids.len(), data_size - Self::HEADER_SIZE);

        let mut data = Vec::with_capacity(data_size);

        data.push(Self::LATEST_VERSION);
        let num_docs_u16: u16 = num_docs
            .try_into()
            .expect("TagPostingsListBlock can only serialize up to 65535 docs");
        data.extend_from_slice(&num_docs_u16.to_le_bytes());
        data.extend(doc_ids);

        data
    }
}

impl From<TagDocument> for TagPostingsListBlock {
    fn from(doc: TagDocument) -> Self {
        let doc_ids = doc.doc_id.to_le_bytes().to_vec();
        Self { doc_ids }
    }
}

impl From<ArchivedTagBlock> for TagPostingsListBlock {
    fn from(archived: ArchivedTagBlock) -> Self {
        let num_docs = archived.num_docs() as usize;

        let mut doc_ids = Vec::with_capacity(num_docs * Self::DOCUMENT_SIZE);

        for doc in archived.iter() {
            doc_ids.extend_from_slice(&doc.doc_id().to_le_bytes());
        }

        Self { doc_ids }
    }
}

impl ValueExt for TagPostingsListBlock {
    type ArchivedType<'a> = ArchivedTagBlock;

    fn as_speedb_value(&self) -> Vec<u8> {
        <Self as block_traits::SerializableBlock>::serialize(self)
    }

    fn archive_from_speedb_value(value: &[u8]) -> Self::ArchivedType<'_> {
        ArchivedTagBlock::from_bytes(value.into())
    }
}
