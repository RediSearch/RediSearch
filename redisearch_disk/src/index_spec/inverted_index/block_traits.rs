use ffi::t_docId;
use inverted_index::RSIndexResult;
use speedb::ColumnFamilyDescriptor;

use super::DeletedIdsStore;

/// Trait for archived document entries in a block.
/// This allows different block types (term, tag) to provide their own document representations.
pub trait ArchivedDocument {
    /// Get the document ID from this archived document
    fn doc_id(&self) -> t_docId;

    /// Populate an RSIndexResult with data from this archived document.
    /// For term documents, this includes field_mask. For tag documents, it's just the doc_id.
    fn populate_result<'index>(&self, result: &mut RSIndexResult<'index>);
}

/// Trait for archived blocks that store documents.
/// This allows different block types to be used with the same reader implementation.
pub trait ArchivedBlock: Sized {
    /// The type of document stored in this block (with a lifetime parameter)
    type Document<'a>: ArchivedDocument
    where
        Self: 'a;

    /// The type used for indexing within the block (u8 for term blocks, u16 for tag blocks)
    type Index: BlockIndex;

    /// Create a new archived block from owned bytes
    fn from_bytes(bytes: Box<[u8]>) -> Self;

    /// Get the number of documents in this block
    fn num_docs(&self) -> Self::Index;

    /// Get the document at the given index, if it exists
    fn get(&self, index: Self::Index) -> Option<Self::Document<'_>>;

    /// Get the document at the given index without bounds checking
    ///
    /// # Safety
    /// The caller must ensure that `index < num_docs()`
    fn get_unchecked(&self, index: Self::Index) -> Self::Document<'_>;

    /// Get the last document in the block, if it exists
    fn last(&self) -> Option<Self::Document<'_>>;
}

/// Trait for block index types (u8, u16, etc.)
/// This allows blocks to use different index sizes based on their capacity.
pub trait BlockIndex:
    Copy + Ord + TryFrom<usize> + Into<usize> + num_traits::PrimInt + num_traits::FromPrimitive
{
}

impl BlockIndex for u8 {}

impl BlockIndex for u16 {}

/// Trait for serializable postings list blocks.
/// This allows different block types to define their own serialization format.
pub trait SerializableBlock {
    /// The type of document that can be pushed into this block
    type Document;

    /// Create a new empty block
    fn new() -> Self;

    /// Create a new block with capacity for `cap` documents
    fn with_capacity(cap: usize) -> Self;

    /// Add a document to the block
    fn push(&mut self, doc: Self::Document);

    /// Serialize the block into bytes for storage
    fn serialize(self) -> Vec<u8>;
}

/// Configuration trait for inverted index types.
/// This allows different index types (term, tag) to specify their column family name,
/// block types, and how to create their column family descriptor.
pub trait IndexConfig {
    /// The type of block used for serialization (e.g., PostingsListBlock, TagPostingsListBlock)
    type SerializableBlock: SerializableBlock;

    /// The type of archived block used for reading (e.g., ArchivedBlock, ArchivedTagBlock)
    type ArchivedBlock: ArchivedBlock;

    /// The name of the column family for this index type
    const COLUMN_FAMILY_NAME: &'static str;

    /// Creates a column family descriptor for this index type.
    /// Implementations can configure merge operators and other options as needed.
    fn cf_descriptor(deleted_ids: Option<DeletedIdsStore>) -> ColumnFamilyDescriptor;
}
