pub mod full_term_block;

use std::{mem::size_of, sync::Arc};

use ffi::{IndexFlags_Index_DocIdsOnly, t_docId, t_fieldMask};
use inverted_index::{FilterMaskReader, IndexReader, RSIndexResult};
use rqe_iterators::inverted_index::InvIndIterator;
use speedb::{BoundColumnFamily, DBIteratorWithThreadMode};
use tracing::error;

use crate::{
    document_id_key::DocumentIdKey,
    search_disk::{AsKeyExt, FromKeyExt, Speedb, SpeedbMultithreadedDatabase},
};
use full_term_block::ArchivedFullTermBlock;

/// Delimiter used in inverted index keys between term and last document ID
const KEY_DELIMETER: u8 = b'_';

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

struct InvertedIndexKey<'term> {
    term: &'term str,
    last_doc_id: Option<t_docId>,
}

impl<'term> AsKeyExt for InvertedIndexKey<'term> {
    fn as_key(&self) -> Vec<u8> {
        let key = if let Some(last_doc_id) = self.last_doc_id {
            let last_doc_id: DocumentIdKey = last_doc_id.into();
            format!("{}_{}", self.term, last_doc_id)
        } else {
            format!("{}_", self.term)
        };

        key.as_bytes().to_vec()
    }
}

/// We use a block-based postings list to store document IDs and metadata for each term. This allows
/// us to quickly jump over large sections of the postings list when searching.
#[derive(Clone, Default)]
pub struct PostingsListBlock {
    /// Document IDs in this postings list block
    doc_ids: Vec<u8>,

    /// The metadata for each term in this postings list block
    metadata: Vec<u8>,
}

impl PostingsListBlock {
    const LATEST_VERSION: u8 = 0;

    /// Size of the version and the length
    const LENGTH_SIZE: usize = size_of::<u8>();
    const VERSION_SIZE: usize = size_of_val(&Self::LATEST_VERSION);
    const HEADER_SIZE: usize = Self::VERSION_SIZE + Self::LENGTH_SIZE;

    /// Amount of bytes needed to store a term in the block
    const TERM_SIZE: usize = Self::DOC_ID_SIZE + FullTermMetadata::SIZE;
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
            metadata: Vec::with_capacity(cap * FullTermMetadata::SIZE),
        }
    }

    /// Add a full term document to the block
    pub fn push(&mut self, term: FullTermDocument) {
        self.doc_ids.extend_from_slice(&term.doc_id.to_le_bytes());

        self.metadata
            .extend_from_slice(&term.metadata.field_mask.to_le_bytes());
        self.metadata
            .extend_from_slice(&term.metadata.frequency.to_le_bytes());
    }

    /// Check if this block is empty
    pub fn is_empty(&self) -> bool {
        debug_assert!(
            self.doc_ids.len() / Self::DOC_ID_SIZE == self.metadata.len() / FullTermMetadata::SIZE,
            "Document IDs and metadata buffers must have the same number of entries"
        );

        self.doc_ids.is_empty()
    }

    /// Serialize a postings list block into bytes for storage.
    ///
    /// # Layout
    /// A block holding two terms is laid out as follows:
    ///
    /// ```txt
    /// | 0    | 1   | [2,9]    | [10,17]  | [18,33]      | [34,51] | [52,67]      | [68,75] |
    /// | ---- | --- | -------- | -------- | ------------ | ------- | ------------ | ------- |
    /// | VERS | LEN | doc_id_1 | doc_id_2 | field_mask_1 | freq_1  | field_mask_2 | freq_2  |
    /// ```
    /// I.e. first comes the header containing the version (1 byte) and the length (1 byte),
    /// then the document IDs, (8 bytes each), then the metadata for each term
    /// which each consist of a field mask (16 bytes each) and a frequency (8 bytes each).
    ///
    /// The output is deterministic: given the same input, the same byte vector will be produced.
    /// This method asserts that the internal buffers are consistent and correctly sized.
    pub fn serialize(&self) -> Vec<u8> {
        let Self { doc_ids, metadata } = self;

        // Assert that the lengths of the data buffers are correct:
        // 1. They must be a multiple of the length of the items they contain
        debug_assert!(doc_ids.len().is_multiple_of(Self::DOC_ID_SIZE));
        debug_assert!(metadata.len().is_multiple_of(FullTermMetadata::SIZE));
        // 2. They must contain an equal number of items
        debug_assert_eq!(
            doc_ids.len() / Self::DOC_ID_SIZE,
            metadata.len() / FullTermMetadata::SIZE
        );

        let num_terms = doc_ids.len() / Self::DOC_ID_SIZE;

        let data_size = num_terms * Self::TERM_SIZE + Self::HEADER_SIZE;

        // 3. The lengths of the buffer must equal the total size
        //    minus the reserved space for the version and length
        debug_assert_eq!(
            doc_ids.len() + metadata.len(),
            data_size - Self::HEADER_SIZE
        );

        let mut data = Vec::with_capacity(data_size);

        data.push(Self::LATEST_VERSION);
        data.push(num_terms.try_into().expect(
            "PostingsListBlock can only serialize up to 255 terms; increase length size if needed",
        ));
        data.extend(doc_ids);
        data.extend(metadata);

        data
    }
}

/// A document in a postings list, including its ID and associated metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FullTermDocument {
    pub doc_id: t_docId,
    pub metadata: FullTermMetadata,
}

impl From<FullTermDocument> for PostingsListBlock {
    fn from(full_term: FullTermDocument) -> Self {
        let doc_ids = full_term.doc_id.to_le_bytes().to_vec();
        let mut metadata = Vec::with_capacity(FullTermMetadata::SIZE);

        metadata.extend_from_slice(&full_term.metadata.field_mask.to_le_bytes());
        metadata.extend_from_slice(&full_term.metadata.frequency.to_le_bytes());

        Self { doc_ids, metadata }
    }
}

/// Metadata associated with a term in a document, including the fields it appears in and its frequency.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FullTermMetadata {
    pub field_mask: u128,
    pub frequency: u64,
}

impl FullTermMetadata {
    pub const SIZE: usize = Self::FIELD_MASK_SIZE + Self::FREQUENCY_SIZE;
    pub const FIELD_MASK_SIZE: usize = size_of::<u128>();
    pub const FREQUENCY_SIZE: usize = size_of::<u64>();
}

impl InvertedIndex {
    /// Creates a new inverted index with the given Speedb database.
    pub fn new(database: SpeedbMultithreadedDatabase, cf_name: String) -> Self {
        // Verify the column family exists
        database
            .cf_handle(&cf_name)
            .expect("Inverted index column family should exist");

        InvertedIndex { database, cf_name }
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
        let block: PostingsListBlock = FullTermDocument {
            doc_id,
            metadata: FullTermMetadata {
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
    ) -> Result<InvIndIterator<'_, FilterMaskReader<PostingsListReader<'_, Speedb>>>, speedb::Error>
    {
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

/// An index reader for a postings list stored in blocks. This reader allows for efficient
/// seeking and iteration over term documents in an index.
pub struct PostingsListReader<'iterator, DBAccess: speedb::DBAccess> {
    /// The underlying Speedb iterator for reading the postings list blocks.
    iterator: DBIteratorWithThreadMode<'iterator, DBAccess>,

    /// The current block being read.
    current_block: Option<ArchivedFullTermBlock>,

    /// The key of the current block being read.
    current_block_key: Option<Box<[u8]>>,

    /// The index within the current block.
    block_index: u8,

    /// An estimate of the number of unique documents in the postings list.
    estimate: u64,

    /// The term we are iterating over.
    term: String,

    /// The key prefix for the term in the inverted index.
    key_prefix: Vec<u8>,
}

impl<'iterator, DBAccess: speedb::DBAccess> PostingsListReader<'iterator, DBAccess> {
    /// Creates a new `PostingsListReader` for the given term using the provided Speedb iterator.
    fn new(
        mut iterator: DBIteratorWithThreadMode<'iterator, DBAccess>,
        term: String,
    ) -> Result<Self, speedb::Error> {
        let key_prefix = InvertedIndexKey {
            term: &term,
            last_doc_id: None,
        }
        .as_key();

        let estimate = Self::get_estimate(&mut iterator, &key_prefix, &term)?;

        // Reset the iterator to the first block for the term
        iterator.set_mode(speedb::IteratorMode::From(
            &key_prefix,
            speedb::Direction::Forward,
        ));

        Ok(Self {
            iterator,
            term,
            key_prefix,
            current_block: None,
            current_block_key: None,
            block_index: 0,
            estimate,
        })
    }

    /// Advances to the next block in the postings list. Returns `true` if a new valid block was
    /// found, or `false` if there are no more blocks for the current term.
    fn next_block(&mut self) -> bool {
        let (key, value) = match self.iterator.next() {
            Some(Ok((key, value)))
                // Make sure we are still in the same term by checking the key's prefix (everything
                // up to the last underscore) matches the term's key prefix. We specifically
                // include the underscore to avoid matching terms that are prefixes of other terms.
                // We also want to use the last underscore because a term may contain underscores
                // which might affect the number of underscores. But the last underscore is always
                // the separator between the term and the last document ID.
                if key.iter().rposition(|&c| {
                    c == KEY_DELIMETER
                }).map(|p| &key[..=p])
                    == Some(&self.key_prefix) =>
            {
                (key, value)
            }
            Some(Err(error)) => {
                let last_block_document_id = self
                    .current_block_key
                    .as_ref()
                    .map(|key| &key[self.key_prefix.len()..])
                    .map(DocumentIdKey::from_key);

                error!(
                    error = &error as &dyn std::error::Error,
                    ?last_block_document_id,
                    "Error reading postings list block",
                );
                panic!("Error reading postings list block: {}", error);
            }
            _ => {
                self.current_block = None;
                self.block_index = 0;
                return false;
            }
        };

        let block = ArchivedFullTermBlock::from_bytes(value);

        self.current_block = Some(block);
        self.current_block_key = Some(key);
        self.block_index = 0;

        true
    }

    /// Get the estimated number of documents for the iterator over the current term.
    fn get_estimate(
        iterator: &mut DBIteratorWithThreadMode<'iterator, DBAccess>,
        key_prefix: &[u8],
        term: &str,
    ) -> Result<u64, speedb::Error> {
        let Some(next) = iterator.next() else {
            return Ok(0);
        };

        let (key, value) = next?;

        // Make sure we are still in the same term by checking the key's prefix (everything up to
        // the last underscore) matches the term's key prefix. We specifically include the
        // underscore to avoid matching terms that are prefixes of other terms. We also want to use
        // the last underscore because a term may contain underscores which might affect the number
        // of underscores. But the last underscore is always the separator between the term and the
        // last document ID.
        if key
            .iter()
            .rposition(|&c| c == KEY_DELIMETER)
            .map(|p| &key[..=p])
            != Some(key_prefix)
        {
            return Ok(0);
        }

        // Get the first document ID in the first block
        let block = ArchivedFullTermBlock::from_bytes(value);
        let first_id = block.get_unchecked(0).doc_id();

        // Now seek to the end to find the last document ID. This ID is stored as a suffix on the
        // entry's key.
        let end_key = InvertedIndexKey {
            term,
            last_doc_id: Some(u64::MAX),
        }
        .as_key();

        iterator.set_mode(speedb::IteratorMode::From(
            &end_key,
            speedb::Direction::Reverse,
        ));

        // We know at minimum that we'll match the same block. So it is safe to unwrap here.
        let (key, _value) = iterator.next().unwrap()?;

        let key = &key[key_prefix.len()..];
        let last_doc_id = DocumentIdKey::from_key(key).as_num();

        Ok(last_doc_id - first_id + 1)
    }
}

impl<'index, 'iterator, DBAccess: speedb::DBAccess> IndexReader<'index>
    for PostingsListReader<'iterator, DBAccess>
{
    fn next_record(
        &mut self,
        result: &mut inverted_index::RSIndexResult<'index>,
    ) -> std::io::Result<bool> {
        if self.current_block.is_none() && !self.next_block() {
            return Ok(false);
        }

        // It is safe t unwrap here because `Self::next_block` ensures we have a block.
        let block = self.current_block.as_ref().unwrap();

        let term = block.get_unchecked(self.block_index);
        result.doc_id = term.doc_id();
        result.field_mask = term.field_mask();

        self.block_index += 1;

        if self.block_index >= block.num_terms() {
            self.next_block();
        }

        Ok(true)
    }

    fn seek_record(
        &mut self,
        doc_id: t_docId,
        result: &mut inverted_index::RSIndexResult<'index>,
    ) -> std::io::Result<bool> {
        if !self.skip_to(doc_id) {
            return Ok(false);
        }

        // It is safe t unwrap here because `Self::skip_to` ensures we have a block.
        let block = self.current_block.as_ref().unwrap();

        // Optimization to avoid binary searching if we are already at the correct index
        if block.get_unchecked(self.block_index).doc_id() != doc_id {
            self.block_index = block
                .binary_search_by_key(self.block_index, &doc_id, |entry| entry.doc_id())
                .unwrap_or_else(|insert_pos| insert_pos);
        };

        let Some(entry) = block.get(self.block_index) else {
            self.block_index = block.num_terms() - 1;
            return Ok(false);
        };

        result.doc_id = entry.doc_id();
        result.field_mask = entry.field_mask();

        self.block_index += 1;

        if self.block_index >= block.num_terms() {
            self.next_block();
        }

        Ok(true)
    }

    fn skip_to(&mut self, doc_id: t_docId) -> bool {
        // Don't seek if we are already in the correct block
        if let Some(last_entry) = self
            .current_block
            .as_ref()
            .and_then(ArchivedFullTermBlock::last)
            && last_entry.doc_id() >= doc_id
        {
            return true;
        }

        self.iterator.set_mode(speedb::IteratorMode::From(
            &InvertedIndexKey {
                term: &self.term,
                last_doc_id: Some(doc_id),
            }
            .as_key(),
            speedb::Direction::Forward,
        ));

        self.next_block()
    }

    fn reset(&mut self) {
        self.iterator.set_mode(speedb::IteratorMode::From(
            &self.key_prefix,
            speedb::Direction::Forward,
        ));
        self.next_block();
    }

    fn unique_docs(&self) -> u64 {
        self.estimate
    }

    fn has_duplicates(&self) -> bool {
        // We never store duplicates in our postings lists.
        false
    }

    fn flags(&self) -> ffi::IndexFlags {
        IndexFlags_Index_DocIdsOnly
    }

    fn needs_revalidation(&self) -> bool {
        // We own the list of IDs, so they won't change out from under us.
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postings_list_block_roundtrip() {
        let mut block = PostingsListBlock::default();

        let term1 = FullTermDocument {
            doc_id: 1,
            metadata: FullTermMetadata {
                field_mask: 0xDEADBEEF,
                frequency: 42,
            },
        };

        let term2 = FullTermDocument {
            doc_id: 2,
            metadata: FullTermMetadata {
                field_mask: 0xCAFEBABE,
                frequency: 84,
            },
        };

        block.push(term1.clone());
        block.push(term2.clone());

        let block = ArchivedFullTermBlock::from_bytes(block.serialize().into());

        assert_eq!(FullTermDocument::from(block.get(0).unwrap()), term1);
        assert_eq!(FullTermDocument::from(block.get(1).unwrap()), term2);
        assert!(block.get(2).is_none());
    }
}
