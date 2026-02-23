use ffi::{IndexFlags_Index_DocIdsOnly, t_docId};
use inverted_index::IndexReader;
use speedb::DBIteratorWithThreadMode;
use thiserror::Error;
use tracing::error;

use crate::{
    document_id_key::DocumentIdKey,
    key_traits::{AsKeyExt, FromKeyExt},
};

use super::{
    InvertedIndexKey,
    block_traits::{ArchivedBlock, ArchivedDocument},
};

/// Checks if a key belongs to the given term by verifying that the key's prefix
/// (everything except the doc_id suffix) matches the expected term key prefix.
fn key_belongs_to_term(key: &[u8], term_key_prefix: &[u8]) -> bool {
    key.len() >= InvertedIndexKey::DOC_ID_KEY_SIZE
        && &key[..key.len() - InvertedIndexKey::DOC_ID_KEY_SIZE] == term_key_prefix
}

/// Error type for posting list construction failures
#[derive(Debug, Error)]
pub enum ReaderCreateError {
    #[error("Failed to parse last key in postings list")]
    ParseLastKey,
    #[error("Speedb error: {0}")]
    SpeedbError(#[from] speedb::Error),
}

/// A generic index reader for postings lists stored in blocks.
/// This reader works with any block type that implements the `ArchivedBlock` trait.
pub struct GenericReader<'iterator, DBAccess, Block>
where
    DBAccess: speedb::DBAccess,
    Block: ArchivedBlock,
{
    /// The underlying Speedb iterator for reading the postings list blocks.
    iterator: DBIteratorWithThreadMode<'iterator, DBAccess>,

    /// The current block being read.
    current_block: Option<Block>,

    /// The key of the current block being read.
    current_block_key: Option<Box<[u8]>>,

    /// The index within the current block.
    block_index: <Block as ArchivedBlock>::Index,

    /// An estimate of the number of unique documents in the postings list.
    estimate: u64,

    /// The prefix we are iterating over.
    prefix: String,

    /// The key prefix for the term/tag in the inverted index.
    key_prefix: Vec<u8>,
}

impl<'iterator, DBAccess, Block> GenericReader<'iterator, DBAccess, Block>
where
    DBAccess: speedb::DBAccess,
    Block: ArchivedBlock,
{
    /// Creates a new `GenericReader` for the given term using the provided Speedb iterator.
    pub fn new(
        mut iterator: DBIteratorWithThreadMode<'iterator, DBAccess>,
        prefix: String,
    ) -> Result<Self, ReaderCreateError> {
        let key_prefix = InvertedIndexKey::new(&prefix, None).as_key();

        let estimate = Self::get_estimate(&mut iterator, &key_prefix, &prefix)?;

        // Reset the iterator to the first block for the term
        iterator.set_mode(speedb::IteratorMode::From(
            &key_prefix,
            speedb::Direction::Forward,
        ));

        Ok(Self {
            iterator,
            key_prefix,
            prefix,
            current_block: None,
            current_block_key: None,
            block_index: num_traits::Zero::zero(),
            estimate,
        })
    }

    /// Advances to the next block in the postings list. Returns `true` if a new valid block was
    /// found, or `false` if there are no more blocks for the current term.
    fn next_block(&mut self) -> bool {
        let (key, value) = match self.iterator.next() {
            Some(Ok((key, value))) if key_belongs_to_term(&key, self.key_prefix.as_slice()) => {
                (key, value)
            }
            Some(Err(error)) => {
                // Skip key_prefix + 1 byte delimiter to get the 8-byte doc_id
                let last_block_document_id = self
                    .current_block_key
                    .as_ref()
                    .map(|key| &key[self.key_prefix.len() + InvertedIndexKey::DELIMITER_SIZE..])
                    .and_then(|key| DocumentIdKey::from_key(key).ok());

                error!(
                    error = &error as &dyn std::error::Error,
                    ?last_block_document_id,
                    "Error reading postings list block",
                );
                panic!("Error reading postings list block: {}", error);
            }
            _ => {
                self.current_block = None;
                self.block_index = num_traits::Zero::zero();
                return false;
            }
        };

        let block = Block::from_bytes(value);

        self.current_block = Some(block);
        self.current_block_key = Some(key);
        self.block_index = num_traits::Zero::zero();

        true
    }

    /// Get the estimated number of documents for the iterator.
    fn get_estimate(
        iterator: &mut DBIteratorWithThreadMode<'iterator, DBAccess>,
        key_prefix: &[u8],
        prefix: &str,
    ) -> Result<u64, ReaderCreateError> {
        let Some(next) = iterator.next() else {
            return Ok(0);
        };

        let (key, value) = next?;

        if !key_belongs_to_term(&key, key_prefix) {
            return Ok(0);
        }

        // Get the first document ID in the first block
        let block = Block::from_bytes(value);
        let first_id = block.get_unchecked(num_traits::Zero::zero()).doc_id();

        // Now seek to the end to find the last document ID
        let end_key = InvertedIndexKey::new(prefix, Some(u64::MAX)).as_key();

        iterator.set_mode(speedb::IteratorMode::From(
            &end_key,
            speedb::Direction::Reverse,
        ));

        // We know at minimum that we'll match the same block
        let (key, _value) = iterator
            .next()
            .unwrap()
            .map_err(ReaderCreateError::SpeedbError)?;

        // Verify the key belongs to our term
        let expected_key_len = key_prefix.len() + InvertedIndexKey::DOC_ID_KEY_SIZE;
        if key.len() != expected_key_len || !key.starts_with(key_prefix) {
            return Err(ReaderCreateError::ParseLastKey);
        }

        // Extract doc_id: skip key_prefix and the delimiter byte
        let doc_id_start = key_prefix.len() + InvertedIndexKey::DELIMITER_SIZE;
        let doc_id_bytes = &key[doc_id_start..];
        let last_doc_id = t_docId::from_be_bytes(
            doc_id_bytes
                .try_into()
                .map_err(|_| ReaderCreateError::ParseLastKey)?,
        );

        Ok(last_doc_id - first_id + 1)
    }
}

impl<'index, 'iterator, DBAccess, Block> IndexReader<'index>
    for GenericReader<'iterator, DBAccess, Block>
where
    DBAccess: speedb::DBAccess,
    Block: ArchivedBlock,
{
    fn next_record(
        &mut self,
        result: &mut inverted_index::RSIndexResult<'index>,
    ) -> std::io::Result<bool> {
        if self.current_block.is_none() && !self.next_block() {
            return Ok(false);
        }

        // Get the document and populate result before any mutable borrows
        {
            let block = self.current_block.as_ref().unwrap();
            let doc = block.get_unchecked(self.block_index);
            doc.populate_result(result);
        }

        self.block_index = self.block_index + num_traits::One::one();

        let num_docs = self.current_block.as_ref().unwrap().num_docs();
        if self.block_index >= num_docs {
            self.next_block();
        }

        Ok(true)
    }

    fn seek_record(
        &mut self,
        doc_id: t_docId,
        result: &mut inverted_index::RSIndexResult<'index>,
    ) -> std::io::Result<bool> {
        if !self.skip_to(doc_id)
            || self.block_index >= self.current_block.as_ref().unwrap().num_docs()
        {
            return Ok(false);
        }

        // Perform binary search if needed and populate result in a scope to avoid borrow issues
        {
            let block = self.current_block.as_ref().unwrap();

            // Optimization to avoid binary searching if we are already at the correct index
            if block.get_unchecked(self.block_index).doc_id() != doc_id {
                self.block_index = block
                    .binary_search_by_key(self.block_index, &doc_id, |entry| entry.doc_id())
                    .unwrap_or_else(|insert_pos| insert_pos);
            };

            let Some(entry) = block.get(self.block_index) else {
                self.block_index = block.num_docs() - num_traits::One::one();
                return Ok(false);
            };

            entry.populate_result(result);
        }

        self.block_index = self.block_index + num_traits::One::one();

        let num_docs = self.current_block.as_ref().unwrap().num_docs();
        if self.block_index >= num_docs {
            self.next_block();
        }

        Ok(true)
    }

    fn skip_to(&mut self, doc_id: t_docId) -> bool {
        // Don't seek if we are already in the correct block
        if let Some(last_entry) = self.current_block.as_ref().and_then(|b| b.last())
            && last_entry.doc_id() >= doc_id
        {
            return true;
        }

        self.iterator.set_mode(speedb::IteratorMode::From(
            &InvertedIndexKey::new(&self.prefix, Some(doc_id)).as_key(),
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
        false
    }

    fn flags(&self) -> ffi::IndexFlags {
        IndexFlags_Index_DocIdsOnly
    }

    fn needs_revalidation(&self) -> bool {
        false
    }

    fn refresh_buffer_pointers(&mut self) {}
}
