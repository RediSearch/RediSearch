use ffi::{IndexFlags_Index_DocIdsOnly, t_docId};
use inverted_index::IndexReader;
use speedb::DBIteratorWithThreadMode;
use thiserror::Error;
use tracing::error;

use crate::{
    document_id_key::DocumentIdKey,
    index_spec::inverted_index::PostingsListBlock,
    key_traits::{AsKeyExt, FromKeyExt},
    value_traits::ValueExt,
};

use super::super::InvertedIndexKey;
use super::block;

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

/// An index reader for a postings list stored in blocks. This reader allows for efficient
/// seeking and iteration over term documents in an index.
pub struct Reader<'iterator, DBAccess: speedb::DBAccess> {
    /// The underlying Speedb iterator for reading the postings list blocks.
    iterator: DBIteratorWithThreadMode<'iterator, DBAccess>,

    /// The current block being read.
    current_block: Option<block::ArchivedBlock>,

    /// The key of the current block being read.
    current_block_key: Option<Box<[u8]>>,

    /// The index within the current block.
    block_index: u8,

    /// An estimate of the number of unique documents in the postings list.
    estimate: u64,

    /// The prefix we are iterating over.
    prefix: String,

    /// The key prefix for the term in the inverted index.
    key_prefix: Vec<u8>,
}

impl<'iterator, DBAccess: speedb::DBAccess> Reader<'iterator, DBAccess> {
    /// Creates a new `Reader` for the given term using the provided Speedb iterator.
    pub fn new(
        mut iterator: DBIteratorWithThreadMode<'iterator, DBAccess>,
        prefix: String,
    ) -> Result<Self, ReaderCreateError> {
        let key_prefix = InvertedIndexKey {
            prefix: &prefix,
            last_doc_id: None,
        }
        .as_key();

        let estimate = Self::get_estimate(&mut iterator, &key_prefix, &prefix)?;

        // Reset the iterator to the first block for the term
        iterator.set_mode(speedb::IteratorMode::From(
            &key_prefix,
            speedb::Direction::Forward,
        ));

        Ok(Self {
            iterator,
            prefix,
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
                self.block_index = 0;
                return false;
            }
        };

        let block = PostingsListBlock::archive_from_speedb_value(&value);

        self.current_block = Some(block);
        self.current_block_key = Some(key);
        self.block_index = 0;

        true
    }

    /// Get the estimated number of documents for the iterator over the current term.
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
        let block = PostingsListBlock::archive_from_speedb_value(&value);
        let first_id = block.get_unchecked(0).doc_id();

        // Now seek to the end to find the last document ID. This ID is stored as a suffix on the
        // entry's key.
        let end_key = InvertedIndexKey {
            prefix,
            last_doc_id: Some(u64::MAX),
        }
        .as_key();

        iterator.set_mode(speedb::IteratorMode::From(
            &end_key,
            speedb::Direction::Reverse,
        ));

        // We know at minimum that we'll match the same block. So it is safe to unwrap here.
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

impl<'index, 'iterator, DBAccess: speedb::DBAccess> IndexReader<'index>
    for Reader<'iterator, DBAccess>
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

        if self.block_index >= block.num_docs() {
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
            self.block_index = block.num_docs() - 1;
            return Ok(false);
        };

        result.doc_id = entry.doc_id();
        result.field_mask = entry.field_mask();

        self.block_index += 1;

        if self.block_index >= block.num_docs() {
            self.next_block();
        }

        Ok(true)
    }

    fn skip_to(&mut self, doc_id: t_docId) -> bool {
        // Don't seek if we are already in the correct block
        if let Some(last_entry) = self
            .current_block
            .as_ref()
            .and_then(block::ArchivedBlock::last)
            && last_entry.doc_id() >= doc_id
        {
            return true;
        }

        self.iterator.set_mode(speedb::IteratorMode::From(
            &InvertedIndexKey {
                prefix: &self.prefix,
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

    fn refresh_buffer_pointers(&mut self) {
        // We don't need to refresh buffer pointers because we don't own the buffer.
    }
}
