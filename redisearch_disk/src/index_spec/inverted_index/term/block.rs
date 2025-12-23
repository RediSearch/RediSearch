use ffi::t_docId;
use std::mem::size_of;

use super::{Document, Metadata};

/// An archived representation of a Document in a postings list. This type holds direct references
/// to byte arrays representing the fields.
pub struct ArchivedDocument<'archive> {
    doc_id: &'archive [u8; size_of::<t_docId>()],
    field_mask: &'archive [u8; size_of::<u128>()],
    frequency: &'archive [u8; size_of::<u64>()],
}

impl<'archive> ArchivedDocument<'archive> {
    /// Map the byte slice to an ArchivedDocument at the given index, panicking if the slice is too short.
    ///
    /// # Panics
    /// Panics if the provided byte slice does not contain enough bytes to read:
    /// - An 8-byte little-endian unsigned integer representing the doc_id at the index.
    /// - A 16-byte little-endian unsigned integer representing the field_mask at the index.
    /// - An 8-byte little-endian unsigned integer representing the frequency at the index.
    #[inline(always)]
    fn from_bytes(bytes: &'archive [u8], index: u8, num_docs: u8) -> Self {
        let doc_id_offset = (index as usize) * size_of::<t_docId>();
        let doc_id_end = doc_id_offset + size_of::<t_docId>();

        // Check that there are enough bytes to read the doc_id
        // - 8 bytes for doc_id
        assert!(
            bytes.len() >= doc_id_end,
            "Insufficient bytes to read doc_id"
        );

        // SAFETY: We have already validated the slice lengths above. We are also extracting exactly the required number of bytes.
        let doc_id = unsafe {
            bytes[doc_id_offset..doc_id_end]
                .try_into()
                .unwrap_unchecked()
        };

        let metadata_start = num_docs as usize * size_of::<t_docId>();
        let metadata_offset = metadata_start + (index as usize) * Metadata::SIZE;
        let frequency_start = metadata_offset + size_of::<u128>();
        let frequency_end = frequency_start + size_of::<u64>();

        // Check that there are enough bytes to read the metadata
        // - 16 bytes for field_mask
        // - 8 bytes for frequency
        assert!(
            bytes.len() >= frequency_end,
            "Insufficient bytes to read Metadata"
        );

        // SAFETY: We have already validated the slice lengths above. We are also extracting exactly the required number of bytes.
        let field_mask = unsafe {
            bytes[metadata_offset..frequency_start]
                .try_into()
                .unwrap_unchecked()
        };

        // SAFETY: We have already validated the slice lengths above. We are also extracting exactly the required number of bytes.
        let frequency = unsafe {
            bytes[frequency_start..frequency_end]
                .try_into()
                .unwrap_unchecked()
        };

        ArchivedDocument {
            doc_id,
            field_mask,
            frequency,
        }
    }

    #[inline(always)]
    pub fn doc_id(&self) -> t_docId {
        u64::from_le_bytes(*self.doc_id)
    }

    #[inline(always)]
    pub fn field_mask(&self) -> u128 {
        u128::from_le_bytes(*self.field_mask)
    }

    #[inline(always)]
    pub fn frequency(&self) -> u64 {
        u64::from_le_bytes(*self.frequency)
    }
}

impl<'archive> From<ArchivedDocument<'archive>> for Document {
    fn from(archived: ArchivedDocument<'archive>) -> Self {
        Document {
            doc_id: archived.doc_id(),
            metadata: Metadata {
                field_mask: archived.field_mask(),
                frequency: archived.frequency(),
            },
        }
    }
}

/// An archived representation of a block of full documents in a term context in a postings list. This type holds a
/// reference to the underlying byte array and provides methods to access the documents.
pub struct ArchivedBlock {
    version: u8,
    /// Currently speedb has a default block size of 4Kb for the value We probably don't want blocks
    /// larger than that anyway. Going towards 64kb block size will probably mean we need to turn on
    /// blob storage on Speedb. That doesn't sound like the most performant component from talking
    /// to the Speedb team.
    ///
    /// Every document entry is 32 bytes (8 bytes for doc_id, 16 bytes for field_mask, 8 bytes for
    /// frequency), so we can fit at most 128 docs in a block. Thus, we can store the number of
    /// docs as a u8 (max 255 entries).
    num_docs: u8,
    bytes: Box<[u8]>,
}

impl ArchivedBlock {
    /// The offset at the front of the data used to store metadata like version and number of docs.
    /// - 1 byte for version
    /// - 1 byte for number of docs
    const BASE_OFFSET: usize = 2;

    /// Create a Block from a byte slice
    ///
    /// # Panics
    /// Panics if the byte slice is less than 2 bytes long, or if it does not contain enough bytes for all docs.
    pub fn from_bytes(bytes: Box<[u8]>) -> Self {
        assert!(
            bytes.len() >= Self::BASE_OFFSET,
            "Byte slice must be at least 2 bytes long to read the version and number of docs"
        );

        let version = bytes[0];

        match version {
            0 => {
                let num_docs = u8::from_le_bytes([bytes[1]]);

                // Calculate expected size
                // - 1 byte for version and 1 byte for count
                // - num_docs * 32 bytes for docs
                //   - 8 bytes for doc_id
                //   - 16 bytes for field_mask
                //   - 8 bytes for frequency
                let expected_size = (num_docs as usize)
                    .saturating_mul(size_of::<t_docId>() + Metadata::SIZE)
                    .saturating_add(Self::BASE_OFFSET);

                assert!(
                    bytes.len() >= expected_size,
                    "Byte slice does not contain enough bytes for all docs"
                );

                ArchivedBlock {
                    bytes,
                    num_docs,
                    version,
                }
            }
            _ => panic!("Unsupported Block version: {}", version),
        }
    }

    /// Get number of docs in the block
    #[inline(always)]
    pub fn num_docs(&self) -> u8 {
        self.num_docs
    }

    /// Get version of the block
    #[inline(always)]
    #[allow(unused)]
    pub fn version(&self) -> u8 {
        self.version
    }

    /// Perform a binary search over the docs in the block using a key extraction function
    pub fn binary_search_by_key<B, F>(&self, start_index: u8, b: &B, mut f: F) -> Result<u8, u8>
    where
        F: FnMut(&ArchivedDocument<'_>) -> B,
        B: Ord,
    {
        (start_index..self.num_docs)
            .collect::<Vec<_>>()
            .binary_search_by_key(b, |index| {
                let doc = ArchivedDocument::from_bytes(
                    &self.bytes[Self::BASE_OFFSET..],
                    *index,
                    self.num_docs,
                );
                f(&doc)
            })
            .map(|pos| pos as u8 + start_index)
            .map_err(|insert_pos| {
                u8::try_from(insert_pos).expect("to not overflow the block entries") + start_index
            })
    }

    /// Get doc at index if it exists
    pub fn get(&self, index: u8) -> Option<ArchivedDocument<'_>> {
        if index >= self.num_docs {
            return None;
        }

        let doc =
            ArchivedDocument::from_bytes(&self.bytes[Self::BASE_OFFSET..], index, self.num_docs);
        Some(doc)
    }

    /// Get doc at index without bounds checking
    ///
    /// # Panic
    /// Panics if index >= num_docs
    pub fn get_unchecked(&self, index: u8) -> ArchivedDocument<'_> {
        ArchivedDocument::from_bytes(&self.bytes[Self::BASE_OFFSET..], index, self.num_docs)
    }

    /// Get the last doc in the block if it exists
    pub fn last(&self) -> Option<ArchivedDocument<'_>> {
        if self.num_docs == 0 {
            return None;
        }

        self.get(self.num_docs - 1)
    }

    /// Get an iterator over the documents in this block
    pub fn iter(&self) -> impl ExactSizeIterator<Item = ArchivedDocument<'_>> {
        (0..self.num_docs).map(|i| self.get_unchecked(i))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_version_0() {
        // Create a byte array representing a version 0 Block with 2 docs
        let mut bytes: Vec<u8> = Vec::with_capacity(2 + 2 * 32);
        bytes.extend_from_slice(&[0u8]); // version 0
        bytes.extend_from_slice(&2u8.to_le_bytes()); // number of docs = 2

        // Doc 1 ID
        bytes.extend_from_slice(&1u64.to_le_bytes()); // doc_id = 1

        // Doc 2 Id
        bytes.extend_from_slice(&2u64.to_le_bytes()); // doc_id = 2

        // Doc 1 Metadata
        bytes.extend_from_slice(&0x10000000000000000000000000000001u128.to_le_bytes()); // field_mask
        bytes.extend_from_slice(&5u64.to_le_bytes()); // frequency = 5

        // Doc 2 Metadata
        bytes.extend_from_slice(&0xFFFFu128.to_le_bytes()); // field_mask
        bytes.extend_from_slice(&10u64.to_le_bytes()); // frequency = 10

        let archived_block = ArchivedBlock::from_bytes(bytes.into());
        assert_eq!(archived_block.version(), 0);
        assert_eq!(archived_block.num_docs(), 2);

        let doc1 = archived_block.get(0).unwrap();
        assert_eq!(doc1.doc_id(), 1);
        assert_eq!(doc1.field_mask(), 0x10000000000000000000000000000001);
        assert_eq!(doc1.frequency(), 5);

        let doc2 = archived_block.get(1).unwrap();
        assert_eq!(doc2.doc_id(), 2);
        assert_eq!(doc2.field_mask(), 0xFFFF);
        assert_eq!(doc2.frequency(), 10);

        assert!(archived_block.get(2).is_none());
    }
}
