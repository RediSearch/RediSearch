use ffi::t_docId;
use std::mem::size_of;

use super::{Document, Metadata};
use crate::index_spec::inverted_index::block_traits::{self, ArchivedBlock as _};

/// An archived representation of a Document in a postings list. This type holds direct references
/// to byte arrays representing the fields.
pub struct ArchivedDocument<'archive> {
    doc_id: &'archive [u8; size_of::<t_docId>()],
    field_mask: &'archive [u8; size_of::<u128>()],
    frequency: &'archive [u8; size_of::<u32>()],
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
        let frequency_start = metadata_offset + Metadata::FIELD_MASK_SIZE;
        let frequency_end = frequency_start + Metadata::FREQUENCY_SIZE;

        // Check that there are enough bytes to read the metadata
        // - 16 bytes for field_mask
        // - 4 bytes for frequency
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
    pub fn frequency(&self) -> u32 {
        u32::from_le_bytes(*self.frequency)
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

    /// Get version of the block
    #[inline(always)]
    #[allow(unused)]
    pub fn version(&self) -> u8 {
        self.version
    }

    /// Get an iterator over the documents in this block
    pub fn iter(&self) -> impl ExactSizeIterator<Item = ArchivedDocument<'_>> {
        (0..self.num_docs).map(|i| self.get_unchecked(i))
    }
}

// Implement the generic block traits for term blocks
impl<'archive> block_traits::ArchivedDocument for ArchivedDocument<'archive> {
    fn doc_id(&self) -> t_docId {
        self.doc_id()
    }

    fn populate_result<'index>(&self, result: &mut inverted_index::RSIndexResult<'index>) {
        result.doc_id = self.doc_id();
        result.field_mask = self.field_mask();
        result.freq = self.frequency();
    }
}

impl block_traits::ArchivedBlock for ArchivedBlock {
    type Document<'a>
        = ArchivedDocument<'a>
    where
        Self: 'a;
    type Index = u8;

    fn from_bytes(bytes: Box<[u8]>) -> Self {
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
                //   - 4 bytes for frequency
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

    fn num_docs(&self) -> Self::Index {
        self.num_docs
    }

    fn get(&self, index: Self::Index) -> Option<Self::Document<'_>> {
        if index >= self.num_docs {
            return None;
        }

        let doc =
            ArchivedDocument::from_bytes(&self.bytes[Self::BASE_OFFSET..], index, self.num_docs);
        Some(doc)
    }

    fn get_unchecked(&self, index: Self::Index) -> Self::Document<'_> {
        ArchivedDocument::from_bytes(&self.bytes[Self::BASE_OFFSET..], index, self.num_docs)
    }

    fn last(&self) -> Option<Self::Document<'_>> {
        if self.num_docs == 0 {
            return None;
        }

        self.get(self.num_docs - 1)
    }

    fn binary_search_by_key<B, F>(
        &self,
        start_index: Self::Index,
        b: &B,
        mut f: F,
    ) -> Result<Self::Index, Self::Index>
    where
        F: FnMut(&Self::Document<'_>) -> B,
        B: Ord,
    {
        let mut low = start_index;
        let mut high = self.num_docs;

        while low < high {
            let mid = low + (high - low) / 2;
            let doc =
                ArchivedDocument::from_bytes(&self.bytes[Self::BASE_OFFSET..], mid, self.num_docs);
            match f(&doc).cmp(b) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => return Ok(mid),
            }
        }

        Err(low)
    }
}
