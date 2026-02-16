use ffi::t_docId;
use std::mem::size_of;

use super::TagDocument;
use crate::index_spec::inverted_index::block_traits::{self, ArchivedBlock};

/// An archived representation of a TagDocument in a postings list. This type holds direct references
/// to byte arrays representing just the document ID.
pub struct ArchivedTagDocument<'archive> {
    doc_id: &'archive [u8; size_of::<t_docId>()],
}

impl<'archive> ArchivedTagDocument<'archive> {
    /// Create a new archived tag document from a byte slice
    ///
    /// # Arguments
    /// * `bytes` - The byte slice containing all document IDs in the block
    /// * `index` - The index of this document in the block
    #[inline(always)]
    pub fn from_bytes(bytes: &'archive [u8], index: u16) -> Self {
        let doc_id_offset = index as usize * TagDocument::SIZE;
        let doc_id = bytes[doc_id_offset..doc_id_offset + TagDocument::SIZE]
            .try_into()
            .expect("doc_id slice to be exactly 8 bytes");

        Self { doc_id }
    }

    /// Get the document ID
    #[inline(always)]
    pub fn doc_id(&self) -> t_docId {
        t_docId::from_le_bytes(*self.doc_id)
    }
}

impl<'archive> block_traits::ArchivedDocument for ArchivedTagDocument<'archive> {
    fn doc_id(&self) -> t_docId {
        self.doc_id()
    }

    fn populate_result<'index>(&self, result: &mut inverted_index::RSIndexResult<'index>) {
        result.doc_id = self.doc_id();
        // Tags don't have field masks - the field is implicit from the tag index itself
    }
}

impl<'archive> From<ArchivedTagDocument<'archive>> for TagDocument {
    fn from(archived: ArchivedTagDocument<'archive>) -> Self {
        TagDocument {
            doc_id: archived.doc_id(),
        }
    }
}

/// An archived representation of a block of tag documents in a postings list. This type holds
/// the underlying byte array and provides methods to access the documents.
pub struct ArchivedTagBlock {
    version: u8,
    /// Currently speedb has a default block size of 4Kb for the value. We probably don't want blocks
    /// larger than that anyway. Going towards 64kb block size will probably mean we need to turn on
    /// blob storage on Speedb. That doesn't sound like the most performant component from talking
    /// to the Speedb team.
    ///
    /// Every tag document entry is 8 bytes (just the doc_id), so we can fit at most 512 docs in a
    /// 4KB block. Thus, we can store the number of docs in a u16 (max 65535).
    num_docs: u16,
    bytes: Box<[u8]>,
}

impl ArchivedTagBlock {
    /// Offset where the actual document data starts (after version and num_docs)
    const BASE_OFFSET: usize = size_of::<u8>() + size_of::<u16>();

    /// Get version of the block
    #[inline(always)]
    #[allow(unused)]
    pub fn version(&self) -> u8 {
        self.version
    }
}

impl block_traits::ArchivedBlock for ArchivedTagBlock {
    type Document<'a>
        = ArchivedTagDocument<'a>
    where
        Self: 'a;
    type Index = u16;

    fn from_bytes(bytes: Box<[u8]>) -> Self {
        assert!(
            bytes.len() >= Self::BASE_OFFSET,
            "Byte slice must be at least {} bytes long to read the version and number of docs",
            Self::BASE_OFFSET
        );

        let version = bytes[0];

        match version {
            0 => {
                let num_docs = u16::from_le_bytes([bytes[1], bytes[2]]);

                // Calculate expected size
                // - 1 byte for version
                // - 2 bytes for num_docs (u16)
                // - num_docs * 8 bytes for doc_ids
                let expected_size = (num_docs as usize)
                    .saturating_mul(TagDocument::SIZE)
                    .saturating_add(Self::BASE_OFFSET);

                assert!(
                    bytes.len() >= expected_size,
                    "Byte slice does not contain enough bytes for all docs"
                );

                Self {
                    version,
                    num_docs,
                    bytes,
                }
            }
            _ => panic!("Unsupported ArchivedTagBlock version: {}", version),
        }
    }

    fn num_docs(&self) -> Self::Index {
        self.num_docs
    }

    fn get(&self, index: Self::Index) -> Option<Self::Document<'_>> {
        if index >= self.num_docs {
            return None;
        }

        let doc = ArchivedTagDocument::from_bytes(&self.bytes[Self::BASE_OFFSET..], index);
        Some(doc)
    }

    fn get_unchecked(&self, index: Self::Index) -> Self::Document<'_> {
        ArchivedTagDocument::from_bytes(&self.bytes[Self::BASE_OFFSET..], index)
    }

    fn last(&self) -> Option<Self::Document<'_>> {
        if self.num_docs == 0 {
            return None;
        }

        self.get(self.num_docs - 1)
    }

    fn iter(&self) -> impl Iterator<Item = Self::Document<'_>> {
        ArchivedTagBlockIterator {
            block: self,
            index: 0,
        }
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
            let doc = ArchivedTagDocument::from_bytes(&self.bytes[Self::BASE_OFFSET..], mid);
            match f(&doc).cmp(b) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => return Ok(mid),
            }
        }

        Err(low)
    }
}

/// Iterator over documents in an archived tag block
pub struct ArchivedTagBlockIterator<'archive> {
    block: &'archive ArchivedTagBlock,
    index: u16,
}

impl<'archive> Iterator for ArchivedTagBlockIterator<'archive> {
    type Item = ArchivedTagDocument<'archive>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.block.num_docs() {
            return None;
        }

        let doc = self.block.get_unchecked(self.index);
        self.index += 1;
        Some(doc)
    }
}
