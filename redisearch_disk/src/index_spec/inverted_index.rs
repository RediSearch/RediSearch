pub mod block_traits;
pub mod generic_index;
pub mod generic_reader;
pub mod tag;
pub mod term;

// Re-export the main types for convenience
pub use tag::{TagIndexConfig, TagInvertedIndex};
pub use term::{InvertedIndex, TermIndexConfig};

// Type aliases for the specific reader types
pub type PostingsListReader<'iterator, DBAccess> =
    generic_reader::GenericReader<'iterator, DBAccess, term::archive::ArchivedBlock>;
pub type TagPostingsListReader<'iterator, DBAccess> =
    generic_reader::GenericReader<'iterator, DBAccess, tag::archive::ArchivedTagBlock>;

pub use generic_reader::ReaderCreateError as PostingReaderCreateError;
pub use generic_reader::ReaderCreateError as TagPostingReaderCreateError;

use std::mem::size_of;

use ffi::t_docId;

use crate::key_traits::AsKeyExt;

// Re-export for internal use by term and tag modules
pub(crate) use super::deleted_ids::DeletedIdsStore;
use crate::database::{Speedb, SpeedbMultithreadedDatabase};

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
    pub(crate) const DELIMITER_SIZE: usize = size_of_val(&Self::TERM_DELIMITER);

    /// Size of the binary-encoded document ID suffix in keys (delimiter + 8 bytes for u64)
    pub(crate) const DOC_ID_KEY_SIZE: usize = Self::DELIMITER_SIZE + size_of::<t_docId>();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value_traits::ValueExt;
    use block_traits::{ArchivedBlock, SerializableBlock};
    use term::PostingsListBlock;

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
        let mut block = term::PostingsListBlock::default();

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

    #[test]
    fn tag_postings_list_block_roundtrip() {
        let mut block = tag::TagPostingsListBlock::default();

        let doc1 = tag::TagDocument::new(1);
        let doc2 = tag::TagDocument::new(2);
        let doc3 = tag::TagDocument::new(100);

        block.push(doc1.clone());
        block.push(doc2.clone());
        block.push(doc3.clone());

        let block =
            tag::archive::ArchivedTagBlock::from_bytes(block.serialize().into_boxed_slice());

        assert_eq!(block.num_docs(), 3);
        assert_eq!(block.get(0).unwrap().doc_id(), doc1.doc_id);
        assert_eq!(block.get(1).unwrap().doc_id(), doc2.doc_id);
        assert_eq!(block.get(2).unwrap().doc_id(), doc3.doc_id);
        assert!(block.get(3).is_none());
    }
}
