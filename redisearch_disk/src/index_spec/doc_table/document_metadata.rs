use std::mem::size_of;

use super::DocumentFlags;
use crate::index_spec::Key;

/// The DocumentMetadata struct represents the metadata associated with a document in the index,
/// including its Redis key.
#[derive(Debug, PartialEq)]
pub struct DocumentMetadata {
    /// The Redis key of the document
    pub key: Key,
    /// The a priori document score as given by the user on insertion
    pub score: f32,
    /// The flags of the document
    pub flags: DocumentFlags,
    /// The maximum frequency of any term in the document, used to normalize frequencies
    pub max_term_freq: u32,
    /// The sum of the frequencies of all terms in the document
    pub doc_len: u32,
}

impl DocumentMetadata {
    /// Serializes the document metadata into a byte vector.
    ///
    /// The serialization format is:
    /// - 4 bytes: key length (little-endian u32)
    /// - N bytes: key data
    /// - 4 bytes: score (little-endian f32)
    /// - 4 bytes: flags (little-endian u32)
    /// - 4 bytes: max_term_freq (little-endian u32)
    /// - 4 bytes: doc_len (little-endian u32)
    pub fn serialize(&self) -> Vec<u8> {
        let key_bytes = &self.key;
        let key_len = key_bytes.len() as u32;

        // Calculate the total size needed
        let mut bytes = Vec::with_capacity(
            size_of::<u32>()
                + key_bytes.len()
                + size_of::<f32>()
                + size_of::<u32>()
                + size_of::<u32>()
                + size_of::<u32>(),
        );
        bytes.extend_from_slice(&key_len.to_le_bytes());
        bytes.extend_from_slice(key_bytes);
        bytes.extend_from_slice(&self.score.to_le_bytes());
        bytes.extend_from_slice(&self.flags.bits().to_le_bytes());
        bytes.extend_from_slice(&self.max_term_freq.to_le_bytes());
        bytes.extend_from_slice(&self.doc_len.to_le_bytes());

        bytes
    }

    /// Deserializes document metadata from a byte slice.
    ///
    /// # Panics
    /// Panics if the byte slice is malformed or too short.
    pub fn deserialize(bytes: &[u8]) -> Self {
        let archive = ArchivedDocumentMetadata::from_bytes(bytes);

        archive.into()
    }
}

/// An archived representation of `DocumentMetadata` for zero-copy deserialization.
pub struct ArchivedDocumentMetadata<'archive> {
    key: &'archive [u8],
    score: &'archive [u8; size_of::<f32>()],
    flags: &'archive [u8; size_of::<u32>()],
    max_term_freq: &'archive [u8; size_of::<u32>()],
    doc_len: &'archive [u8; size_of::<u32>()],
}

impl<'archive> ArchivedDocumentMetadata<'archive> {
    /// Creates an `ArchivedDocumentMetadata` from a byte slice.
    ///
    /// # Panics
    /// Panics if the provided byte slice does not contain enough bytes to read:
    /// - A 4-byte little-endian unsigned integer representing the length of the key.
    /// - The key bytes of the specified length.
    /// - A 4-byte little-endian float representing the score.
    /// - A 4-byte little-endian unsigned integer representing the flags.
    /// - A 4-byte little-endian unsigned integer representing the max_term_freq.
    /// - A 4-byte little-endian unsigned integer representing the doc_len.
    pub fn from_bytes(bytes: &'archive [u8]) -> Self {
        assert!(
            bytes.len() >= size_of::<u32>(),
            "Insufficient bytes to read key length"
        );

        // SAFETY: We just checked that bytes has at least 4 bytes and are extracting exactly 4 bytes.
        let key_len =
            unsafe { u32::from_le_bytes(bytes[0..4].try_into().unwrap_unchecked()) as usize };

        // Calculate expected size
        let expected_size = (key_len).saturating_add(
            size_of::<u32>() // key_len
                + size_of::<f32>() // score
                + size_of::<u32>() // flags
                + size_of::<u32>() // max_term_freq
                + size_of::<u32>(), // doc_len
        );

        assert!(
            bytes.len() >= expected_size,
            "Insufficient bytes to read complete DocumentMetadata"
        );

        let key_start = size_of::<u32>();
        let key_end = key_start + key_len;
        let score_start = key_end;
        let flags_start = score_start + size_of::<f32>();
        let max_term_freq_start = flags_start + size_of::<u32>();
        let doc_len_start = max_term_freq_start + size_of::<u32>();
        let doc_len_end = doc_len_start + size_of::<u32>();

        let key = &bytes[key_start..key_end];

        // SAFETY: We have already validated the slice lengths above. We are also extracting exactly 4 bytes.
        let score = unsafe {
            bytes[score_start..flags_start]
                .try_into()
                .unwrap_unchecked()
        };

        // SAFETY: We have already validated the slice lengths above.
        let flags = unsafe {
            bytes[flags_start..max_term_freq_start]
                .try_into()
                .unwrap_unchecked()
        };

        // SAFETY: We have already validated the slice lengths above. We are also extracting exactly 4 bytes.
        let max_term_freq = unsafe {
            bytes[max_term_freq_start..doc_len_start]
                .try_into()
                .unwrap_unchecked()
        };

        // SAFETY: We have already validated the slice lengths above. We are also extracting exactly 4 bytes.
        let doc_len = unsafe {
            bytes[doc_len_start..doc_len_end]
                .try_into()
                .unwrap_unchecked()
        };

        ArchivedDocumentMetadata {
            key,
            score,
            flags,
            max_term_freq,
            doc_len,
        }
    }

    #[inline(always)]
    pub fn key(&self) -> &'archive [u8] {
        self.key
    }

    #[inline(always)]
    pub fn score(&self) -> f32 {
        f32::from_le_bytes(*self.score)
    }

    /// Returns the combined flags as DocumentFlags.
    #[inline(always)]
    pub fn flags(&self) -> DocumentFlags {
        DocumentFlags::from_bits_truncate(u32::from_le_bytes(*self.flags))
    }

    #[inline(always)]
    pub fn max_term_freq(&self) -> u32 {
        u32::from_le_bytes(*self.max_term_freq)
    }

    #[inline(always)]
    pub fn doc_len(&self) -> u32 {
        u32::from_le_bytes(*self.doc_len)
    }
}

impl<'archive> From<ArchivedDocumentMetadata<'archive>> for DocumentMetadata {
    fn from(archived: ArchivedDocumentMetadata<'archive>) -> Self {
        DocumentMetadata {
            key: Key::from(archived.key.to_vec()),
            score: archived.score(),
            flags: archived.flags(),
            max_term_freq: archived.max_term_freq(),
            doc_len: archived.doc_len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::document_flags::DocumentFlag;
    use super::*;
    use crate::index_spec::Key;

    fn build_bytes(
        key: &[u8],
        score: f32,
        flags: DocumentFlags,
        max_term_freq: u32,
        doc_len: u32,
    ) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(key.len() as u32).to_le_bytes());
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(&score.to_le_bytes());
        bytes.extend_from_slice(&flags.bits().to_le_bytes());
        bytes.extend_from_slice(&max_term_freq.to_le_bytes());
        bytes.extend_from_slice(&doc_len.to_le_bytes());
        bytes
    }

    #[test]
    fn document_metadata_roundtrip() {
        let original = DocumentMetadata {
            key: Key::from("document_1"),
            score: 3.5,
            flags: DocumentFlag::Deleted | DocumentFlag::HasExpiration,
            max_term_freq: 10,
            doc_len: 100,
        };

        let serialized = original.serialize();
        let deserialized = DocumentMetadata::deserialize(&serialized);

        assert_eq!(original, deserialized);
    }

    #[test]
    fn access_each_field() {
        let key = b"hello";
        let score: f32 = 1.5;
        let flags = DocumentFlag::Deleted | DocumentFlag::HasExpiration;
        let max_term_freq: u32 = 10;
        let doc_len: u32 = 100;

        let bytes = build_bytes(key, score, flags, max_term_freq, doc_len);
        let archived = ArchivedDocumentMetadata::from_bytes(&bytes);

        assert_eq!(archived.key(), key);
        assert_eq!(archived.score(), score);
        assert_eq!(archived.max_term_freq(), max_term_freq);
        assert_eq!(archived.doc_len(), doc_len);

        let flags = archived.flags();
        assert!(flags.contains(DocumentFlag::Deleted));
        assert!(flags.contains(DocumentFlag::HasExpiration));
    }

    #[test]
    fn zero_values() {
        let bytes = build_bytes(b"key", 0.0, DocumentFlags::empty(), 0, 0);
        let archived = ArchivedDocumentMetadata::from_bytes(&bytes);

        assert_eq!(archived.score(), 0.0);
        assert_eq!(archived.flags(), DocumentFlags::empty());
        assert_eq!(archived.max_term_freq(), 0);
        assert_eq!(archived.doc_len(), 0);
    }

    #[test]
    fn max_u32_values() {
        let flags = DocumentFlags::all();
        let bytes = build_bytes(b"key", f32::MAX, flags, u32::MAX, u32::MAX);
        let archived = ArchivedDocumentMetadata::from_bytes(&bytes);

        assert_eq!(archived.score(), f32::MAX);
        assert_eq!(archived.max_term_freq(), u32::MAX);
        assert_eq!(archived.doc_len(), u32::MAX);
        assert_eq!(archived.flags(), flags);
    }

    #[test]
    fn negative_score() {
        let bytes = build_bytes(b"key", -42.5, DocumentFlag::Deleted.into(), 5, 100);
        let archived = ArchivedDocumentMetadata::from_bytes(&bytes);

        assert_eq!(archived.score(), -42.5);
    }

    #[test]
    #[should_panic(expected = "Insufficient bytes to read key length")]
    fn insufficient_bytes_for_key_length() {
        let bytes = vec![0u8, 1, 2]; // Only 3 bytes, need at least 4
        ArchivedDocumentMetadata::from_bytes(&bytes);
    }

    #[test]
    #[should_panic(expected = "Insufficient bytes to read complete DocumentMetadata")]
    fn insufficient_bytes_for_key_data() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(10u32).to_le_bytes()); // Key length is 10
        bytes.extend_from_slice(b"short"); // But only 5 bytes of key data
        ArchivedDocumentMetadata::from_bytes(&bytes);
    }

    #[test]
    #[should_panic(expected = "Insufficient bytes to read complete DocumentMetadata")]
    fn insufficient_bytes_for_score() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(3u32).to_le_bytes()); // Key length is 3
        bytes.extend_from_slice(b"key");
        bytes.extend_from_slice(&[0u8, 1]); // Only 2 bytes for score, need 4
        ArchivedDocumentMetadata::from_bytes(&bytes);
    }

    #[test]
    #[should_panic(expected = "Insufficient bytes to read complete DocumentMetadata")]
    fn insufficient_bytes_for_flags() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(3u32).to_le_bytes()); // Key length is 3
        bytes.extend_from_slice(b"key");
        bytes.extend_from_slice(&(1.5f32).to_le_bytes()); // Score
        bytes.extend_from_slice(&[0u8, 1]); // Only 2 bytes for flags, need 4
        ArchivedDocumentMetadata::from_bytes(&bytes);
    }

    #[test]
    #[should_panic(expected = "Insufficient bytes to read complete DocumentMetadata")]
    fn insufficient_bytes_for_max_term_freq() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(3u32).to_le_bytes()); // Key length is 3
        bytes.extend_from_slice(b"key");
        bytes.extend_from_slice(&(1.5f32).to_le_bytes()); // Score
        bytes.extend_from_slice(&DocumentFlags::empty().bits().to_le_bytes()); // Flags
        bytes.extend_from_slice(&[0u8, 1]); // Only 2 bytes for max_term_freq, need 4
        ArchivedDocumentMetadata::from_bytes(&bytes);
    }

    #[test]
    #[should_panic(expected = "Insufficient bytes to read complete DocumentMetadata")]
    fn insufficient_bytes_for_doc_len() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(3u32).to_le_bytes()); // Key length is 3
        bytes.extend_from_slice(b"key");
        bytes.extend_from_slice(&(1.5f32).to_le_bytes()); // Score
        bytes.extend_from_slice(&DocumentFlags::empty().bits().to_le_bytes()); // Flags
        bytes.extend_from_slice(&(10u32).to_le_bytes()); // max_term_freq
        bytes.extend_from_slice(&[0u8, 1]); // Only 2 bytes for doc_len, need 4
        ArchivedDocumentMetadata::from_bytes(&bytes);
    }

    #[test]
    fn extra_bytes_ignored() {
        let flags = DocumentFlag::Deleted | DocumentFlag::HasExpiration;
        let mut bytes = build_bytes(b"key", 1.0, flags, 3, 4);
        bytes.extend_from_slice(b"extra_data_that_should_be_ignored");

        let archived = ArchivedDocumentMetadata::from_bytes(&bytes);

        assert_eq!(archived.key(), b"key");
        assert_eq!(archived.score(), 1.0);
        assert_eq!(archived.flags(), flags);
        assert_eq!(archived.max_term_freq(), 3);
        assert_eq!(archived.doc_len(), 4);
    }

    #[test]
    fn from_archived_to_document_metadata() {
        let key = b"document_key";
        let score: f32 = 5.5;
        let flags: DocumentFlags = DocumentFlag::Deleted | DocumentFlag::HasExpiration;
        let max_term_freq: u32 = 200;
        let doc_len: u32 = 300;

        let flags_bytes = flags.bits().to_le_bytes();

        let archived = ArchivedDocumentMetadata {
            key,
            score: &score.to_le_bytes(),
            flags: &flags_bytes,
            max_term_freq: &max_term_freq.to_le_bytes(),
            doc_len: &doc_len.to_le_bytes(),
        };

        let metadata: DocumentMetadata = archived.into();

        assert_eq!(&metadata.key[..], key);
        assert_eq!(metadata.score, score);
        assert_eq!(metadata.flags, flags);
        assert_eq!(metadata.max_term_freq, max_term_freq);
        assert_eq!(metadata.doc_len, doc_len);
    }

    #[test]
    fn special_characters_in_key() {
        let key = b"key\x00with\x01special\xffchars";
        let flags = DocumentFlag::Deleted | DocumentFlag::HasExpiration;
        let bytes = build_bytes(key, 7.2, flags, 500, 1000);
        let archived = ArchivedDocumentMetadata::from_bytes(&bytes);

        assert_eq!(archived.key(), key);
        assert_eq!(archived.score(), 7.2);
        assert_eq!(archived.flags(), flags);
        assert_eq!(archived.max_term_freq(), 500);
        assert_eq!(archived.doc_len(), 1000);
    }
}
