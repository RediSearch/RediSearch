use std::mem::size_of;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::{DocumentFlag, DocumentFlags};
use crate::{index_spec::Key, value_traits::ValueExt};

/// Size in bytes of the expiration field (u64 seconds + u32 nanoseconds)
const EXPIRATION_SIZE: usize = size_of::<u64>() + size_of::<u32>();

/// The DocumentMetadata struct represents the metadata associated with a document in the index,
/// including its Redis key.
#[derive(Debug, Default, PartialEq)]
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
    /// Optional document expiration time. Present when `HasExpiration` flag is set.
    pub expiration: Option<SystemTime>,
}

impl ValueExt for DocumentMetadata {
    type ArchivedType<'a> = ArchivedDocumentMetadata<'a>;

    /// Serializes the document metadata into a byte vector.
    ///
    /// The serialization format is:
    /// - 4 bytes: key length (little-endian u32)
    /// - N bytes: key data
    /// - 4 bytes: score (little-endian f32)
    /// - 4 bytes: flags (little-endian u32)
    /// - 4 bytes: max_term_freq (little-endian u32)
    /// - 4 bytes: doc_len (little-endian u32)
    ///
    /// Optional Section:
    /// The following fields are conditionally present based on the `flags` field above:
    ///
    /// - When `HasExpiration` flag is set:
    ///   - 8 bytes: expiration seconds since UNIX_EPOCH (little-endian u64)
    ///   - 4 bytes: expiration subsecond nanoseconds (little-endian u32)
    fn as_speedb_value(&self) -> Vec<u8> {
        let Self {
            key: key_bytes,
            score,
            flags,
            max_term_freq,
            doc_len,
            expiration,
        } = self;
        let key_len = key_bytes.len() as u32;

        // Calculate the total size needed (base size + optional expiration)
        let base_size = size_of::<u32>()
            + key_bytes.len()
            + size_of::<f32>()
            + size_of::<u32>()
            + size_of::<u32>()
            + size_of::<u32>();
        let optional_section_size = expiration.map_or(0, |_| EXPIRATION_SIZE);
        let mut bytes = Vec::with_capacity(base_size + optional_section_size);

        bytes.extend_from_slice(&key_len.to_le_bytes());
        bytes.extend(key_bytes);
        bytes.extend_from_slice(&score.to_le_bytes());
        bytes.extend_from_slice(&flags.bits().to_le_bytes());
        bytes.extend_from_slice(&max_term_freq.to_le_bytes());
        bytes.extend_from_slice(&doc_len.to_le_bytes());

        // Optional section: expiration (when HasExpiration flag is set)
        if let Some(exp) = expiration {
            debug_assert!(
                flags.contains(DocumentFlag::HasExpiration),
                "HasExpiration flag must be set when accessing expiration time"
            );
            let duration = exp.duration_since(UNIX_EPOCH).unwrap();
            bytes.extend_from_slice(&duration.as_secs().to_le_bytes());
            bytes.extend_from_slice(&duration.subsec_nanos().to_le_bytes());
        }

        bytes
    }

    /// Deserializes document metadata from a byte slice.
    ///
    /// # Panics
    /// Panics if the byte slice is malformed or too short.
    fn archive_from_speedb_value(value: &[u8]) -> Self::ArchivedType<'_> {
        ArchivedDocumentMetadata::from_bytes(value)
    }
}

/// An archived representation of `DocumentMetadata` for zero-copy deserialization.
pub struct ArchivedDocumentMetadata<'archive> {
    key: &'archive [u8],
    score: &'archive [u8; size_of::<f32>()],
    flags: DocumentFlags,
    max_term_freq: &'archive [u8; size_of::<u32>()],
    doc_len: &'archive [u8; size_of::<u32>()],
    expiration_secs: Option<&'archive [u8; size_of::<u64>()]>,
    expiration_nanos: Option<&'archive [u8; size_of::<u32>()]>,
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
    /// - When `HasExpiration` flag is set:
    ///   - An 8-byte little-endian unsigned integer representing expiration seconds since UNIX_EPOCH.
    ///   - A 4-byte little-endian unsigned integer representing expiration subsecond nanoseconds.
    fn from_bytes(bytes: &'archive [u8]) -> Self {
        assert!(
            bytes.len() >= size_of::<u32>(),
            "Insufficient bytes to read key length"
        );

        // SAFETY: We just checked that bytes has at least 4 bytes and are extracting exactly 4 bytes.
        let key_len =
            unsafe { u32::from_le_bytes(bytes[0..4].try_into().unwrap_unchecked()) as usize };

        // Calculate expected base size (without optional fields)
        let base_size = (key_len).saturating_add(
            size_of::<u32>() // key_len
                + size_of::<f32>() // score
                + size_of::<u32>() // flags
                + size_of::<u32>() // max_term_freq
                + size_of::<u32>(), // doc_len
        );

        assert!(
            bytes.len() >= base_size,
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
        let flags: &[u8; size_of::<u32>()] = unsafe {
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

        let parsed_flags = DocumentFlags::from_bits_truncate(u32::from_le_bytes(*flags));
        debug_assert!(
            !parsed_flags.contains(DocumentFlag::HasExpiration)
                || bytes.len() >= base_size + EXPIRATION_SIZE,
            "Insufficient bytes to read expiration fields"
        );

        let (expiration_secs, expiration_nanos) = if parsed_flags
            .contains(DocumentFlag::HasExpiration)
        {
            let exp_bytes = &bytes[doc_len_end..doc_len_end + EXPIRATION_SIZE];

            // SAFETY: We have already validated the slice lengths above.
            let exp_secs = unsafe { exp_bytes[..size_of::<u64>()].try_into().unwrap_unchecked() };
            // SAFETY: We have already validated the slice lengths above.
            let exp_nanos = unsafe { exp_bytes[size_of::<u64>()..].try_into().unwrap_unchecked() };

            (Some(exp_secs), Some(exp_nanos))
        } else {
            (None, None)
        };

        ArchivedDocumentMetadata {
            key,
            score,
            flags: parsed_flags,
            max_term_freq,
            doc_len,
            expiration_secs,
            expiration_nanos,
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
        self.flags
    }

    #[inline(always)]
    pub fn max_term_freq(&self) -> u32 {
        u32::from_le_bytes(*self.max_term_freq)
    }

    #[inline(always)]
    pub fn doc_len(&self) -> u32 {
        u32::from_le_bytes(*self.doc_len)
    }

    #[inline(always)]
    pub fn expiration(&self) -> Option<SystemTime> {
        match (self.expiration_secs, self.expiration_nanos) {
            (Some(secs), Some(nanos)) => {
                let secs = u64::from_le_bytes(*secs);
                let nanos = u32::from_le_bytes(*nanos);
                Some(UNIX_EPOCH + Duration::new(secs, nanos))
            }
            _ => None,
        }
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
            expiration: archived.expiration(),
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
        expiration: Option<SystemTime>,
    ) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(key.len() as u32).to_le_bytes());
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(&score.to_le_bytes());
        bytes.extend_from_slice(&flags.bits().to_le_bytes());
        bytes.extend_from_slice(&max_term_freq.to_le_bytes());
        bytes.extend_from_slice(&doc_len.to_le_bytes());
        if let Some(exp) = expiration {
            let duration = exp.duration_since(UNIX_EPOCH).unwrap();
            bytes.extend_from_slice(&duration.as_secs().to_le_bytes());
            bytes.extend_from_slice(&duration.subsec_nanos().to_le_bytes());
        }
        bytes
    }

    #[test]
    fn document_metadata_roundtrip_without_expiration() {
        let original = DocumentMetadata {
            key: Key::from("document_1"),
            score: 3.5,
            flags: DocumentFlag::Deleted.into(),
            max_term_freq: 10,
            doc_len: 100,
            expiration: None,
        };

        let serialized = original.as_speedb_value();
        let deserialized = DocumentMetadata::from_speedb_value(&serialized);

        assert_eq!(original, deserialized);
    }

    #[test]
    fn document_metadata_roundtrip_with_expiration() {
        let expiration_time = UNIX_EPOCH + Duration::new(1706460000, 123456789);
        let original = DocumentMetadata {
            key: Key::from("document_1"),
            score: 3.5,
            flags: DocumentFlag::Deleted | DocumentFlag::HasExpiration,
            max_term_freq: 10,
            doc_len: 100,
            expiration: Some(expiration_time),
        };

        let serialized = original.as_speedb_value();
        let deserialized = DocumentMetadata::from_speedb_value(&serialized);

        assert_eq!(original, deserialized);
    }

    #[test]
    #[should_panic(expected = "HasExpiration flag must be set when accessing expiration time")]
    fn document_metadata_roundtrip_expiration_without_flag() {
        let expiration_time = UNIX_EPOCH + Duration::new(1706460000, 123456789);
        let original = DocumentMetadata {
            key: Key::from("document_1"),
            score: 3.5,
            flags: DocumentFlag::Deleted.into(), // No HasExpiration flag
            max_term_freq: 10,
            doc_len: 100,
            expiration: Some(expiration_time), // But expiration is Some
        };

        original.as_speedb_value(); // This should panic since HasExpiration flag is not set and expiration is Some
    }

    #[test]
    fn archived_expiration_without_flag_returns_none() {
        let expiration = Some(UNIX_EPOCH + Duration::new(1706460000, 123456789));
        let flags = DocumentFlag::Deleted.into(); // No HasExpiration flag

        let bytes = build_bytes(b"key", 1.0, flags, 10, 100, expiration);
        let archived = DocumentMetadata::archive_from_speedb_value(&bytes);

        // Without the HasExpiration flag, expiration data shouldn't be read
        assert_eq!(archived.expiration(), None);
    }

    #[test]
    fn access_each_field() {
        let key = b"hello";
        let score: f32 = 1.5;
        let flags = DocumentFlag::Deleted | DocumentFlag::HasExpiration;
        let max_term_freq: u32 = 10;
        let doc_len: u32 = 100;
        let expiration = Some(UNIX_EPOCH + Duration::new(1706460000, 123456789));

        let bytes = build_bytes(key, score, flags, max_term_freq, doc_len, expiration);
        let archived = DocumentMetadata::archive_from_speedb_value(&bytes);

        assert_eq!(archived.key(), key);
        assert_eq!(archived.score(), score);
        assert_eq!(archived.max_term_freq(), max_term_freq);
        assert_eq!(archived.doc_len(), doc_len);
        assert_eq!(archived.expiration(), expiration);

        let flags = archived.flags();
        assert!(flags.contains(DocumentFlag::Deleted));
        assert!(flags.contains(DocumentFlag::HasExpiration));
    }

    #[test]
    fn zero_values() {
        let bytes = build_bytes(b"key", 0.0, DocumentFlags::empty(), 0, 0, None);
        let archived = DocumentMetadata::archive_from_speedb_value(&bytes);

        assert_eq!(archived.score(), 0.0);
        assert_eq!(archived.flags(), DocumentFlags::empty());
        assert_eq!(archived.max_term_freq(), 0);
        assert_eq!(archived.doc_len(), 0);
        assert_eq!(archived.expiration(), None);
    }

    #[test]
    fn max_u32_values() {
        let flags = DocumentFlags::all();
        // Use max values for expiration as well
        let expiration = Some(UNIX_EPOCH + Duration::new(u64::MAX / 2, 999_999_999));
        let bytes = build_bytes(b"key", f32::MAX, flags, u32::MAX, u32::MAX, expiration);
        let archived = DocumentMetadata::archive_from_speedb_value(&bytes);

        assert_eq!(archived.score(), f32::MAX);
        assert_eq!(archived.max_term_freq(), u32::MAX);
        assert_eq!(archived.doc_len(), u32::MAX);
        assert_eq!(archived.flags(), flags);
        assert_eq!(archived.expiration(), expiration);
    }

    #[test]
    fn negative_score() {
        let bytes = build_bytes(b"key", -42.5, DocumentFlag::Deleted.into(), 5, 100, None);
        let archived = DocumentMetadata::archive_from_speedb_value(&bytes);

        assert_eq!(archived.score(), -42.5);
    }

    #[test]
    #[should_panic(expected = "Insufficient bytes to read key length")]
    fn insufficient_bytes_for_key_length() {
        let bytes = vec![0u8, 1, 2]; // Only 3 bytes, need at least 4
        DocumentMetadata::archive_from_speedb_value(&bytes);
    }

    #[test]
    #[should_panic(expected = "Insufficient bytes to read complete DocumentMetadata")]
    fn insufficient_bytes_for_key_data() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(10u32).to_le_bytes()); // Key length is 10
        bytes.extend_from_slice(b"short"); // But only 5 bytes of key data
        DocumentMetadata::archive_from_speedb_value(&bytes);
    }

    #[test]
    #[should_panic(expected = "Insufficient bytes to read complete DocumentMetadata")]
    fn insufficient_bytes_for_score() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(3u32).to_le_bytes()); // Key length is 3
        bytes.extend_from_slice(b"key");
        bytes.extend_from_slice(&[0u8, 1]); // Only 2 bytes for score, need 4
        DocumentMetadata::archive_from_speedb_value(&bytes);
    }

    #[test]
    #[should_panic(expected = "Insufficient bytes to read complete DocumentMetadata")]
    fn insufficient_bytes_for_flags() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(3u32).to_le_bytes()); // Key length is 3
        bytes.extend_from_slice(b"key");
        bytes.extend_from_slice(&(1.5f32).to_le_bytes()); // Score
        bytes.extend_from_slice(&[0u8, 1]); // Only 2 bytes for flags, need 4
        DocumentMetadata::archive_from_speedb_value(&bytes);
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
        DocumentMetadata::archive_from_speedb_value(&bytes);
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
        DocumentMetadata::archive_from_speedb_value(&bytes);
    }

    #[test]
    fn extra_bytes_ignored() {
        let flags = DocumentFlag::Deleted | DocumentFlag::HasExpiration;
        let expiration = Some(UNIX_EPOCH + Duration::new(1706460000, 123456789));
        let mut bytes = build_bytes(b"key", 1.0, flags, 3, 4, expiration);
        bytes.extend_from_slice(b"extra_data_that_should_be_ignored");

        let archived = DocumentMetadata::archive_from_speedb_value(&bytes);

        assert_eq!(archived.key(), b"key");
        assert_eq!(archived.score(), 1.0);
        assert_eq!(archived.flags(), flags);
        assert_eq!(archived.max_term_freq(), 3);
        assert_eq!(archived.doc_len(), 4);
        assert_eq!(archived.expiration(), expiration);
    }

    #[test]
    fn from_archived_to_document_metadata() {
        let key = b"document_key";
        let score: f32 = 5.5;
        let flags: DocumentFlags = DocumentFlag::Deleted.into();
        let max_term_freq: u32 = 200;
        let doc_len: u32 = 300;

        let archived = ArchivedDocumentMetadata {
            key,
            score: &score.to_le_bytes(),
            flags,
            max_term_freq: &max_term_freq.to_le_bytes(),
            doc_len: &doc_len.to_le_bytes(),
            expiration_secs: None,
            expiration_nanos: None,
        };

        let metadata: DocumentMetadata = archived.into();

        assert_eq!(&metadata.key[..], key);
        assert_eq!(metadata.score, score);
        assert_eq!(metadata.flags, flags);
        assert_eq!(metadata.max_term_freq, max_term_freq);
        assert_eq!(metadata.doc_len, doc_len);
        assert_eq!(metadata.expiration, None);
    }

    #[test]
    fn from_archived_to_document_metadata_with_expiration() {
        let key = b"document_key";
        let score: f32 = 5.5;
        let flags: DocumentFlags = DocumentFlag::Deleted | DocumentFlag::HasExpiration;
        let max_term_freq: u32 = 200;
        let doc_len: u32 = 300;
        let exp_secs: u64 = 1706460000;
        let exp_nanos: u32 = 123456789;

        let exp_secs_bytes = exp_secs.to_le_bytes();
        let exp_nanos_bytes = exp_nanos.to_le_bytes();

        let archived = ArchivedDocumentMetadata {
            key,
            score: &score.to_le_bytes(),
            flags,
            max_term_freq: &max_term_freq.to_le_bytes(),
            doc_len: &doc_len.to_le_bytes(),
            expiration_secs: Some(&exp_secs_bytes),
            expiration_nanos: Some(&exp_nanos_bytes),
        };

        let metadata: DocumentMetadata = archived.into();

        assert_eq!(&metadata.key[..], key);
        assert_eq!(metadata.score, score);
        assert_eq!(metadata.flags, flags);
        assert_eq!(metadata.max_term_freq, max_term_freq);
        assert_eq!(metadata.doc_len, doc_len);
        assert_eq!(
            metadata.expiration,
            Some(UNIX_EPOCH + Duration::new(exp_secs, exp_nanos))
        );
    }

    #[test]
    fn special_characters_in_key() {
        let key = b"key\x00with\x01special\xffchars";
        let flags = DocumentFlag::Deleted | DocumentFlag::HasExpiration;
        let expiration = Some(UNIX_EPOCH + Duration::new(1706460000, 123456789));
        let bytes = build_bytes(key, 7.2, flags, 500, 1000, expiration);
        let archived = DocumentMetadata::archive_from_speedb_value(&bytes);

        assert_eq!(archived.key(), key);
        assert_eq!(archived.score(), 7.2);
        assert_eq!(archived.flags(), flags);
        assert_eq!(archived.max_term_freq(), 500);
        assert_eq!(archived.doc_len(), 1000);
        assert_eq!(archived.expiration(), expiration);
    }

    #[test]
    #[should_panic(expected = "Insufficient bytes to read expiration fields")]
    fn insufficient_bytes_for_expiration() {
        // Build bytes with HasExpiration flag but without expiration data
        let flags: DocumentFlags = DocumentFlag::HasExpiration.into();
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(3u32).to_le_bytes()); // Key length is 3
        bytes.extend_from_slice(b"key");
        bytes.extend_from_slice(&(1.5f32).to_le_bytes()); // Score
        bytes.extend_from_slice(&flags.bits().to_le_bytes()); // Flags with HasExpiration
        bytes.extend_from_slice(&(10u32).to_le_bytes()); // max_term_freq
        bytes.extend_from_slice(&(100u32).to_le_bytes()); // doc_len
        // Missing expiration bytes
        DocumentMetadata::archive_from_speedb_value(&bytes);
    }
}
