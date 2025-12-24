use std::{fmt::Display, str::FromStr};

use ffi::t_docId;
use thiserror::Error;

use crate::key_traits::{AsKeyExt, FromKeyExt, KeyExt};

/// This newtype is used to represent a document ID as a key of a Speedb column entry
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub struct DocumentIdKey(t_docId);

impl DocumentIdKey {
    const FMT_PADDING_LENGTH: usize = 20;

    /// Returns the document ID as a number
    pub fn as_num(&self) -> t_docId {
        self.0
    }
}

impl Display for DocumentIdKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // We want the ID to be lexicographically comparable, so we pad it with leading zeros
        write!(f, "{:0width$}", self.0, width = Self::FMT_PADDING_LENGTH)
    }
}

impl From<t_docId> for DocumentIdKey {
    fn from(doc_id: t_docId) -> Self {
        DocumentIdKey(doc_id)
    }
}

impl FromStr for DocumentIdKey {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let doc_id = s.parse::<t_docId>()?;
        Ok(DocumentIdKey(doc_id))
    }
}

impl AsKeyExt for DocumentIdKey {
    fn as_key(&self) -> Vec<u8> {
        self.to_string().into_bytes()
    }
}

#[derive(Debug, Error)]
pub enum DocumentIdKeyFromKeyError {
    #[error("Invalid UTF-8 in document ID key")]
    InvalidUtf8(#[from] std::str::Utf8Error),
    #[error("Failed to parse document ID")]
    ParseInt(#[from] std::num::ParseIntError),
}

impl FromKeyExt for DocumentIdKey {
    type Error = DocumentIdKeyFromKeyError;

    fn from_key(key: &[u8]) -> Result<Self, Self::Error> {
        let s = std::str::from_utf8(key)?;
        Ok(s.parse()?)
    }
}

impl KeyExt for DocumentIdKey {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_id_key_zero() {
        let doc_id = DocumentIdKey(0);
        assert_eq!(doc_id.to_string(), "00000000000000000000");
        assert_eq!(doc_id.as_num(), 0);
    }

    #[test]
    fn document_id_key_max_value() {
        let doc_id = DocumentIdKey(u64::MAX);
        let formatted = doc_id.to_string();
        assert_eq!(formatted.len(), 20);
        assert_eq!(formatted, "18446744073709551615");
        assert_eq!(doc_id.as_num(), u64::MAX);
    }

    #[test]
    fn document_id_key_lexicographic_ordering() {
        // Verify that string representations are lexicographically ordered
        let id9 = DocumentIdKey(9).to_string();
        let id73 = DocumentIdKey(73).to_string();
        let id512 = DocumentIdKey(512).to_string();
        let id1000 = DocumentIdKey(1000).to_string();

        assert!(id9 < id73);
        assert!(id73 < id512);
        assert!(id512 < id1000);
    }

    #[test]
    fn document_id_key_from_str_valid() {
        let doc_id: DocumentIdKey = "42".parse().unwrap();
        assert_eq!(doc_id.as_num(), 42);

        let doc_id_padded: DocumentIdKey = "00000000000000000042".parse().unwrap();
        assert_eq!(doc_id_padded.as_num(), 42);
        assert_eq!(doc_id, doc_id_padded);
    }

    #[test]
    fn document_id_key_from_str_invalid() {
        let result: Result<DocumentIdKey, _> = "not_a_number".parse();
        assert!(result.is_err());
        assert!(result.unwrap_err().kind() == &std::num::IntErrorKind::InvalidDigit);

        let result: Result<DocumentIdKey, _> = "".parse();
        assert!(result.is_err());
        assert!(result.unwrap_err().kind() == &std::num::IntErrorKind::Empty);

        let result: Result<DocumentIdKey, _> = "-42".parse();
        assert!(result.is_err());
        // Negative numbers are invalid digits for unsigned integers
        assert!(result.unwrap_err().kind() == &std::num::IntErrorKind::InvalidDigit);
    }

    #[test]
    fn document_id_key_from_str_overflow() {
        // Attempting to parse a number larger than u64::MAX should fail
        let result: Result<DocumentIdKey, _> = "18446744073709551616".parse();
        assert!(result.is_err());
        assert!(*result.unwrap_err().kind() == std::num::IntErrorKind::PosOverflow);
    }

    #[test]
    fn document_id_key_equality() {
        let doc_id1 = DocumentIdKey(42);
        let doc_id2 = DocumentIdKey(42);
        let doc_id3 = DocumentIdKey(43);

        assert_eq!(doc_id1, doc_id2);
        assert_ne!(doc_id1, doc_id3);
    }

    #[test]
    fn document_id_key_roundtrip_conversion() {
        // Test that converting to string and back preserves the value
        for id in [1, 42, 1000, 999999] {
            let original = DocumentIdKey(id);
            let string_repr = original.to_string();
            let parsed: DocumentIdKey = string_repr.parse().unwrap();
            assert_eq!(original, parsed, "for id: {id}");
            assert_eq!(original.as_num(), parsed.as_num(), "for id: {id}");
        }
    }

    #[test]
    fn document_id_key_as_key_from_key_roundtrip() {
        for id in [0, 1, 42, 1000, 999999, u64::MAX] {
            let original = DocumentIdKey(id);
            let parsed = DocumentIdKey::from_key(&original.as_key()).unwrap();
            assert_eq!(original, parsed, "for id: {id}");
            assert_eq!(original.as_num(), parsed.as_num(), "for id: {id}");
        }
    }

    #[test]
    fn document_id_key_from_key_invalid_utf8() {
        // Test with invalid UTF-8 bytes
        let invalid_utf8 = vec![0xFF, 0xFE, 0xFD];
        let result = DocumentIdKey::from_key(&invalid_utf8);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            DocumentIdKeyFromKeyError::InvalidUtf8(_)
        ));
    }

    #[test]
    fn document_id_key_from_key_parse_error() {
        // Test with valid UTF-8 but invalid number
        let invalid_number = b"not_a_number";
        let result = DocumentIdKey::from_key(invalid_number);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            DocumentIdKeyFromKeyError::ParseInt(_)
        ));

        // Test with empty string
        let empty = b"";
        let result = DocumentIdKey::from_key(empty);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            DocumentIdKeyFromKeyError::ParseInt(_)
        ));

        // Test with overflow
        let overflow = b"18446744073709551616";
        let result = DocumentIdKey::from_key(overflow);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            DocumentIdKeyFromKeyError::ParseInt(_)
        ));
    }
}
