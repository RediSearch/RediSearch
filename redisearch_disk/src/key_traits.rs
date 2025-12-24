use crate::document_id_key::{DocumentIdKey, DocumentIdKeyFromKeyError};
use ffi::t_docId;

/// Trait extension to convert various types into byte slices for use as keys.
pub trait AsKeyExt {
    /// Get this value as a Speedb key.
    fn as_key(&self) -> Vec<u8>;
}

/// Trait extension to convert various types from byte slices for use as keys.
pub trait FromKeyExt: Sized {
    /// The error type returned when parsing fails.
    type Error;

    /// Construct this value from a Speedb key.
    fn from_key(key: &[u8]) -> Result<Self, Self::Error>;
}

/// Trait extension to convert various types into/from byte slices for use as keys.
pub trait KeyExt: AsKeyExt + FromKeyExt {}

impl AsKeyExt for t_docId {
    fn as_key(&self) -> Vec<u8> {
        let doc_id_key: DocumentIdKey = (*self).into();
        doc_id_key.as_key()
    }
}

impl FromKeyExt for t_docId {
    type Error = DocumentIdKeyFromKeyError;

    fn from_key(key: &[u8]) -> Result<Self, Self::Error> {
        let doc_id_key = DocumentIdKey::from_key(key)?;
        Ok(doc_id_key.as_num())
    }
}
