/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

/// The various document types supported by RediSearch.
///
/// cbindgen:prefix-with-name
#[repr(C)]
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    strum::EnumString,
    strum::Display,
    strum::AsRefStr,
    strum::FromRepr,
)]
pub enum DocumentType {
    /// Hash document type
    #[strum(serialize = "hash")]
    Hash = 0,
    /// JSON document type
    #[strum(serialize = "json")]
    Json = 1,
    /// Unsupported document type
    #[strum(serialize = "unsupported")]
    Unsupported = 2,
}

impl From<u32> for DocumentType {
    fn from(value: u32) -> Self {
        let Ok(value_usize) = value.try_into() else {
            return Self::Unsupported;
        };

        Self::from_repr(value_usize).unwrap_or(Self::Unsupported)
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use crate::DocumentType;

    #[test]
    fn test_serialize_deserialize_from_ffi() {
        #[rustfmt::skip]
        let doc_types = [
            (DocumentType::Hash,        "hash",         0),
            (DocumentType::Json,        "json",         1),
            (DocumentType::Unsupported, "unsupported",  2),
        ];
        for (doc_type, expected_str, expected_ffi) in doc_types {
            let serialized = doc_type.to_string();
            assert_eq!(serialized, expected_str);

            let deserialized = DocumentType::from_str(expected_str).unwrap();
            assert_eq!(deserialized, doc_type);

            let from_num = DocumentType::from(expected_ffi as u32);
            assert_eq!(from_num, doc_type);

            let from_repr = DocumentType::from_repr(expected_ffi).unwrap();
            assert_eq!(from_repr, doc_type);
        }

        assert_eq!(DocumentType::from(u32::MAX), DocumentType::Unsupported);

        assert_eq!(
            DocumentType::from_str("Je n'existe"),
            Err(strum::ParseError::VariantNotFound)
        );

        assert_eq!(DocumentType::from_repr(usize::MAX), None);
    }
}
