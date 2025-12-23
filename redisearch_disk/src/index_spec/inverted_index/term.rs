pub mod block;

use ffi::t_docId;
use std::mem::size_of;

/// A document in a postings list, including its ID and associated metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Document {
    pub doc_id: t_docId,
    pub metadata: Metadata,
}

/// Metadata associated with a term in a document, including the fields it appears in and its frequency.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Metadata {
    pub field_mask: u128,
    pub frequency: u64,
}

impl Metadata {
    pub const SIZE: usize = Self::FIELD_MASK_SIZE + Self::FREQUENCY_SIZE;
    pub const FIELD_MASK_SIZE: usize = size_of::<u128>();
    pub const FREQUENCY_SIZE: usize = size_of::<u64>();
}
