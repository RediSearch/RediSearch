/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::fmt::Debug;

use query_term::RSQueryTerm;

use super::offsets::{RSOffsetSlice, RSOffsetVector};

/// Represents a single record of a document inside a term in the inverted index
/// cbindgen:prefix-with-name=true
#[repr(u8)]
pub enum RSTermRecord<'index> {
    Borrowed {
        /// The term that brought up this record.
        ///
        /// The term is owned by the record. The name of the variant, `Borrowed`,
        /// refers to the `offsets` field.
        ///
        /// The term is wrapped in a `Box` to ensure that both `Owned` and `Borrowed`
        /// variants have the same memory layout.
        term: Option<Box<RSQueryTerm>>,

        /// The encoded offsets in which the term appeared in the document
        ///
        /// A decoder can choose to borrow this data from the index block, hence the `'index` lifetime.
        offsets: RSOffsetSlice<'index>,
    },
    Owned {
        /// The term that brought up this record.
        ///
        /// It borrows the term from another record.
        /// The name of the variant, `Owned`, refers to the `offsets` field.
        term: Option<&'index RSQueryTerm>,

        /// The encoded offsets in which the term appeared in the document
        ///
        /// The owned version owns a copy of the offsets data, which is freed on drop.
        offsets: RSOffsetVector,
    },
    FullyOwned {
        /// The term that brought up this record.
        ///
        /// The term is owned by the record (wrapped in a `Box`), same as in the
        /// `Borrowed` variant.
        term: Option<Box<RSQueryTerm>>,

        /// The encoded offsets in which the term appeared in the document.
        ///
        /// Unlike `Borrowed`, the offsets are owned by the record as well and
        /// therefore do not tie the record to an external lifetime. Used when
        /// the decoded record must outlive the source of the offset bytes
        /// (e.g. reading from a disk page that may be evicted).
        offsets: RSOffsetVector,
    },
}

impl PartialEq for RSTermRecord<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.query_term() == other.query_term() && self.offsets() == other.offsets()
    }
}

impl Eq for RSTermRecord<'_> {}

impl<'index> RSTermRecord<'index> {
    /// Create a new term record without term pointer and offsets.
    pub const fn new() -> Self {
        Self::Borrowed {
            term: None,
            offsets: RSOffsetSlice::empty(),
        }
    }

    /// Create a new borrowed term record with the given term and offsets.
    pub const fn with_term(
        term: Box<RSQueryTerm>,
        offsets: RSOffsetSlice<'index>,
    ) -> RSTermRecord<'index> {
        Self::Borrowed {
            term: Some(term),
            offsets,
        }
    }

    /// Is this term record borrowed or owned?
    pub const fn is_copy(&self) -> bool {
        matches!(
            self,
            RSTermRecord::Owned { .. } | RSTermRecord::FullyOwned { .. }
        )
    }

    /// Get the offsets of this term record as a byte slice.
    pub const fn offsets(&self) -> &[u8] {
        match self {
            RSTermRecord::Borrowed { offsets, .. } => offsets.as_bytes(),
            RSTermRecord::Owned { offsets, .. } => offsets.as_bytes(),
            RSTermRecord::FullyOwned { offsets, .. } => offsets.as_bytes(),
        }
    }

    /// Get a reference to the query term of this term record, if one is set.
    pub fn query_term(&self) -> Option<&RSQueryTerm> {
        match self {
            RSTermRecord::Borrowed { term, .. } => term.as_deref(),
            RSTermRecord::Owned { term, .. } => *term,
            RSTermRecord::FullyOwned { term, .. } => term.as_deref(),
        }
    }

    /// Create an owned copy of this term record, allocating new memory for the offsets, but reusing the term.
    pub fn to_owned<'a>(&'a self) -> RSTermRecord<'a> {
        RSTermRecord::Owned {
            term: self.query_term(),
            offsets: match self {
                RSTermRecord::Borrowed { offsets, .. } => offsets.to_owned(),
                RSTermRecord::Owned { offsets, .. } => offsets.as_slice().to_owned(),
                RSTermRecord::FullyOwned { offsets, .. } => offsets.as_slice().to_owned(),
            },
        }
    }

    /// Set the offsets of this term record, replacing any existing offsets.
    ///
    /// For the `Owned` and `FullyOwned` variants the slice is copied into a
    /// fresh allocation, so the input does not need to satisfy any lifetime
    /// relationship beyond the call itself.
    pub fn set_offsets(&mut self, offsets: RSOffsetSlice<'index>) {
        match self {
            RSTermRecord::Borrowed { offsets: o, .. } => {
                *o = offsets;
            }
            RSTermRecord::Owned { offsets: o, .. } => {
                // Assign the new owned copy; the old value is auto-dropped, freeing old data.
                *o = offsets.to_owned();
            }
            RSTermRecord::FullyOwned { offsets: o, .. } => {
                *o = offsets.to_owned();
            }
        }
    }

    /// Set the offsets of this term record from an already-owned
    /// [`RSOffsetVector`].
    ///
    /// Unlike [`Self::set_offsets`], this method does not tie the input to the
    /// record's `'index` lifetime, since an [`RSOffsetVector`] owns its data.
    /// This is the method to use from decoders that borrow their source bytes
    /// with a lifetime shorter than `'index` (for example, a disk-backed block
    /// that may be replaced on the next read).
    ///
    /// # Panics
    ///
    /// Panics if the record is in the [`RSTermRecord::Borrowed`] variant,
    /// which stores an [`RSOffsetSlice`] rather than an owned vector. Callers
    /// that need to call this method should construct the record via
    /// [`RSTermResultBuilder::fully_owned_record`](super::core::RSTermResultBuilder::fully_owned_record)
    /// or [`RSTermResultBuilder::owned_record`](super::core::RSTermResultBuilder::owned_record).
    pub fn set_offsets_owned(&mut self, offsets: RSOffsetVector) {
        match self {
            RSTermRecord::Borrowed { .. } => {
                panic!(
                    "set_offsets_owned called on RSTermRecord::Borrowed; \
                     construct the record with fully_owned_record or owned_record instead"
                );
            }
            RSTermRecord::Owned { offsets: o, .. } => {
                *o = offsets;
            }
            RSTermRecord::FullyOwned { offsets: o, .. } => {
                *o = offsets;
            }
        }
    }
}

impl Debug for RSTermRecord<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RSTermRecord::Borrowed { offsets, .. } => f
                .debug_struct("RSTermRecord(Borrowed)")
                .field("term", &self.query_term())
                .field("offsets", offsets)
                .finish(),
            RSTermRecord::Owned { offsets, .. } => f
                .debug_struct("RSTermRecord(Owned)")
                .field("term", &self.query_term())
                .field("offsets", offsets)
                .finish(),
            RSTermRecord::FullyOwned { offsets, .. } => f
                .debug_struct("RSTermRecord(FullyOwned)")
                .field("term", &self.query_term())
                .field("offsets", offsets)
                .finish(),
        }
    }
}

impl Default for RSTermRecord<'_> {
    fn default() -> Self {
        Self::new()
    }
}
