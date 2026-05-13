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
use ref_mode::{Active, Ptr, Ref};

use super::offsets::{RSOffsetSlice, RSOffsetVector, RawOffsetSlice};

/// Represents a single record of a document inside a term in the inverted index
#[cheadergen::config(prefix_with_name)]
#[repr(u8)]
pub enum RawTermRecord<R: Ref> {
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
        /// A decoder can choose to borrow this data from the index block, hence the `R`
        /// parameter (which carries the index lifetime in [`Active`] mode).
        offsets: RawOffsetSlice<R>,
    },
    Owned {
        /// The term that brought up this record.
        ///
        /// It borrows the term from another record. `None` encodes "no
        /// term"; thanks to `NonNull`'s niche, `Option<Ptr<R, RSQueryTerm>>`
        /// has the same C ABI as a nullable `*const RSQueryTerm`.
        term: Option<Ptr<R, RSQueryTerm>>,

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

/// The [`Active`] instantiation of [`RawTermRecord`].
pub type RSTermRecord<'a> = RawTermRecord<Active<'a>>;

impl<'a> PartialEq for RSTermRecord<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.query_term() == other.query_term() && self.offsets() == other.offsets()
    }
}

impl<'a> Eq for RSTermRecord<'a> {}

impl<R: Ref> RawTermRecord<R> {
    /// Create a new term record without term pointer and offsets.
    pub const fn new() -> Self {
        Self::Borrowed {
            term: None,
            offsets: RawOffsetSlice::empty(),
        }
    }

    /// Is this term record borrowed or owned?
    pub const fn is_copy(&self) -> bool {
        matches!(
            self,
            RawTermRecord::Owned { .. } | RawTermRecord::FullyOwned { .. }
        )
    }
}

impl<'a> RSTermRecord<'a> {
    /// Create a new borrowed term record with the given term and offsets.
    pub const fn with_term(term: Box<RSQueryTerm>, offsets: RSOffsetSlice<'a>) -> RSTermRecord<'a> {
        Self::Borrowed {
            term: Some(term),
            offsets,
        }
    }

    /// Get the offsets of this term record as a byte slice.
    pub const fn offsets(&self) -> &[u8] {
        match self {
            RawTermRecord::Borrowed { offsets, .. } => offsets.as_bytes(),
            RawTermRecord::Owned { offsets, .. } => offsets.as_bytes(),
            RawTermRecord::FullyOwned { offsets, .. } => offsets.as_bytes(),
        }
    }

    /// Get a reference to the query term of this term record, if one is set.
    pub fn query_term(&self) -> Option<&RSQueryTerm> {
        match self {
            RawTermRecord::Borrowed { term, .. } => term.as_deref(),
            RawTermRecord::Owned { term, .. } => term.map(|p| p.get()),
            RawTermRecord::FullyOwned { term, .. } => term.as_deref(),
        }
    }

    /// Create an owned copy of this term record, allocating new memory for the offsets, but reusing the term.
    pub fn to_owned(&'a self) -> RSTermRecord<'a> {
        let term = self.query_term().map(Ptr::new);
        RawTermRecord::Owned {
            term,
            offsets: match self {
                RawTermRecord::Borrowed { offsets, .. } => offsets.to_owned(),
                RawTermRecord::Owned { offsets, .. } => offsets.as_slice().to_owned(),
                RawTermRecord::FullyOwned { offsets, .. } => offsets.as_slice().to_owned(),
            },
        }
    }

    /// Set the offsets of this term record, replacing any existing offsets.
    ///
    /// For the `Owned` and `FullyOwned` variants the slice is copied into a
    /// fresh allocation, so the input does not need to satisfy any lifetime
    /// relationship beyond the call itself.
    pub fn set_offsets(&mut self, offsets: RSOffsetSlice<'a>) {
        match self {
            RawTermRecord::Borrowed { offsets: o, .. } => {
                *o = offsets;
            }
            RawTermRecord::Owned { offsets: o, .. } => {
                // Assign the new owned copy; the old value is auto-dropped, freeing old data.
                *o = offsets.to_owned();
            }
            RawTermRecord::FullyOwned { offsets: o, .. } => {
                *o = offsets.to_owned();
            }
        }
    }

    /// Set the offsets of this term record from an already-owned
    /// [`RSOffsetVector`].
    ///
    /// Unlike [`Self::set_offsets`], this method does not tie the input to the
    /// record's `'a` lifetime, since an [`RSOffsetVector`] owns its data.
    /// This is the method to use from decoders that borrow their source bytes
    /// with a lifetime shorter than `'a` (for example, a disk-backed block
    /// that may be replaced on the next read).
    ///
    /// # Panics
    ///
    /// Panics if the record is in the [`RawTermRecord::Borrowed`] variant,
    /// which stores an [`RSOffsetSlice`] rather than an owned vector. Callers
    /// that need to call this method should construct the record via
    /// [`RawTermResultBuilder::fully_owned_record`](super::core::RawTermResultBuilder::fully_owned_record)
    /// or [`RawTermResultBuilder::owned_record`](super::core::RawTermResultBuilder::owned_record).
    pub fn set_offsets_owned(&mut self, offsets: RSOffsetVector) {
        match self {
            RawTermRecord::Borrowed { .. } => {
                panic!(
                    "set_offsets_owned called on RSTermRecord::Borrowed; \
                     construct the record with fully_owned_record or owned_record instead"
                );
            }
            RawTermRecord::Owned { offsets: o, .. } => {
                *o = offsets;
            }
            RawTermRecord::FullyOwned { offsets: o, .. } => {
                *o = offsets;
            }
        }
    }
}

impl<'a> Debug for RSTermRecord<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RawTermRecord::Borrowed { offsets, .. } => f
                .debug_struct("RSTermRecord(Borrowed)")
                .field("term", &self.query_term())
                .field("offsets", offsets)
                .finish(),
            RawTermRecord::Owned { offsets, .. } => f
                .debug_struct("RSTermRecord(Owned)")
                .field("term", &self.query_term())
                .field("offsets", offsets)
                .finish(),
            RawTermRecord::FullyOwned { offsets, .. } => f
                .debug_struct("RSTermRecord(FullyOwned)")
                .field("term", &self.query_term())
                .field("offsets", offsets)
                .finish(),
        }
    }
}

impl<R: Ref> Default for RawTermRecord<R> {
    fn default() -> Self {
        Self::new()
    }
}
