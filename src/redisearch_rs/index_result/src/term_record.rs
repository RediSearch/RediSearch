/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use query_term::RSQueryTerm;

use crate::SharedQueryTerm;
use ref_mode::{Active, Ref};

use super::offsets::{RSOffsetSlice, RSOffsetVector, RawOffsetSlice};

/// Represents a single record of a document inside a term in the inverted index
#[cheadergen::config(prefix_with_name)]
#[derive(Debug)]
#[repr(u8)]
pub enum RawTermRecord<R: Ref> {
    /// A record whose offsets may borrow from the index block they were decoded from, so it
    /// cannot outlive that block. Its term is shared, not borrowed.
    ///
    /// [`RSTermRecord::to_owned`] turns it into an [`Owned`](Self::Owned) record.
    Borrowed {
        /// The term that brought up this record.
        ///
        /// Shared so that [`RSTermRecord::to_owned`] copies of the record
        /// can keep the term alive without copying it.
        term: Option<SharedQueryTerm>,

        /// The encoded offsets in which the term appeared in the document
        ///
        /// A decoder can choose to borrow this data from the index block, hence the `R`
        /// parameter (which carries the index lifetime in [`Active`] mode).
        offsets: RawOffsetSlice<R>,
    },
    /// A record that borrows nothing, so it can outlive both the source of its offsets and the
    /// iterator holding its term (see [`RSTermRecord::to_owned`]).
    Owned {
        /// The term that brought up this record.
        ///
        /// The record shares ownership of the term, same as in the `Borrowed`
        /// variant.
        term: Option<SharedQueryTerm>,

        /// The encoded offsets in which the term appeared in the document.
        ///
        /// Unlike `Borrowed`, the offsets are owned by the record as well and
        /// therefore do not tie the record to an external lifetime.
        offsets: RSOffsetVector,
    },
}

/// The [`Active`] instantiation of [`RawTermRecord`].
#[cheadergen::config(export)]
pub type RSTermRecord<'a> = RawTermRecord<Active<'a>>;

// Compile-time proof that the `Active` and `Suspended` instantiations of
// `RawTermRecord` are layout-identical. Only `size_of`/`align_of` are checked:
// `offset_of!` cannot address `#[repr(u8)]` enum variant fields. The variant
// payloads that carry `R` are guarded one level deeper — `RawOffsetSlice`
// (`offsets.rs`) and `SharedPtr` (`ref_mode`) — so the internals this block
// cannot see are still pinned. Part of the recursive net backing the
// conversions on `RawIndexResult` (see `core/mod.rs`).
const _: () = {
    use ref_mode::Suspended;
    use std::mem::{align_of, size_of};
    type A = RawTermRecord<Active<'static>>;
    type S = RawTermRecord<Suspended>;
    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

// Manual (rather than derived) so equality is well-defined across the two
// variants (which store the term and offsets differently): compares the
// dereferenced query term and the byte view of the offsets.
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
        matches!(self, Self::Owned { .. })
    }

    /// Get a reference to the query term of this term record, if one is set.
    pub fn query_term(&self) -> Option<&RSQueryTerm> {
        match self {
            Self::Borrowed { term, .. } | Self::Owned { term, .. } => term.as_deref(),
        }
    }
}

impl<'a> RSTermRecord<'a> {
    /// Create a new borrowed term record with the given term and offsets.
    pub fn with_term(term: Box<RSQueryTerm>, offsets: RSOffsetSlice<'a>) -> RSTermRecord<'a> {
        Self::Borrowed {
            term: Some(SharedQueryTerm::new(term)),
            offsets,
        }
    }

    /// Get the offsets of this term record as a byte slice.
    pub const fn offsets(&self) -> &[u8] {
        match self {
            Self::Borrowed { offsets, .. } => offsets.as_bytes(),
            Self::Owned { offsets, .. } => offsets.as_bytes(),
        }
    }

    /// Create a copy of this term record that owns its offsets and shares ownership of its term.
    ///
    /// The copy borrows nothing from `self`, so it stays valid after the record, or the
    /// iterator holding its term, is dropped.
    pub fn to_owned(&self) -> RSTermRecord<'a> {
        let term = match self {
            Self::Borrowed { term, .. } | Self::Owned { term, .. } => term.clone(),
        };
        Self::Owned {
            term,
            offsets: self.owned_offsets(),
        }
    }

    /// Copy the offsets of this term record into a fresh allocation.
    fn owned_offsets(&self) -> RSOffsetVector {
        match self {
            Self::Borrowed { offsets, .. } => offsets.to_owned(),
            Self::Owned { offsets, .. } => offsets.as_slice().to_owned(),
        }
    }

    /// Set the offsets of this term record, replacing any existing offsets.
    ///
    /// For the `Owned` variant the slice is copied into a fresh allocation, so the input does not
    /// need to satisfy any lifetime relationship beyond the call itself.
    pub fn set_offsets(&mut self, offsets: RSOffsetSlice<'a>) {
        match self {
            Self::Borrowed { offsets: o, .. } => {
                *o = offsets;
            }
            Self::Owned { offsets: o, .. } => {
                // Assign the new owned copy; the old value is auto-dropped, freeing old data.
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
    /// Panics if the record is in the [`Self::Borrowed`] variant,
    /// which stores an [`RSOffsetSlice`] rather than an owned vector. Callers
    /// that need to call this method should construct the record via
    /// [`RawTermResultBuilder::owned_record`](super::core::RawTermResultBuilder::owned_record).
    pub fn set_offsets_owned(&mut self, offsets: RSOffsetVector) {
        match self {
            Self::Borrowed { .. } => {
                panic!(
                    "set_offsets_owned called on RSTermRecord::Borrowed; \
                     construct the record with owned_record instead"
                );
            }
            Self::Owned { offsets: o, .. } => {
                *o = offsets;
            }
        }
    }
}

impl<R: Ref> Default for RawTermRecord<R> {
    fn default() -> Self {
        Self::new()
    }
}
