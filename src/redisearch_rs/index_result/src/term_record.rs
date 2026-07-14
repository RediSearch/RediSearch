/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use query_term::RSQueryTerm;
use ref_mode::{Active, Ref};

use super::offsets::{RSOffsetSlice, RSOffsetVector, RawOffsetSlice};

/// Represents a single record of a document inside a term in the inverted index
#[cheadergen::config(prefix_with_name)]
#[derive(Debug)]
#[repr(u8)]
pub enum RawTermRecord<'query, R: Ref> {
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
        /// term"; thanks to the `NonNull` niche of `&T`, `Option<&'query RSQueryTerm>`
        /// has the same C ABI as a nullable `*const RSQueryTerm`.
        ///
        /// The borrowed query term belongs to the query pipeline, so it lives
        /// under the `'query` lifetime and is never weakened by the
        /// `Active`/`Suspended` ref-mode transitions — it stays a genuine
        /// `&'query` borrow, so it is modelled as a plain reference rather than
        /// a ref-mode-parametrised `SharedPtr`.
        term: Option<&'query RSQueryTerm>,

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
#[cheadergen::config(export)]
pub type RSTermRecord<'a> = RawTermRecord<'a, Active<'a>>;

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
    type A = RawTermRecord<'static, Active<'static>>;
    type S = RawTermRecord<'static, Suspended>;
    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

// Manual (rather than derived) so equality is well-defined across the three
// variants (which store the term and offsets differently): compares the
// dereferenced query term and the byte view of the offsets.
impl<'a> PartialEq for RSTermRecord<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.query_term() == other.query_term() && self.offsets() == other.offsets()
    }
}

impl<'a> Eq for RSTermRecord<'a> {}

impl<'query, R: Ref> RawTermRecord<'query, R> {
    /// Create a new term record without term pointer and offsets.
    pub const fn new() -> Self {
        Self::Borrowed {
            term: None,
            offsets: RawOffsetSlice::empty(),
        }
    }

    /// Is this term record borrowed or owned?
    pub const fn is_copy(&self) -> bool {
        matches!(self, Self::Owned { .. } | Self::FullyOwned { .. })
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
            Self::Borrowed { offsets, .. } => offsets.as_bytes(),
            Self::Owned { offsets, .. } => offsets.as_bytes(),
            Self::FullyOwned { offsets, .. } => offsets.as_bytes(),
        }
    }

    /// Get a reference to the query term of this term record, if one is set.
    pub fn query_term(&self) -> Option<&RSQueryTerm> {
        match self {
            Self::Borrowed { term, .. } => term.as_deref(),
            Self::Owned { term, .. } => *term,
            Self::FullyOwned { term, .. } => term.as_deref(),
        }
    }

    /// Create an owned copy of this term record, allocating new memory for the offsets, but reusing the term.
    pub fn to_owned(&'a self) -> RSTermRecord<'a> {
        let term = self.query_term();
        Self::Owned {
            term,
            offsets: match self {
                Self::Borrowed { offsets, .. } => offsets.to_owned(),
                Self::Owned { offsets, .. } => offsets.as_slice().to_owned(),
                Self::FullyOwned { offsets, .. } => offsets.as_slice().to_owned(),
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
            Self::Borrowed { offsets: o, .. } => {
                *o = offsets;
            }
            Self::Owned { offsets: o, .. } => {
                // Assign the new owned copy; the old value is auto-dropped, freeing old data.
                *o = offsets.to_owned();
            }
            Self::FullyOwned { offsets: o, .. } => {
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
    /// [`RawTermResultBuilder::fully_owned_record`](super::core::RawTermResultBuilder::fully_owned_record)
    /// or [`RawTermResultBuilder::owned_record`](super::core::RawTermResultBuilder::owned_record).
    pub fn set_offsets_owned(&mut self, offsets: RSOffsetVector) {
        match self {
            Self::Borrowed { .. } => {
                panic!(
                    "set_offsets_owned called on RSTermRecord::Borrowed; \
                     construct the record with fully_owned_record or owned_record instead"
                );
            }
            Self::Owned { offsets: o, .. } => {
                *o = offsets;
            }
            Self::FullyOwned { offsets: o, .. } => {
                *o = offsets;
            }
        }
    }
}

impl<'query, R: Ref> Default for RawTermRecord<'query, R> {
    fn default() -> Self {
        Self::new()
    }
}
