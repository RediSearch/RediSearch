/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::fmt;
use std::ptr::NonNull;

use numeric_range_tree::NumericRangeTree;
use tag_index::{InMemoryMode, Tag, TagIndex};

/// Whether a numeric terminal stores its values with float compression.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FloatCompression {
    /// Values are stored as given.
    Off,
    /// Values that fit are stored in a smaller representation. See
    /// [`NumericRangeTree::new`].
    On,
}

/// The kind of every [`Terminal`] of a [`NestedIndex`](crate::NestedIndex), set by the type of the
/// last attribute in `INDEX`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalKind {
    /// The last attribute is NUMERIC.
    Numeric(FloatCompression),
    /// The last attribute is TAG.
    Tag,
}

impl TerminalKind {
    /// Whether `value` has the type a terminal of this kind stores.
    pub const fn accepts(&self, value: &TerminalValue<'_>) -> bool {
        matches!(
            (self, value),
            (Self::Numeric(_), TerminalValue::Numeric(_)) | (Self::Tag, TerminalValue::Tag(_))
        )
    }
}

/// The postings at the end of one path: the elements whose routing values are exactly that path.
///
/// A numeric terminal holds the [`NumericRangeTree`] a top-level NUMERIC field uses, so the
/// numeric iterators that read those fields read it unchanged. A tag terminal holds a
/// [`TagIndex`].
///
/// The terminal owns its tree or tag index through the pointer [`Box::into_raw`] returned, as a
/// FieldSpec owns a top-level field's. Iterators that outlive a lock release need that owner's
/// pointer, not one derived from a reference, since a writer's `&mut` would invalidate the latter;
/// [`Terminal::numeric_ptr`] and [`Terminal::tag_ptr`] hand it out.
pub struct Terminal(Postings);

enum Postings {
    Numeric(NonNull<NumericRangeTree>),
    Tag(NonNull<TagIndex<InMemoryMode>>),
}

/// A terminal's postings, borrowed for writing.
pub(crate) enum PostingsMut<'a> {
    Numeric(&'a mut NumericRangeTree),
    Tag(&'a mut TagIndex<InMemoryMode>),
}

impl Terminal {
    /// An empty terminal of the given kind.
    pub fn new(kind: TerminalKind) -> Self {
        Self(match kind {
            TerminalKind::Numeric(compression) => {
                let tree = NumericRangeTree::new(matches!(compression, FloatCompression::On));
                Postings::Numeric(owned(tree))
            }
            // No suffix trie: NESTED sub-fields reject WITHSUFFIXTRIE.
            TerminalKind::Tag => {
                let tags = TagIndex::<InMemoryMode>::new(false);
                Postings::Tag(owned(tags))
            }
        })
    }

    /// The tree of a numeric terminal.
    pub const fn numeric(&self) -> Option<&NumericRangeTree> {
        match self.0 {
            // SAFETY: the tree is live while the terminal is, and written only through
            // `postings_mut`, which borrows the terminal mutably.
            Postings::Numeric(tree) => Some(unsafe { tree.as_ref() }),
            Postings::Tag(_) => None,
        }
    }

    /// The tag index of a tag terminal.
    pub const fn tag(&self) -> Option<&TagIndex<InMemoryMode>> {
        match self.0 {
            Postings::Numeric(_) => None,
            // SAFETY: as in `numeric`.
            Postings::Tag(tags) => Some(unsafe { tags.as_ref() }),
        }
    }

    /// The owner's pointer to a numeric terminal's tree, as the numeric iterators' `RangeSource`
    /// requires. It stays valid until the terminal is dropped.
    pub const fn numeric_ptr(&self) -> Option<NonNull<NumericRangeTree>> {
        match self.0 {
            Postings::Numeric(tree) => Some(tree),
            Postings::Tag(_) => None,
        }
    }

    /// The owner's pointer to a tag terminal's index, as
    /// [`TrieLookup::new`](tag_index::TrieLookup::new) requires. It stays valid until the
    /// terminal is dropped.
    pub const fn tag_ptr(&self) -> Option<NonNull<TagIndex<InMemoryMode>>> {
        match self.0 {
            Postings::Numeric(_) => None,
            Postings::Tag(tags) => Some(tags),
        }
    }

    /// The postings, for writing.
    pub(crate) const fn postings_mut(&mut self) -> PostingsMut<'_> {
        match self.0 {
            // SAFETY: the terminal is borrowed mutably, so nothing else reaches the tree through
            // it; the owner's pointer is reborrowed, raw → `&mut` → raw.
            Postings::Numeric(mut tree) => PostingsMut::Numeric(unsafe { tree.as_mut() }),
            // SAFETY: as above.
            Postings::Tag(mut tags) => PostingsMut::Tag(unsafe { tags.as_mut() }),
        }
    }

    /// The terminal's overhead, excluding its postings, as `FT.INFO` reports overhead for a
    /// top-level field of the same type: the tree struct for a numeric terminal, the value trie
    /// for a tag terminal. Postings are counted separately, through [`IndexDelta::size_delta`];
    /// summing both never counts a byte twice.
    ///
    /// [`IndexDelta::size_delta`]: crate::IndexDelta::size_delta
    pub const fn mem_usage(&self) -> usize {
        match self.tag() {
            None => size_of::<NumericRangeTree>(),
            Some(tags) => tags.mem_usage(),
        }
    }
}

/// `value` on the heap, owned through the pointer [`Box::into_raw`] returns.
fn owned<T>(value: T) -> NonNull<T> {
    NonNull::new(Box::into_raw(Box::new(value))).expect("a box pointer is never null")
}

impl Drop for Terminal {
    fn drop(&mut self) {
        match self.0 {
            // SAFETY: the pointer came from `Box::into_raw` in `Terminal::new` and is freed once,
            // here.
            Postings::Numeric(tree) => drop(unsafe { Box::from_raw(tree.as_ptr()) }),
            // SAFETY: as above.
            Postings::Tag(tags) => drop(unsafe { Box::from_raw(tags.as_ptr()) }),
        }
    }
}

impl fmt::Debug for Terminal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Postings::Numeric(tree) => {
                // SAFETY: as in `Terminal::numeric`.
                let tree = unsafe { tree.as_ref() };
                f.debug_struct("Numeric")
                    .field("entries", &tree.num_entries())
                    .field("ranges", &tree.num_ranges())
                    .finish()
            }
            Postings::Tag(tags) => {
                // SAFETY: as in `Terminal::tag`.
                let tags = unsafe { tags.as_ref() };
                f.debug_struct("Tag")
                    .field("values", &tags.n_tags())
                    .finish()
            }
        }
    }
}

/// One element's value for the last attribute, the one its terminal stores.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TerminalValue<'a> {
    /// The value for a NUMERIC terminal.
    Numeric(f64),
    /// The value for a TAG terminal, already normalised by the caller.
    Tag(Tag<'a>),
}
