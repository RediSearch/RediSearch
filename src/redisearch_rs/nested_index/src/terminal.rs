/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::fmt;

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
/// A numeric terminal is the [`NumericRangeTree`] a top-level NUMERIC field uses, so the numeric
/// iterators that read those fields read it unchanged. A tag terminal is a [`TagIndex`].
pub enum Terminal {
    /// A numeric range tree over the elements' last attribute, as for a NUMERIC field.
    Numeric(NumericRangeTree),
    /// One doc-id posting list per value of the elements' last attribute, as for a TAG field.
    Tag(TagIndex<InMemoryMode>),
}

impl Terminal {
    /// An empty terminal of the given kind.
    pub fn new(kind: TerminalKind) -> Self {
        match kind {
            TerminalKind::Numeric(compression) => Self::Numeric(NumericRangeTree::new(matches!(
                compression,
                FloatCompression::On
            ))),
            // No suffix trie: NESTED sub-fields reject WITHSUFFIXTRIE.
            TerminalKind::Tag => Self::Tag(TagIndex::<InMemoryMode>::new(false)),
        }
    }

    /// The terminal's overhead, excluding its postings, as `FT.INFO` reports overhead for a
    /// top-level field of the same type: the tree struct for a numeric terminal, the value trie
    /// for a tag terminal. Postings are counted separately, through [`IndexDelta::size_delta`];
    /// summing both never counts a byte twice.
    ///
    /// [`IndexDelta::size_delta`]: crate::IndexDelta::size_delta
    pub const fn mem_usage(&self) -> usize {
        match self {
            Self::Numeric(_) => size_of::<NumericRangeTree>(),
            Self::Tag(tags) => tags.mem_usage(),
        }
    }
}

impl fmt::Debug for Terminal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Numeric(tree) => f
                .debug_struct("Numeric")
                .field("entries", &tree.num_entries())
                .field("ranges", &tree.num_ranges())
                .finish(),
            Self::Tag(tags) => f
                .debug_struct("Tag")
                .field("values", &tags.n_tags())
                .finish(),
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
