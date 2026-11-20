/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The index of one NESTED field: the routing levels of a [`CompoundIndex`] with a
//! [`Terminal`] holding the postings at the end of each path.
//!
//! [`NestedIndex::index_document`] indexes all the elements of one document at once. It checks
//! every element, and every path together with
//! [`CompoundIndex::check_paths`], before writing anything, so a document is indexed whole or not
//! at all.
//!
//! The design is described in `docs/design/nested-compound-index.md`.

mod error;
mod index;
mod terminal;

pub use compound_index::CompoundIndex;
pub use error::IndexError;
pub use index::{Element, IndexDelta, IndexParams, NestedIndex};
pub use terminal::{FloatCompression, Terminal, TerminalKind, TerminalKindTag, TerminalValue};
