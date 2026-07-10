/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C entry points for [`spellcheck_dictionary::SpellCheckDictionary`].
//!
//! All string parameters are byte pointers with an explicit length and
//! must be valid UTF-8. Passing invalid UTF-8 panics.
//!
//! The dictionary is **not** thread-safe: a mutating call ([`SpellCheckDictionary_Add`],
//! [`SpellCheckDictionary_Remove`], [`SpellCheckDictionary_Free`]) requires
//! exclusive access — no other call on the same dictionary may run
//! concurrently with it, and no iterator obtained from it may be alive.
//! Read-only calls may run concurrently with each other.

#![allow(non_snake_case)]

mod dictionary;
mod iter_cursor;
mod rdb;

pub use dictionary::*;
pub use iter_cursor::*;
pub use rdb::*;

/// A set of spell-check dictionary terms supporting case-insensitive exact
/// and fuzzy (Levenshtein edit distance) lookups.
///
/// Opaque to C; obtained from [`SpellCheckDictionary_New`] and freed with
/// [`SpellCheckDictionary_Free`].
pub use spellcheck_dictionary::SpellCheckDictionary;
