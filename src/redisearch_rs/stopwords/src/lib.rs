/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Case-insensitive stopword sets with deterministic byte ordering.

#![warn(
    clippy::unwrap_used,
    clippy::dbg_macro,
    clippy::todo,
    clippy::unimplemented,
    clippy::print_stdout,
    clippy::panic,
    clippy::unnecessary_safety_comment,
    clippy::ptr_as_ptr,
    clippy::borrow_as_ptr,
    clippy::ref_as_ptr,
    clippy::indexing_slicing
)]

use string_utils::unicode::tolower_bytes;
use trie_rs::TrieMap;

/// Maximum number of input words consumed when constructing a stopword list.
pub const MAX_STOPWORDLIST_SIZE: usize = 1024;

/// English stopwords used when an index does not supply its own list.
pub const DEFAULT_STOPWORDS: &[&[u8]] = &[
    b"a", b"is", b"the", b"an", b"and", b"are", b"as", b"at", b"be", b"but", b"by", b"for", b"if",
    b"in", b"into", b"it", b"no", b"not", b"of", b"on", b"or", b"such", b"that", b"their", b"then",
    b"there", b"these", b"they", b"this", b"to", b"was", b"will", b"with",
];

/// An immutable set of normalized stopwords, including non-UTF-8 byte strings.
///
/// Normalization follows [`tolower_bytes`]. Stored and lookup keys use the low
/// sixteen bits of the normalized byte length to preserve existing matching behavior,
/// not because RDB strings or [`TrieMap`] require it. For example, a normalized
/// query of 65,537 bytes starting with `a` matches the stopword `a`.
pub struct StopWordList {
    words: TrieMap<()>,
}

impl StopWordList {
    /// Normalize and deduplicate the first [`MAX_STOPWORDLIST_SIZE`] input words.
    /// Duplicate words count toward the input limit.
    pub fn new<'a>(words: impl IntoIterator<Item = &'a [u8]>) -> Self {
        let words = words.into_iter().take(MAX_STOPWORDLIST_SIZE);
        Self::from_stored(words.map(tolower_bytes))
    }

    /// Restore stored keys without normalizing them again.
    ///
    /// The caller must check the serialized element count against
    /// [`MAX_STOPWORDLIST_SIZE`] before reading the keys.
    pub fn from_stored(words: impl IntoIterator<Item = impl AsRef<[u8]>>) -> Self {
        let mut list = Self {
            words: TrieMap::new(),
        };
        for word in words {
            list.words.insert(stored_key(word.as_ref()), ());
        }
        list
    }

    /// Whether `term` belongs to this set after [normalization](Self).
    #[inline]
    pub fn contains(&self, term: &[u8]) -> bool {
        if self.is_empty() {
            return false;
        }
        self.words.find(stored_key(&tolower_bytes(term))).is_some()
    }

    /// The number of unique stored words.
    pub const fn len(&self) -> usize {
        self.words.n_unique_keys()
    }

    /// Whether no words are stored.
    pub const fn is_empty(&self) -> bool {
        self.words.n_unique_keys() == 0
    }

    /// Yield the exact stored keys in byte lexicographic order.
    pub fn iter(&self) -> impl Iterator<Item = Vec<u8>> + '_ {
        self.words.iter().map(|(word, ())| word)
    }
}

impl Default for StopWordList {
    fn default() -> Self {
        Self::new(DEFAULT_STOPWORDS.iter().copied())
    }
}

fn stored_key(word: &[u8]) -> &[u8] {
    word.split_at(usize::from(word.len() as u16)).0
}
