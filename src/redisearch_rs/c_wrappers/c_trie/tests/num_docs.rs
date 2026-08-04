/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for [`CTrieRef::num_docs`].
//!
//! Each test builds a live C trie through the linked static library, inserts
//! terms the way the indexer does, and looks their document count back up.

// Links the Rust-provided and C-provided symbols of the whole module.
extern crate redisearch_rs;
// Provides the Redis allocator (and stubs) the C trie code relies on.
redis_mock::mock_or_stub_missing_redis_c_symbols!();

use std::{
    ffi::{c_char, c_void},
    ptr,
};

use c_trie::CTrieRef;

/// A term to index: its bytes, the byte that followed it in the buffer
/// insertion decoded from, and the document count to store it under.
///
/// The trailing byte matters because the decode reads past a term whose last
/// multibyte sequence is cut short by its own end, and what it finds there
/// becomes part of the stored key. The indexer supplies a NUL only when the
/// tokenizer copied the token; otherwise it is the separator that ended the
/// token.
struct Term {
    bytes: &'static [u8],
    next_byte: u8,
    num_docs: usize,
}

/// Build a terms trie holding `terms`, run `f`, then free it.
///
/// Each term is laid out as its own bytes, then `next_byte`, then enough zeros
/// that the decode's over-read — up to three bytes past the length it is given —
/// always lands inside the allocation.
fn with_terms_trie(terms: &[Term], f: impl FnOnce(&CTrieRef)) {
    // SAFETY: `NewTrie` returns a fresh, valid, empty terms trie; a terms trie
    // stores no payload, so a null free callback is correct.
    let ptr = unsafe { ffi::NewTrie(None, ffi::TrieSortMode_Trie_Sort_Lex) };
    assert!(!ptr.is_null(), "NewTrie returned null");
    for term in terms {
        let mut buf = term.bytes.to_vec();
        buf.push(term.next_byte);
        buf.resize(term.bytes.len() + 3, 0);
        // SAFETY: `ptr` is the live trie just created; `buf` holds the term's
        // `bytes.len()` bytes followed by padding covering the over-read; a null
        // payload is accepted.
        unsafe {
            ffi::Trie_InsertStringBuffer(
                ptr,
                buf.as_ptr().cast::<c_char>(),
                term.bytes.len(),
                1.0,
                0,
                ptr::null_mut(),
                term.num_docs,
            );
        }
    }
    // SAFETY: `ptr` is a valid trie that stays alive for the whole closure and is
    // not freed until after it returns.
    let trie = unsafe { CTrieRef::from_raw(ptr) };
    f(&trie);
    // SAFETY: `ptr` was produced by `NewTrie` and is freed exactly once here,
    // after the last use of `trie`.
    unsafe { ffi::TrieType_Free(ptr.cast::<c_void>()) };
}

/// A plain ASCII term, as the indexer stores one, ending at a separator.
const fn ascii(bytes: &'static [u8], num_docs: usize) -> Term {
    Term {
        bytes,
        next_byte: b' ',
        num_docs,
    }
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn reports_the_stored_count() {
    with_terms_trie(&[ascii(b"apple", 5), ascii(b"apricot", 7)], |trie| {
        assert_eq!(trie.num_docs(b"apple"), 5);
        assert_eq!(trie.num_docs(b"apricot"), 7);
        assert_eq!(trie.num_docs(b"pear"), 0, "absent term");
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn finds_a_key_that_is_not_valid_utf8() {
    // `ED A0 80` is rune `0xD800`, a lone surrogate — what truncating a non-BMP
    // codepoint to a rune at index time produces. It is a *complete* three-byte
    // sequence, so nothing is read past the term and the key round-trips exactly;
    // validating it as UTF-8 would report zero documents for precisely the terms
    // such a codepoint creates, inflating their IDF.
    const SURROGATE: &[u8] = b"a\xED\xA0\x80b";
    with_terms_trie(&[ascii(b"plain", 1), ascii(SURROGATE, 2)], |trie| {
        assert_eq!(trie.num_docs(b"plain"), 1);
        assert_eq!(trie.num_docs(SURROGATE), 2);
    });
}

/// A three-byte lead with only two bytes left before the term ends, so the
/// decode consumes the byte that follows the term.
const TRUNCATED: &[u8] = b"a\xE6\x97";

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn finds_a_truncated_key_stored_from_a_copied_token() {
    // The tokenizer copies a token whenever normalization allocated, and the copy
    // is NUL-terminated — so insertion decoded `E6 97 00`. The lookup pads with
    // zeros and reaches the same rune.
    with_terms_trie(
        &[
            ascii(b"plain", 1),
            Term {
                bytes: TRUNCATED,
                next_byte: 0,
                num_docs: 2,
            },
        ],
        |trie| {
            assert_eq!(trie.num_docs(b"plain"), 1);
            assert_eq!(trie.num_docs(TRUNCATED), 2);
        },
    );
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn misses_a_truncated_key_stored_from_an_uncopied_token() {
    // The limit, pinned so it is not mistaken for covered. On the path where the
    // tokenizer does not copy, the token points into the document buffer and the
    // byte after it is the separator that ended it — so insertion decoded
    // `E6 97 20` and stored a different rune than any byte-only lookup can name.
    // The stored key genuinely depends on a byte outside the term.
    with_terms_trie(
        &[
            ascii(b"plain", 1),
            Term {
                bytes: TRUNCATED,
                next_byte: b' ',
                num_docs: 2,
            },
        ],
        |trie| {
            assert_eq!(trie.num_docs(b"plain"), 1);
            assert_eq!(
                trie.num_docs(TRUNCATED),
                0,
                "not reachable by its own bytes"
            );
        },
    );
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn reports_zero_for_a_truncated_key_that_was_never_stored() {
    with_terms_trie(&[ascii(b"apple", 5)], |trie| {
        assert_eq!(trie.num_docs(b"apple\xC3"), 0);
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn empty_term_is_absent() {
    with_terms_trie(&[ascii(b"apple", 5)], |trie| {
        assert_eq!(trie.num_docs(b""), 0);
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn reports_zero_for_a_lead_byte_that_swallows_the_last_byte() {
    // `E6` promises three bytes from index 0, so the decode consumes the trailing
    // `a` as a continuation and runs one past a two-byte term — without ever
    // landing on that last byte. Judging the tail by its final byte alone would
    // call this safe to decode in place and read out of bounds.
    with_terms_trie(&[ascii(b"apple", 5)], |trie| {
        assert_eq!(trie.num_docs(b"\xE6a"), 0);
        assert_eq!(trie.num_docs(b"x\xF0ab"), 0);
    });
}
