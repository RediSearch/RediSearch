/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A byte-string set supporting substring and ends-with queries against its
//! members, backing the `WITHSUFFIXTRIE` option on TAG fields.
//!
//! This is the Rust counterpart of the `TrieMap`-flavored suffix structure in
//! `src/suffix.c` (`addSuffixTrieMap`, `deleteSuffixTrieMap`,
//! `GetList_SuffixTrieMap`). Members are raw byte strings: tag values are
//! already normalized upstream (case folding, separator trimming), so this
//! structure applies no normalization of its own, and suffixes are taken at
//! every *byte* offset — exactly like the C `TrieMap` variant, and unlike the
//! rune-based `Trie` variant used for TEXT fields.
//!
//! # Why a suffix trie answers substring queries
//!
//! Every substring of a string is a prefix of some suffix of that string. So
//! indexing all suffixes of every member and prefix-searching for the needle
//! surfaces every member containing it. [`TagSuffixIndex::iter_contains`] is
//! the prefix lookup; [`TagSuffixIndex::iter_suffix`] is the exact lookup.
//!
//! # Memory model
//!
//! For a member of `N` bytes, `N` trie entries reference it (the member's own
//! entry plus its `N - 1` proper suffixes). Like the C implementation, one
//! heap copy of the term is made and every entry's back-reference array holds
//! a weak pointer to it ([`TermPtr`], 8 bytes); the member's own entry
//! additionally holds the owning handle ([`OwnedTerm`], C's non-`NULL`
//! `term` field), and [`TagSuffixIndex::remove`] strips all back-references
//! before freeing the copy. [`ThinVec`] keeps the back-reference array at
//! 8 bytes inline, so [`SuffixData`] is 16 bytes, matching C's `suffixData`.
//!
//! The term's length is stored as a [2-byte prefix](TERM_LEN_PREFIX) inside
//! the allocation itself, so a back-reference stays a thin 8-byte pointer and
//! resolving a match costs a single dependent load, as in C. An earlier
//! design kept the memory fully safe by storing terms in an arena and using
//! 4-byte ids as back-references, but resolving an id costs an extra random
//! load through the arena table, which benchmarked at ~2x the per-match cost
//! of the C implementation on exact-suffix gets.
//!
//! # Safety invariant
//!
//! All `unsafe` in this module rests on one invariant, the same one the C
//! code relies on: *a [`TermPtr`] stored in some entry's back-reference array
//! always points into an allocation owned by the [`OwnedTerm`] of the
//! member's own entry, and every back-reference is removed before that owner
//! is dropped.* [`TagSuffixIndex::add`] establishes it, [`TagSuffixIndex::remove`]
//! preserves it by walking every suffix of the removed member (dropping the
//! owner only afterwards), and dropping the whole index tears everything
//! down together.
#![allow(rustdoc::private_intra_doc_links)]

use std::ptr::NonNull;

use thin_vec::{AlignedU32, ThinVec};
use trie_rs::TrieMap;

/// Weak, thin (8-byte) pointer to a term allocation. The pointee is owned by
/// the [`OwnedTerm`] of the member's own trie entry; see the
/// [safety invariant](self#safety-invariant). Compared by address: every
/// member has exactly one allocation, so pointer equality is term identity
/// (C compares the bytes instead; the identity is the same).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TermPtr(NonNull<u8>);

/// Owning handle to a term allocation; the Rust counterpart of C's
/// `suffixData::term` field. Dropping it frees the allocation — the
/// [safety invariant](self#safety-invariant) demands all [`TermPtr`]
/// back-references are gone by then.
#[derive(Debug)]
struct OwnedTerm(NonNull<u8>);

/// Payload of one trie entry.
///
/// A key `K` can play two independent roles at once: `K` may itself be a
/// member of the set, and `K` may be a suffix (proper or not) of one or more
/// members. Mirrors C's `suffixData { char *term; arrayof(char *) array; }`
/// at the same 16-byte size — see the [memory model](self#memory-model).
#[derive(Debug, Default)]
struct SuffixData {
    /// `Some` iff this entry's key is itself a member: the owning handle of
    /// that member's term allocation. The C equivalent is a non-`NULL`
    /// `term` marking ownership of the heap copy.
    full_term: Option<OwnedTerm>,
    /// Every member this entry's key is a suffix of — including the key
    /// itself when [`Self::full_term`] is set, exactly like C's `array`.
    /// The 32-bit capacity type shrinks the heap header to 8 bytes — the
    /// same as C's `array_hdr_t` — and still bounds an entry's
    /// back-references far above the number of representable members.
    /// [`AlignedU32`] rather than `u32`: its empty-header singleton is
    /// 8-byte aligned, letting `ThinVec` elide the empty-singleton
    /// alignment guard for the 8-byte-aligned [`TermPtr`] element type —
    /// the capacity type the crate intends for such elements. Same header
    /// bytes and heap layout as `u32`; benchmarked
    /// performance-indistinguishable from it.
    refs: ThinVec<TermPtr, AlignedU32>,
}

/// See the [module documentation](self) for the data structure and its
/// C counterpart.
#[derive(Debug, Default)]
pub struct TagSuffixIndex {
    entries: TrieMap<SuffixData>,
    /// Number of member terms.
    n_terms: usize,
    /// Total bytes of live term allocations, for [`Self::mem_usage`].
    term_bytes: usize,
}

/// Iterator over the member terms recorded in one entry's back-reference
/// array; returned by [`TagSuffixIndex::iter_suffix`]. A plain slice walk
/// resolving each thin pointer, so a match costs one dependent load — the
/// same access pattern as iterating C's `char *` arrays.
pub struct Matches<'idx> {
    refs: std::slice::Iter<'idx, TermPtr>,
}
