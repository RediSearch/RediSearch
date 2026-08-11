/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Shared machinery for node types that expand a pattern over the spec's tries
//! into a union of per-term readers.
//!
//! The walk itself differs per node type — that lives in the node's own module —
//! but opening a reader for an expanded term, capping the number of expansions,
//! and encoding a rune slice back into the term's stored key are common to all
//! of them.

use std::{marker::PhantomData, ops::ControlFlow, ptr::NonNull};

use field::FieldMaskOrIndex;
use query_term::RSQueryTerm;
use rqe_core::FieldMask;
use rqe_iterators::{build_term_iterator, c2rust::CRQEIterator, interop::RQEIteratorWrapper};
use search_disk::SearchDiskHandle;

use crate::{QueryEvalContext, disk};

/// Open a search-on-disk reader for a single expanded term, wrapping the
/// enterprise disk iterator as a [`CRQEIterator`].
///
/// `num_docs` is the term's document count, used to compute its IDF;
/// `needs_offsets` selects the offset-carrying iterator variant. Returns `None`
/// — after [`disk::new_term_iterator`] records the failure on the query status —
/// when the iterator cannot be built.
fn open_expanded_term_reader_disk(
    ctx: &mut QueryEvalContext,
    disk: SearchDiskHandle,
    term_bytes: &[u8],
    num_docs: usize,
    field_mask: FieldMask,
    weight: f64,
    needs_offsets: bool,
) -> Option<CRQEIterator> {
    let num_documents = ctx.spec().stats.scoring.numDocuments;
    let token_id = ctx.next_token_id() as i32;
    let mut term = RSQueryTerm::new_bytes(term_bytes, token_id, 0);
    term.set_idfs(num_documents, num_docs);

    let it = disk::new_term_iterator(ctx, disk, term, field_mask, weight, needs_offsets)?;
    let ptr = RQEIteratorWrapper::boxed_new(it);
    let nn = NonNull::new(ptr).expect("disk term iterator must not be null");
    // SAFETY: `nn` is a valid, owning C `QueryIterator`.
    Some(unsafe { CRQEIterator::new(nn) })
}

/// An [`ffi::RSToken`] naming a borrowed byte slice, for the C lookups that read
/// only a token's string and length.
///
/// The token holds a raw pointer into the slice it was built from, so the two
/// must not be separated; the lifetime parameter ties them together. The slice
/// need not be NUL-terminated: a consumer that reads the string through the
/// token's length never looks for a terminator.
struct BorrowedToken<'a> {
    tok: ffi::RSToken,
    _bytes: PhantomData<&'a [u8]>,
}

impl<'a> BorrowedToken<'a> {
    /// Build a token naming `bytes`, carrying no flags.
    const fn new(bytes: &'a [u8]) -> Self {
        // SAFETY: `RSToken` is a plain-data struct whose all-zero bit pattern is
        // valid — a null `str` pointer, zero `len`, and zeroed `flags`/`expanded`
        // bitfields.
        let mut tok: ffi::RSToken = unsafe { std::mem::zeroed() };
        tok.str_ = bytes.as_ptr() as *mut std::ffi::c_char;
        tok.len = bytes.len();

        Self {
            tok,
            _bytes: PhantomData,
        }
    }

    /// A pointer to the token, for handing to C. Valid for as long as `self` is,
    /// and read-only: nothing here expects C to write through it.
    const fn as_ptr(&self) -> *const ffi::RSToken {
        std::ptr::from_ref(&self.tok)
    }
}

/// Open a reader for a single expanded term produced by a prefix
/// expansion, wrapping it as a [`CRQEIterator`] so it can join a
/// union of sibling expansions.
///
/// `term_bytes` is the term's key as the index stored it, and is looked up in
/// the spec's inverted index verbatim. Those bytes are not necessarily valid
/// UTF-8: a rune that is a lone surrogate — what truncating a non-BMP codepoint
/// to [`u16`] at index time produces — encodes to its three-byte form, which
/// UTF-8 forbids. They must stay unvalidated; rejecting them would drop exactly
/// the terms such a codepoint creates.
///
/// Every expanded reader is opened with weight `1.0`: the node's own weight is
/// applied once, by the enclosing union, so applying it here too would
/// double-count it.
///
/// Returns `None` when the term has no matching inverted index (absent, empty,
/// or no results in the queried field(s)).
fn open_expanded_term_reader(
    ctx: &mut QueryEvalContext,
    term_bytes: &[u8],
    num_docs: usize,
    field_mask: FieldMask,
    needs_offsets: bool,
) -> Option<CRQEIterator> {
    // See the doc comment: expansion children always carry unit weight.
    const CHILD_WEIGHT: f64 = 1.0;

    // SAFETY: `ctx.spec().diskSpec` is either null or a valid disk index spec
    // that stays valid for the query; `SearchDiskHandle::new` yields `None` for
    // the null (in-memory) case handled below.
    if let Some(disk) = unsafe { SearchDiskHandle::new(ctx.spec().diskSpec) } {
        return open_expanded_term_reader_disk(
            ctx,
            disk,
            term_bytes,
            num_docs,
            field_mask,
            CHILD_WEIGHT,
            needs_offsets,
        );
    }

    // In-memory path.
    // Consume a token id for this expansion up front, before opening the reader:
    // every expanded term is assigned one whether or not it has an inverted
    // index, keeping ids in step with the disk path above, which also consumes
    // one id per expanded term.
    let token_id = ctx.next_token_id() as i32;

    // `Redis_OpenReaderIndex` reads only the token's string and length, so a
    // token borrowing the term's key is all the lookup needs.
    let tok = BorrowedToken::new(term_bytes);

    debug_assert!(
        !ctx.sctx_ptr().is_null(),
        "QueryEvalContext must hold a non-null search context"
    );
    // SAFETY: `ctx.sctx_ptr()` is valid (`QueryEvalContext` invariant 2) and
    // `tok` is a valid, live `RSToken` for the call; `Redis_OpenReaderIndex`
    // only reads the token and does not retain it.
    let idx = unsafe { ffi::Redis_OpenReaderIndex(ctx.sctx_ptr(), tok.as_ptr(), field_mask) };
    let idx = NonNull::new(idx)?;

    // Expanded terms carry no token flags.
    let term = RSQueryTerm::new_bytes(term_bytes, token_id, 0);

    // SAFETY: `ctx.sctx_ptr()` is non-null (`QueryEvalContext` invariant 2,
    // asserted above).
    let sctx = unsafe { NonNull::new_unchecked(ctx.sctx_ptr().cast_mut()) };
    // SAFETY: `idx` is the term's inverted index just opened for this spec and
    // stays valid for the query (`QueryEvalContext` invariants 1/2); `sctx` and
    // its spec are valid; `term` is a freshly heap-allocated query term whose
    // ownership transfers to the iterator.
    let iter = unsafe {
        build_term_iterator(
            idx.as_ptr(),
            sctx,
            FieldMaskOrIndex::Mask(field_mask),
            term,
            CHILD_WEIGHT,
        )
    };
    let ptr = RQEIteratorWrapper::boxed_new(iter);
    let nn = NonNull::new(ptr).expect("term iterator must not be null");
    // SAFETY: `nn` is a valid, owning C `QueryIterator` with all callbacks
    // populated — exactly the precondition of `CRQEIterator::new`.
    Some(unsafe { CRQEIterator::new(nn) })
}

/// Encode a rune slice back into the term's stored key bytes, delegating to the
/// C `runesToStr` so the reconstruction matches how the terms trie encoded the
/// key at index time — byte for byte, including the WTF-8 form a lone surrogate
/// takes.
///
/// Returns `None` when the slice is longer than
/// [`MAX_RUNE_STR_LEN`](ffi::MAX_RUNE_STR_LEN) runes, the point at which the
/// conversion declines and the expansion must be skipped.
pub(crate) fn runes_to_key(runes: &[ffi::rune]) -> Option<Vec<u8>> {
    let mut len: usize = 0;
    // SAFETY: `runes` is a valid slice of `runes.len()` runes; `runesToStr`
    // returns a freshly allocated, NUL-terminated buffer of `len` bytes, or NULL
    // when the slice exceeds `MAX_RUNE_STR_LEN`.
    let ptr = unsafe { ffi::runesToStr(runes.as_ptr(), runes.len(), &mut len) };
    let ptr = NonNull::new(ptr)?;
    // SAFETY: `ptr` points to `len` valid bytes written by the call above.
    let key = unsafe { std::slice::from_raw_parts(ptr.as_ptr().cast::<u8>(), len) }.to_vec();
    // SAFETY: `RedisModule_Free` is set during module init and not mutated
    // afterwards.
    let rm_free = unsafe { redis_module::RedisModule_Free.expect("Redis allocator not available") };
    // SAFETY: `ptr` was allocated by the module allocator inside `runesToStr`.
    unsafe { rm_free(ptr.as_ptr().cast::<std::ffi::c_void>()) };
    Some(key)
}

/// A single pattern expansion in progress: the inputs every walk shares, the
/// context they open readers against, and the readers opened so far.
///
/// Built once per `QN_PREFIX` or `QN_WILDCARD_QUERY` evaluation. The prefix walks
/// consume it and hand back the accumulated readers; the wildcard walks take
/// `&mut self`, since a declined suffix walk falls back to the terms-trie walk
/// with both accumulating into the same `children`.
pub(super) struct Expansion<'a> {
    /// The evaluation context readers are opened against, and where the
    /// expansion-cap warning and the unsupported-fields error are recorded.
    pub(super) ctx: &'a mut QueryEvalContext,
    /// One reader per expanded term, in walk order.
    pub(super) children: Vec<CRQEIterator>,
    /// The node's field mask narrowed to the query-wide one, applied to each
    /// opened reader.
    pub(super) field_mask: FieldMask,
    /// Whether the spec is disk-backed; selects the disk reader and, with it, the
    /// per-term IDF lookup.
    pub(super) is_disk: bool,
    /// Whether expanded disk readers must carry term offsets (disk path only).
    pub(super) needs_offsets: bool,
    /// Upper bound on the number of terms an expansion may open.
    pub(super) max_expansions: usize,
}

impl Expansion<'_> {
    /// Open a reader for `term_bytes` and push it as a union child, unless the
    /// expansion cap has already been reached — in which case the "reached max
    /// prefix expansions" warning is recorded and the trie walk is asked to stop.
    ///
    /// `num_docs` is the term's document count, used only on the disk path for
    /// the IDF.
    pub(crate) fn push_child(&mut self, num_docs: usize, term_bytes: &[u8]) -> ControlFlow<()> {
        if self.cap_reached() {
            return ControlFlow::Break(());
        }
        if let Some(it) = open_expanded_term_reader(
            self.ctx,
            term_bytes,
            num_docs,
            self.field_mask,
            self.needs_offsets,
        ) {
            self.children.push(it);
        }
        ControlFlow::Continue(())
    }

    /// Whether the expansion cap has been reached, recording the "reached max
    /// prefix expansions" warning when it has.
    ///
    /// [`push_child`](Self::push_child) applies this itself, so a walk only needs
    /// to consult it directly when producing the term key costs something — a
    /// walk that keeps calling back after being asked to stop would pay that cost
    /// once per remaining match.
    pub(crate) fn cap_reached(&mut self) -> bool {
        if self.children.len() >= self.max_expansions {
            self.ctx
                .status()
                .warnings_mut()
                .set_reached_max_prefix_expansions();
            true
        } else {
            false
        }
    }
}
