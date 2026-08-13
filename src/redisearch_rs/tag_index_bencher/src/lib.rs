/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Shared setup helpers for the [`tag_index`] Criterion benches.
//!
//! Every bench runs each configuration twice — once against the [`tag_index`]
//! crate and once against the C `TagIndex` it replaces — so the two arms have to
//! see byte-identical data. That is what [`TagCorpus`] is for: it owns the tags
//! once, as NUL-terminated allocations, and hands out a borrowed view per
//! language ([`TagCorpus::rust_docs`], [`TagCorpus::c_docs`]).
//!
//! Only memory mode is benchmarked. In disk mode both implementations forward to
//! the same `SearchDisk_*` calls, so there is nothing to compare.

use std::{
    collections::HashSet,
    ffi::{CStr, CString, c_char, c_void},
    ptr,
};

use rand::RngExt;
use rqe_core::DocId;
use tag_index::{TagIndex, WritePostingsDelta};

// Force-link the umbrella `redisearch_rs` crate so its `#[used]` symbol table keeps
// the Rust FFI functions the linked C code calls back into — `TrieMap_*`,
// `InvertedIndex_*`, `MetricsVec_New`, which is most of what the C tag index is built
// out of. Without the `extern crate` reference the umbrella rlib is dropped as unused
// and those symbols go undefined at link time.
extern crate redisearch_rs;

// Some of the missing C symbols are actually Rust-provided.
redis_mock::mock_or_stub_missing_redis_c_symbols!();

/// Bytes tags are drawn from. NUL-free, and ordered so that
/// [`TagCorpusInput::alphabet`] can select a prefix of it as the branching
/// factor.
const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";

/// Cap on the terms a wildcard expansion is allowed to yield, mirroring
/// `RSGlobalConfig.iteratorsConfigParams.maxPrefixExpansions`
/// (`DEFAULT_MAX_PREFIX_EXPANSIONS` in `src/config.h`).
///
/// Only the wildcard form is capped during expansion, and both implementations do
/// it there: C inside `_getWildcardArray`, Rust inside
/// [`TagIndex::suffix_expand`]. The suffix and contains forms leave the cap to
/// their caller, so it is deliberately *not* applied to them here — see
/// [`consume_suffix_matches`].
pub const MAX_PREFIX_EXPANSIONS: usize = 200;

/// The field the benches index into. They only ever use one, so any value works
/// as long as both arms agree.
pub const FIELD_INDEX: ffi::t_fieldIndex = 0;

fn random_around_mean<R: RngExt>(mean: usize, absolute_variance: usize, rng: &mut R) -> usize {
    let min = mean.saturating_sub(absolute_variance);
    let max = mean.saturating_add(absolute_variance);

    rng.random_range(min..=max)
}

/// Shape of the tag corpus, which is the shape of the values trie the benched
/// operations walk.
#[derive(Clone, Copy, Debug)]
pub struct TagCorpusInput {
    /// How many distinct tags to generate: the trie's width.
    pub unique_tags: usize,
    /// Mean tag length in bytes: the trie's depth, and the dominant cost of the
    /// suffix-trie work, which inserts one entry per suffix of each tag.
    pub tag_len_mean: usize,
    /// Tag lengths are drawn uniformly from `tag_len_mean ± tag_len_variation`.
    pub tag_len_variation: usize,
    /// Length of the prefix each tag shares with the others drawing the same one
    /// out of [`prefix_pool`](Self::prefix_pool). Zero disables prefix sharing,
    /// which makes the trie shallow and wide at the root.
    pub shared_prefix_depth: usize,
    /// How many distinct shared prefixes exist. Ignored when
    /// [`shared_prefix_depth`](Self::shared_prefix_depth) is zero.
    pub prefix_pool: usize,
    /// How many distinct bytes tags are built from, i.e. the trie's branching
    /// factor. Capped by the length of [`ALPHABET`].
    pub alphabet: usize,
}

/// Which documents to index, and how many tags each carries.
#[derive(Clone, Copy, Debug)]
pub struct DocsInput {
    /// How many documents to generate.
    pub count: usize,
    /// The first document id. Ids increase by one from here, since the inverted
    /// index only accepts ascending ids.
    pub start_doc_id_from: DocId,
    /// Tags per document, drawn uniformly from
    /// `tags_per_doc_mean ± tags_per_doc_variation`.
    pub tags_per_doc_mean: usize,
    /// See [`tags_per_doc_mean`](Self::tags_per_doc_mean).
    pub tags_per_doc_variation: usize,
}

/// One generated document: the tags it carries, as indices into the
/// [`TagCorpus`] that produced it, so neither language arm holds its own copy of
/// the bytes.
#[derive(Clone, Debug)]
pub struct Doc {
    /// The document id to index under.
    pub doc_id: DocId,
    /// Indices into [`TagCorpus::tags`].
    pub tags: Vec<usize>,
}

/// The generated tags, owned once and shared by both language arms.
///
/// Each tag is stored NUL-terminated, which gives the C arm the `const char *`
/// it expects and simultaneously discharges [`TagIndex::index`]'s requirement
/// that every tag borrow from a NUL-terminated buffer.
pub struct TagCorpus {
    tags: Vec<CString>,
}

impl TagCorpus {
    /// Generate a corpus of distinct, NUL-free tags shaped by `input`.
    ///
    /// # Panics
    /// Panics if `input.alphabet` exceeds [`ALPHABET`]'s length, or if the
    /// requested number of distinct tags cannot be drawn from the configured
    /// shape (too small an alphabet for too many tags).
    pub fn generate<R: RngExt>(input: TagCorpusInput, rng: &mut R) -> Self {
        assert!(
            input.alphabet >= 2 && input.alphabet <= ALPHABET.len(),
            "alphabet must be in 2..={}, got {}",
            ALPHABET.len(),
            input.alphabet
        );
        let alphabet = &ALPHABET[..input.alphabet];

        let draw_bytes = |len: usize, rng: &mut R| -> Vec<u8> {
            (0..len)
                .map(|_| alphabet[rng.random_range(0..alphabet.len())])
                .collect()
        };

        let prefixes: Vec<Vec<u8>> = if input.shared_prefix_depth == 0 {
            vec![Vec::new()]
        } else {
            (0..input.prefix_pool.max(1))
                .map(|_| draw_bytes(input.shared_prefix_depth, rng))
                .collect()
        };

        // Retry on collision rather than accepting a short corpus: the benches
        // report `unique_tags` as a parameter, so it has to be the truth.
        let mut seen: HashSet<Vec<u8>> = HashSet::with_capacity(input.unique_tags);
        let mut tags = Vec::with_capacity(input.unique_tags);
        let max_attempts = input.unique_tags.saturating_mul(100).saturating_add(1_000);
        let mut attempts = 0usize;
        while tags.len() < input.unique_tags {
            attempts += 1;
            assert!(
                attempts <= max_attempts,
                "could not draw {} distinct tags from {input:?} after {attempts} attempts; \
                 widen the alphabet or the tag length",
                input.unique_tags,
            );

            let prefix = &prefixes[tags.len() % prefixes.len()];
            // A tag has to be longer than the prefix it shares, or the whole
            // corpus would collapse onto `prefix_pool` distinct values.
            let len = random_around_mean(input.tag_len_mean, input.tag_len_variation, rng)
                .max(prefix.len() + 1);

            let mut tag = prefix.clone();
            tag.extend_from_slice(&draw_bytes(len - prefix.len(), rng));

            if seen.insert(tag.clone()) {
                tags.push(CString::new(tag).expect("tags are drawn from a NUL-free alphabet"));
            }
        }

        Self { tags }
    }

    /// The generated tags, NUL-terminated.
    pub fn tags(&self) -> &[CString] {
        &self.tags
    }

    /// How many distinct tags the corpus holds.
    pub const fn len(&self) -> usize {
        self.tags.len()
    }

    /// Whether the corpus is empty. Only ever true for a zero-sized input.
    pub const fn is_empty(&self) -> bool {
        self.tags.is_empty()
    }

    /// Every tag as the byte slice the Rust arm indexes, terminator excluded —
    /// the same view C gets after `strlen`.
    pub fn rust_tags(&self) -> Vec<&[u8]> {
        self.tags.iter().map(|tag| tag.as_bytes()).collect()
    }

    /// Every tag as the `const char *` the C arm indexes.
    pub fn c_tags(&self) -> Vec<*const c_char> {
        self.tags.iter().map(|tag| tag.as_ptr()).collect()
    }

    /// Generate documents drawing their tags from this corpus.
    pub fn docs<R: RngExt>(&self, input: DocsInput, rng: &mut R) -> Vec<Doc> {
        assert!(!self.is_empty(), "cannot draw tags from an empty corpus");

        (0..input.count)
            .map(|i| {
                let n =
                    random_around_mean(input.tags_per_doc_mean, input.tags_per_doc_variation, rng)
                        .max(1);
                // Tags are drawn independently, so a document can repeat one.
                // That is a real multi-value shape, and both arms see it
                // identically.
                let tags = (0..n).map(|_| rng.random_range(0..self.len())).collect();
                Doc {
                    doc_id: input.start_doc_id_from + i as DocId,
                    tags,
                }
            })
            .collect()
    }

    /// Project `docs` into the per-document tag slices the Rust arm indexes.
    ///
    /// Built once per configuration, outside the timed loop, so the measurement
    /// covers indexing rather than this projection.
    pub fn rust_docs(&self, docs: &[Doc]) -> Vec<(DocId, Vec<&[u8]>)> {
        docs.iter()
            .map(|doc| {
                let tags = doc.tags.iter().map(|&i| self.tags[i].as_bytes()).collect();
                (doc.doc_id, tags)
            })
            .collect()
    }

    /// Project `docs` into the per-document `const char *` arrays the C arm
    /// indexes. The counterpart of [`rust_docs`](Self::rust_docs), pointing at
    /// the same allocations.
    pub fn c_docs(&self, docs: &[Doc]) -> Vec<(DocId, Vec<*const c_char>)> {
        docs.iter()
            .map(|doc| {
                let tags = doc.tags.iter().map(|&i| self.tags[i].as_ptr()).collect();
                (doc.doc_id, tags)
            })
            .collect()
    }

    /// The longest tag in the corpus, the one [`pattern_for`](Self::pattern_for)
    /// carves its patterns out of so there are always enough bytes.
    fn longest_tag(&self) -> &[u8] {
        self.tags
            .iter()
            .map(|tag| tag.as_bytes())
            .max_by_key(|tag| tag.len())
            .expect("corpus is not empty")
    }

    /// Build an affix pattern for `mode` that is guaranteed to match at least one
    /// term, taken out of a tag the corpus actually holds.
    ///
    /// `selectivity` picks the token length: a short token is a prefix of many
    /// suffix keys and so matches many terms, a longer one matches few.
    ///
    /// # Panics
    /// Panics if the corpus' longest tag is too short to carve the pattern from.
    pub fn pattern_for(&self, mode: ExpandMode, selectivity: Selectivity) -> Vec<u8> {
        let tag = self.longest_tag();
        let token_len = selectivity.token_len();
        assert!(
            tag.len() > token_len,
            "longest tag ({} bytes) is too short for a {token_len}-byte token; \
             raise tag_len_mean",
            tag.len(),
        );

        match mode {
            // An exact suffix-trie node key: the last `token_len` bytes of a tag
            // are, by construction, one of the suffixes inserted for it.
            ExpandMode::Suffix => tag[tag.len() - token_len..].to_vec(),
            // Prefix-matched against the suffix keys, so any substring of a tag
            // works: the substring at offset `p` is a prefix of the key for the
            // suffix starting at `p`.
            ExpandMode::Contains => tag[1..1 + token_len].to_vec(),
            // `*token*` gives `choose_token` (and C's `Suffix_ChooseToken`) a
            // literal anchor, so neither arm takes the no-anchor early exit.
            ExpandMode::Wildcard => {
                let mut pattern = vec![b'*'];
                pattern.extend_from_slice(&tag[1..1 + token_len]);
                pattern.push(b'*');
                pattern
            }
        }
    }
}

/// Which affix query [`suffix_expand`](TagIndex::suffix_expand) — and its C
/// counterpart — is benchmarked with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpandMode {
    /// `*foo`: one exact suffix-trie node lookup.
    Suffix,
    /// `*foo*`: a prefix walk over the suffix trie.
    Contains,
    /// A pattern with `*`/`?` metacharacters, anchored on its most selective
    /// literal token.
    Wildcard,
}

impl ExpandMode {
    /// The value used in the benchmark id, so
    /// `.skills/compare-bench-c-vs-rust` can pivot on it.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Suffix => "suffix",
            Self::Contains => "contains",
            Self::Wildcard => "wildcard",
        }
    }
}

/// How much of the corpus an affix pattern should match.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Selectivity {
    /// A long token, matching few terms.
    Few,
    /// A short token, matching many terms.
    Many,
}

impl Selectivity {
    /// Token length in bytes. Short tokens are prefixes of more suffix keys, so
    /// they match more terms.
    pub const fn token_len(self) -> usize {
        match self {
            Self::Few => 5,
            Self::Many => 2,
        }
    }

    /// The value used in the benchmark id.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Few => "few",
            Self::Many => "many",
        }
    }
}

/// Create an empty memory-mode Rust index.
pub fn build_rust(with_suffix: bool) -> TagIndex {
    TagIndex::new_in_memory(0, with_suffix)
}

/// Index `doc_id` under `tags` in a memory-mode Rust index.
///
/// [`TagIndex::index`] is `unsafe` only because of its disk-mode contract, which
/// memory mode cannot violate: `ctx`/`batch` are ignored and the tag bytes are
/// never read past their length. Discharging that here keeps the obligation out
/// of every bench.
pub fn index_rust(idx: &mut TagIndex, tags: &[&[u8]], doc_id: DocId) -> Option<WritePostingsDelta> {
    // SAFETY: memory mode, so neither disk-mode condition applies.
    unsafe { idx.index(ptr::null(), ptr::null(), tags, doc_id, false) }
}

/// Run the post-indexing commit phase for `tags` on a Rust index.
///
/// [`TagIndex::commit`] is `unsafe` because the tags must be NUL-free, which
/// [`TagCorpus::generate`] guarantees by drawing them from [`ALPHABET`].
pub fn commit_rust(idx: &mut TagIndex, tags: &[&[u8]]) -> u32 {
    // SAFETY: as above — every tag is drawn from a NUL-free alphabet.
    unsafe { idx.commit(tags) }
}

/// Populate a fresh Rust index with `docs`, committing each document's tags.
pub fn populate_rust(with_suffix: bool, docs: &[(DocId, Vec<&[u8]>)]) -> TagIndex {
    let mut idx = build_rust(with_suffix);
    for (doc_id, tags) in docs {
        index_rust(&mut idx, tags, *doc_id);
        commit_rust(&mut idx, tags);
    }
    idx
}

/// RAII handle around the C `TagIndex*`. Calls `TagIndex_Free` on drop so the
/// sweep doesn't leak a values trie (and its per-tag posting lists) per
/// iteration.
pub struct CTagIndex(*mut ffi::TagIndex);

impl CTagIndex {
    /// Borrow the raw pointer for passing to other `TagIndex_*` functions. It is
    /// valid for as long as `self` is alive.
    pub const fn as_ptr(&self) -> *mut ffi::TagIndex {
        self.0
    }
}

impl Drop for CTagIndex {
    fn drop(&mut self) {
        // SAFETY: `self.0` came from `NewTagIndex` in `build_c` and is not
        // aliased, so this is the sole owner handing it back.
        unsafe { ffi::TagIndex_Free(self.0) };
    }
}

/// C analogue of [`build_rust`]: an empty memory-mode index.
///
/// Memory mode is what a NULL `diskSpec` selects.
pub fn build_c(with_suffix: bool) -> CTagIndex {
    // SAFETY: a NULL `diskSpec` is the documented way to ask for memory mode,
    // in which the `fieldIndex` argument is unused.
    let idx = unsafe { ffi::NewTagIndex(ptr::null_mut(), FIELD_INDEX, with_suffix) };
    assert!(!idx.is_null(), "NewTagIndex returned NULL");
    CTagIndex(idx)
}

/// C analogue of [`index_rust`].
///
/// `stats` collects the accounting the Rust arm returns as a
/// [`WritePostingsDelta`]; pass a zeroed [`ffi::IndexStats`].
///
/// # Safety
///
/// `values` must hold `NUL`-terminated tag strings that outlive the call, and
/// `stats` must point to a valid [`ffi::IndexStats`].
pub unsafe fn index_c(
    idx: &CTagIndex,
    values: &[*const c_char],
    doc_id: DocId,
    stats: *mut ffi::IndexStats,
) -> bool {
    let index_ctx = ffi::TagIndexIndexCtx {
        batch: ptr::null_mut(),
        values: values.as_ptr().cast_mut(),
        n: values.len(),
        docId: doc_id,
        hasFieldExpiration: false,
        stats,
    };
    // SAFETY: memory mode ignores the `RedisModuleCtx` and the batch; `idx` is
    // live for the borrow, and the caller vouches for `values` and `stats`.
    unsafe { ffi::TagIndex_Index(ptr::null_mut(), idx.as_ptr(), &index_ctx) }
}

/// C analogue of [`commit_rust`].
///
/// # Safety
///
/// Same as [`index_c`]: `values` must hold NUL-terminated tags that outlive the
/// call, and `stats` must be valid.
pub unsafe fn commit_c(idx: &CTagIndex, values: &[*const c_char], stats: *mut ffi::IndexStats) {
    // SAFETY: the caller vouches for `values` and `stats`, and `idx` is live for
    // the borrow.
    unsafe {
        ffi::TagIndex_Commit(
            idx.as_ptr(),
            values.as_ptr().cast_mut(),
            values.len(),
            stats,
        )
    };
}

/// C analogue of [`populate_rust`], over the same documents.
///
/// # Safety
///
/// Every pointer in `docs` must be a NUL-terminated tag outliving the returned
/// index.
pub unsafe fn populate_c(with_suffix: bool, docs: &[(DocId, Vec<*const c_char>)]) -> CTagIndex {
    let idx = build_c(with_suffix);
    let mut stats = zeroed_stats();
    for (doc_id, tags) in docs {
        // SAFETY: the caller's contract is `index_c`'s, and `stats` is a live
        // local.
        unsafe { index_c(&idx, tags, *doc_id, &mut stats) };
        // SAFETY: as above, for `commit_c`.
        unsafe { commit_c(&idx, tags, &mut stats) };
    }
    idx
}

/// A zeroed [`ffi::IndexStats`], the accumulator the C write path needs and the
/// Rust one returns instead.
pub const fn zeroed_stats() -> ffi::IndexStats {
    // SAFETY: `IndexStats` is a plain-old-data struct of counters, for which the
    // all-zero bit pattern is the valid "nothing counted yet" state.
    unsafe { std::mem::zeroed() }
}

/// Walk every term in the `arrayof(char **)` that `TagIndex_GetSuffixMatches`
/// returns — `strlen` included, as `src/query.c` does — and free the outer array.
/// Returns how many terms were visited.
///
/// The inner arrays are borrowed from the suffix trie, so only the outer array is
/// freed. Consuming the terms is what makes the comparison fair: the Rust arm
/// yields them lazily, so an unconsumed iterator would measure nothing.
///
/// No cap is applied, deliberately. `GetList_SuffixTrieMap` walks the whole
/// matching subtree before returning, so capping only the enumeration would leave
/// C paying for a full walk while Rust's lazy iterator stopped at the cap — a gap
/// of two orders of magnitude on a broad pattern, measuring laziness rather than
/// the per-term cost this bench is after. Production does cap, at
/// [`MAX_PREFIX_EXPANSIONS`], and there the Rust port can stop the walk early
/// where C cannot; that structural advantage is real and is simply not what these
/// numbers describe.
///
/// # Safety
///
/// `arr` must be the (possibly NULL) array `TagIndex_GetSuffixMatches` returned,
/// not yet freed, and the suffix trie its inner arrays borrow from must still be
/// alive.
pub unsafe fn consume_suffix_matches(arr: *mut *mut *mut c_char) -> usize {
    if arr.is_null() {
        return 0;
    }

    let mut visited = 0usize;
    // SAFETY: the caller vouches for `arr` being a live `arrayof`.
    let outer = unsafe { ffi::array_len_func(arr.cast::<c_void>()) } as usize;
    for i in 0..outer {
        // SAFETY: `i < outer`, the array's own length, so the offset stays in
        // bounds.
        let slot = unsafe { arr.add(i) };
        // SAFETY: `slot` addresses an initialized element of the outer array.
        let inner_arr = unsafe { *slot };
        // SAFETY: every element of the outer array is itself a live `arrayof`,
        // borrowed from the suffix trie the caller keeps alive.
        let inner = unsafe { ffi::array_len_func(inner_arr.cast::<c_void>()) } as usize;
        for j in 0..inner {
            // SAFETY: `j < inner`, the inner array's own length.
            let slot = unsafe { inner_arr.add(j) };
            // SAFETY: `slot` addresses an initialized element of the inner array.
            let term = unsafe { *slot };
            // SAFETY: the suffix trie stores NUL-terminated term copies, which
            // is what `src/query.c` relies on when it calls `strlen` here.
            let len = unsafe { CStr::from_ptr(term) }.count_bytes();
            std::hint::black_box(len);
            visited += 1;
        }
    }

    // SAFETY: `arr` is the outer array and is not freed again; the inner arrays
    // belong to the suffix trie and are deliberately left alone.
    unsafe { ffi::array_free(arr.cast::<c_void>()) };

    visited
}

/// Walk the flat `arrayof(char *)` that `TagIndex_GetSuffixWildcardMatches`
/// returns and free it. Returns how many terms were visited.
///
/// No cap is applied here either, but for a different reason than in
/// [`consume_suffix_matches`]: the wildcard form is capped *during* expansion by
/// both implementations — C inside `_getWildcardArray`, Rust inside
/// [`TagIndex::suffix_expand`] — so the array is already bounded on arrival.
///
/// # Safety
///
/// `arr` must be the array `TagIndex_GetSuffixWildcardMatches` returned — never
/// the `BAD_POINTER` sentinel — not yet freed, with its suffix trie still alive.
pub unsafe fn consume_wildcard_matches(arr: *mut *mut c_char) -> usize {
    if arr.is_null() {
        return 0;
    }

    // SAFETY: the caller vouches for `arr` being a live `arrayof`.
    let len = unsafe { ffi::array_len_func(arr.cast::<c_void>()) } as usize;
    for i in 0..len {
        // SAFETY: `i < len`, the array's own length, so the offset stays in
        // bounds.
        let slot = unsafe { arr.add(i) };
        // SAFETY: `slot` addresses an initialized element of the array.
        let term = unsafe { *slot };
        // SAFETY: the suffix trie stores NUL-terminated term copies.
        let bytes = unsafe { CStr::from_ptr(term) }.count_bytes();
        std::hint::black_box(bytes);
    }

    // SAFETY: `arr` is owned by the caller of the C function and freed once.
    unsafe { ffi::array_free(arr.cast::<c_void>()) };

    len
}

/// The `BAD_POINTER` sentinel `GetList_SuffixTrieMap_Wildcard` returns for a
/// pattern with no literal token to anchor on — the C counterpart of
/// [`tag_index::NoAnchorToken`].
///
/// Benched patterns always carry an anchor, so this only ever appears in an
/// assertion.
pub const BAD_POINTER: usize = 0xBAAA_AAAD;

/// An all-zero `timespec`, which every affix call here pairs with
/// `skipTimeoutChecks = true` so the deadline-probe cadence stays out of the
/// measurement.
pub const NO_TIMEOUT: ffi::timespec = ffi::timespec {
    tv_sec: 0,
    tv_nsec: 0,
};
