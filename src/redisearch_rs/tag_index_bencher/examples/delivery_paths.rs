/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Where the exact-suffix loss actually goes, layer by layer — and what removing
//! the per-term `strlen` would buy.
//!
//! `examples/suffix_exact_c_wins.rs` establishes that C wins `*foo` because it
//! borrows the node's pre-built `char **` instead of delivering terms, leaving the
//! port paying a `strlen` both arms share plus a few nanoseconds per term of its
//! own. This example splits those few nanoseconds by rebuilding the port's
//! delivery pipeline one layer at a time over the *same* terms, so each layer is
//! priced on its own.
//!
//! The last two rows price the candidate fix. `src/query.c` needs the term length
//! — it calls `strlen` and hands the result to `TagIndex_OpenReader` — so the
//! length is real work someone must do. C has no choice but to recompute it,
//! because a `char **` carries no lengths. The port owns its term allocations and
//! could *store* the length, which is what row 7 measures: terms laid out as
//! `[u32 len][bytes][NUL]` with the pointer aimed at `bytes`, so the length load
//! shares a cache line with the bytes it describes.
//!
//! # The gate currently fails, and that is the headline
//!
//! Row 5 lands **31-37% below row 0**, so this reconstruction does *not* model the
//! real iterator: something in the port's delivery costs more than an equivalent
//! pipeline written here, and the residual grows with term length (~1.3 ns/term at
//! `tag_len=8`, ~2.2 at 48). The per-layer deltas below are therefore a *lower
//! bound* on each layer, not an attribution of the real cost — read them as "at
//! least this much", and read the profile for the real split.
//!
//! A `samply` profile of `suffix_exact_c_wins --profile rust` gives that split, and
//! it is not what a first pass at the numbers suggests: ~60% of the arm sits in
//! `FlatMap::next` (which is where `chain`, `map` and the slice rebuild all inline
//! to, so it *is* the delivery pipeline), ~11% in the consuming loop and its
//! `dyn next` call site, ~14% in `strlen`, ~4% in the per-call `Box` allocation,
//! and 0.2% in `TrieMap_Find`. So the trie descent is irrelevant, the `strlen` is a
//! seventh of the cost rather than the bulk of it, and the pipeline itself is the
//! thing to attack. Row 7 removes the `strlen` and nothing else, which is why it
//! flatters the fix: it is priced against this file's cheaper pipeline, not against
//! the real one.
//!
//! # Reading the rows honestly
//!
//! Two controls keep the last row from flattering itself further:
//!
//! - **Row 0 is a validation gate.** It calls the real
//!   [`TagIndex::suffix_expand`], and row 5 is supposed to be a faithful
//!   reconstruction of it. If the two disagree by more than a few percent, the
//!   ladder is not modelling the real iterator and rows 1-7 say nothing about it.
//! - **Row 6 is a locality control.** Rows 1-5 walk terms the suffix trie
//!   allocated during a 100k-tag commit, scattered across the heap; the copies
//!   rows 6 and 7 walk were allocated back to back and are therefore friendlier to
//!   the prefetcher. Row 6 is row 5 over the copies with the `strlen` *kept*, so
//!   `row 6 - row 7` isolates the length-recovery change with locality held
//!   constant, and `row 5 - row 6` exposes how much of the apparent win is really
//!   just the copies being contiguous.
//!
//! # Usage
//!
//! ```sh
//! cargo run --release --manifest-path src/redisearch_rs/Cargo.toml \
//!     -p tag_index_bencher --example delivery_paths
//! ```
//!
//! Build with `--release`, for the reason given in the sibling examples.

use std::{
    alloc::{Layout, alloc, dealloc},
    ffi::CStr,
    hint::black_box,
    slice,
    time::Duration,
};

use tag_index::SuffixQuery;
use tag_index_bencher::{ExpandMode, Selectivity, SuffixFixture, ns_per_call};

/// Same seed as the benches, so this dissects the tries they measured.
const SEED: u64 = 42;

/// Per-row measurement window. Every row here runs in tens to hundreds of
/// nanoseconds, so the rows are uniform enough to share one process — unlike the
/// two query forms in `examples/exact_node_vs_prefix_walk.rs`.
const MEASURE_FOR: Duration = Duration::from_millis(300);

/// Both term lengths, since whether the fix is length-independent is half of what
/// this example is for.
const CONFIGS: &[(usize, usize)] = &[(100_000, 8), (100_000, 48)];

/// Width of the length prefix row 7 reads. A `u32` covers any tag length the index
/// accepts, and keeps the payload 4-byte aligned so the load is aligned.
const PREFIX: usize = size_of::<u32>();

/// How far row 5 may drift from row 0 before the reconstruction is not credible.
const GATE_TOLERANCE: f64 = 0.08;

/// Copies of the node's terms, each laid out as `[u32 len][bytes][NUL]` with the
/// stored pointer aimed at `bytes` — the layout the fix would give
/// `OwnedTerm`/`TermPtr`.
///
/// The stored length is the *allocation* size (bytes plus terminator), matching
/// what `TermPtr::alloc_size` returns today, so the slice a row builds from it is
/// byte-for-byte what the real `materialize` builds.
struct PrefixedCopies {
    /// Payload pointers, in the order of the terms they copy.
    ptrs: Vec<*const u8>,
}

impl PrefixedCopies {
    /// One allocation per term, deliberately: the suffix trie allocates its terms
    /// individually too, and a single arena would hand the fix a locality
    /// advantage the real layout could never deliver.
    fn new(terms: &[&[u8]]) -> Self {
        let ptrs = terms
            .iter()
            .map(|with_nul| {
                let layout = layout_for(with_nul.len());

                // SAFETY: `layout` has non-zero size — it is at least `PREFIX`.
                let base = unsafe { alloc(layout) };
                assert!(!base.is_null(), "allocating a term copy");

                // SAFETY: `base` is `u32`-aligned per `layout` and the allocation
                // opens with `PREFIX` bytes reserved for exactly this write.
                unsafe { base.cast::<u32>().write(with_nul.len() as u32) };

                // SAFETY: the allocation holds `PREFIX + with_nul.len()` bytes, so
                // this offset and the copy below stay in bounds.
                let payload = unsafe { base.add(PREFIX) };
                // SAFETY: source and destination are valid for `with_nul.len()`
                // bytes and cannot overlap — `payload` is freshly allocated.
                unsafe {
                    std::ptr::copy_nonoverlapping(with_nul.as_ptr(), payload, with_nul.len())
                };

                payload.cast_const()
            })
            .collect();

        Self { ptrs }
    }
}

impl Drop for PrefixedCopies {
    fn drop(&mut self) {
        for &payload in &self.ptrs {
            // SAFETY: every pointer came from `new`, which offset it by `PREFIX`
            // from the allocation base.
            let base = unsafe { payload.sub(PREFIX) }.cast_mut();
            // SAFETY: `base` opens with the `u32` length `new` wrote there.
            let len = unsafe { base.cast::<u32>().read() } as usize;
            // SAFETY: `base` came from `alloc` with exactly this layout, and this
            // is the only deallocation.
            unsafe { dealloc(base, layout_for(len)) };
        }
    }
}

/// Layout of a prefixed copy holding `with_nul_len` payload bytes.
fn layout_for(with_nul_len: usize) -> Layout {
    Layout::from_size_align(PREFIX + with_nul_len, align_of::<u32>())
        .expect("a term copy is far below Layout's size limit")
}

/// Recover a term's length the way the port does today: a `strlen` behind
/// `CStr::from_ptr`.
///
/// # Safety
/// `p` must point at a live, NUL-terminated term.
///
/// `const`, as `TermPtr::alloc_size` is, so this reconstruction is compiled the
/// same way the port's length recovery is.
const unsafe fn materialize_strlen<'a>(p: *const u8) -> &'a [u8] {
    // SAFETY: the caller vouches for `p` being NUL-terminated.
    let len = unsafe { CStr::from_ptr(p.cast()) }
        .to_bytes_with_nul()
        .len();
    // SAFETY: as above — the allocation holds `len` initialized bytes.
    unsafe { slice::from_raw_parts(p, len) }
}

/// Recover a term's length from a `u32` stored immediately before it.
///
/// # Safety
/// `p` must be a [`PrefixedCopies`] payload pointer.
const unsafe fn materialize_stored<'a>(p: *const u8) -> &'a [u8] {
    // SAFETY: the caller vouches for `p` carrying a length prefix, so the four
    // bytes below it are in bounds of the same allocation.
    let prefix = unsafe { p.sub(PREFIX) }.cast::<u32>();
    // SAFETY: `prefix` is `u32`-aligned and initialized by `PrefixedCopies::new`.
    let len = unsafe { prefix.read() } as usize;
    // SAFETY: as above — the payload holds `len` initialized bytes.
    unsafe { slice::from_raw_parts(p, len) }
}

/// The port's delivery shape, reconstructed: the `Option` its node lookup yields,
/// `flat_map`ped over a `chain` of the node's own term and its refs.
///
/// `full_term` is `None` for the node these rows walk — its key is a 2-byte
/// suffix, not a whole tag, so every member arrives through `refs` — but the
/// `chain` still pays its per-item branch, which is the point of including it.
/// `materialize` is `Copy` for the same reason the port's is: it captures nothing,
/// so it can be moved into the `flat_map` closure rather than borrowed by it.
fn like_port<'a, F>(refs: &'a [*const u8], materialize: F) -> impl Iterator<Item = &'a [u8]> + 'a
where
    F: Fn(*const u8) -> &'a [u8] + Copy + 'a,
{
    let full: Option<*const u8> = None;

    Some(refs).into_iter().flat_map(move |refs| {
        full.into_iter()
            .chain(refs.iter().copied())
            .map(materialize)
    })
}

/// [`like_port`] behind a trait object, the way [`TagIndex::suffix_expand`] hands
/// its iterator out.
///
/// `#[inline(never)]` is load-bearing. Boxing a concrete iterator and consuming it
/// in the same function lets LLVM see the concrete type through the `Box<dyn ..>`,
/// devirtualize every `next` call and inline the whole pipeline into the consuming
/// loop — which measured a *free* trait object and made this ladder miss 40% of the
/// real cost. The port's iterator is built in `tag_index` and consumed in another
/// crate, so its caller really does see nothing but the vtable. Denying the inline
/// restores that.
#[inline(never)]
fn boxed_like_port<'a, F>(
    refs: &'a [*const u8],
    materialize: F,
) -> Box<dyn Iterator<Item = &'a [u8]> + 'a>
where
    F: Fn(*const u8) -> &'a [u8] + Copy + 'a,
{
    Box::new(like_port(refs, materialize))
}

/// Drive an iterator to exhaustion exactly as the bench arm does.
fn drain<'a>(it: impl Iterator<Item = &'a [u8]>) -> usize {
    let mut visited = 0;
    for term in it {
        black_box(term);
        visited += 1;
    }
    visited
}

fn main() {
    for &(unique_tags, tag_len) in CONFIGS {
        ladder(unique_tags, tag_len);
    }
}

fn ladder(unique_tags: usize, tag_len: usize) {
    let fixture = SuffixFixture::new(unique_tags, tag_len, SEED);
    let pattern = fixture.pattern(ExpandMode::Suffix, Selectivity::Many);
    let query = SuffixQuery::Suffix(&pattern);

    let terms = fixture.rust_terms(query);
    let n = terms.len();
    assert!(n > 0, "the pattern is carved out of a tag the corpus holds");

    // The trie-owned terms, as the bare pointers every row below walks.
    let ptrs: Vec<*const u8> = terms.iter().map(|t| t.as_ptr()).collect();
    let copies = PrefixedCopies::new(&terms);

    println!("\n=== unique_tags={unique_tags} / tag_len={tag_len} / matches=many ===");
    println!("{n} terms, mean length {:.1} bytes", mean_len(&terms));

    // Row 0: the real thing, for the gate.
    let real = ns_per_call(MEASURE_FOR, || fixture.rust_expand_and_walk(query));

    let row1 = ns_per_call(MEASURE_FOR, || {
        for &p in &ptrs {
            black_box(p);
        }
        ptrs.len()
    });

    let row2 = ns_per_call(MEASURE_FOR, || {
        for &p in &ptrs {
            // SAFETY: `p` points at a live, NUL-terminated trie-owned term.
            let len = unsafe { CStr::from_ptr(p.cast()) }
                .to_bytes_with_nul()
                .len();
            black_box(len);
        }
        ptrs.len()
    });

    let row3 = ns_per_call(MEASURE_FOR, || {
        for &p in &ptrs {
            // SAFETY: as above.
            let term = unsafe { materialize_strlen(p) };
            black_box(term);
        }
        ptrs.len()
    });

    let row4 = ns_per_call(MEASURE_FOR, || {
        drain(like_port(&ptrs, |p| {
            // SAFETY: `p` is a live, NUL-terminated term owned by the suffix trie
            // in `fixture`, which outlives the measurement.
            unsafe { materialize_strlen(p) }
        }))
    });

    let row5 = ns_per_call(MEASURE_FOR, || {
        drain(boxed_like_port(&ptrs, |p| {
            // SAFETY: as in row 4 — same pointers, same trie.
            unsafe { materialize_strlen(p) }
        }))
    });

    let row6 = ns_per_call(MEASURE_FOR, || {
        drain(boxed_like_port(&copies.ptrs, |p| {
            // SAFETY: `copies` holds NUL-terminated payloads and outlives the
            // measurement, so the `strlen` path applies to them too.
            unsafe { materialize_strlen(p) }
        }))
    });

    let row7 = ns_per_call(MEASURE_FOR, || {
        drain(boxed_like_port(&copies.ptrs, |p| {
            // SAFETY: every pointer in `copies.ptrs` carries the `u32` length
            // prefix `PrefixedCopies::new` wrote.
            unsafe { materialize_stored(p) }
        }))
    });

    let per_term = |ns: f64| ns / n as f64;
    println!("\n  {:<44} {:>10} {:>10}", "row", "ns/call", "ns/term");
    for (label, ns) in [
        ("0  real suffix_expand  [gate]", real),
        ("1  pointer walk", row1),
        ("2  + strlen", row2),
        ("3  + slice build  [= materialize]", row3),
        ("4  + chain/flat_map  [= members]", row4),
        ("5  + Box<dyn Iterator>  [= the port]", row5),
        ("6  row 5 over copies, strlen kept  [control]", row6),
        ("7  row 6 with a stored u32 length  [the fix]", row7),
    ] {
        println!("  {label:<44} {ns:>10.1} {:>10.2}", per_term(ns));
    }

    // The gate. Rows 1-7 only mean something if row 5 reproduces row 0.
    let drift = (row5 - real).abs() / real;
    if drift > GATE_TOLERANCE {
        println!(
            "\n  GATE FAILED: row 5 is {:+.1}% off row 0 (tolerance {:.0}%).\n  \
             This reconstruction is cheaper than the port's real pipeline, so the\n  \
             layer costs below are a lower bound per layer, not an attribution.\n  \
             Profile the real arm instead:\n    \
             samply record src/redisearch_rs/target/release/examples/\\\n      \
             suffix_exact_c_wins --profile rust 20",
            (row5 - real) / real * 100.0,
            GATE_TOLERANCE * 100.0,
        );
    } else {
        println!(
            "\n  gate ok: row 5 is {:+.1}% off row 0",
            (row5 - real) / real * 100.0
        );
    }

    println!("\n  layer costs, ns/term:");
    for (label, delta) in [
        ("strlen", row2 - row1),
        ("slice build", row3 - row2),
        ("chain + flat_map", row4 - row3),
        ("Box<dyn> dispatch", row5 - row4),
    ] {
        println!("    {label:<20} {:>6.2}", per_term(delta));
    }

    // Locality first, then the change itself, so neither absorbs the other.
    let locality = per_term(row5 - row6);
    let fix = per_term(row6 - row7);
    println!(
        "\n  copies are {locality:>6.2} ns/term cheaper than the trie's own terms (locality)\n  \
         stored length saves {fix:>6.2} ns/term with locality held constant"
    );

    // Project the fix onto the real path by applying only the isolated saving,
    // then put it next to C measured in this same process.
    let projected = real - (row6 - row7);
    let c = ns_per_call(MEASURE_FOR, || fixture.c_expand_and_walk(&pattern, false));
    println!(
        "\n  c  [expand + walk]                          {c:>10.1} {:>10.2}\n  \
         port with a stored length, OPTIMISTIC      {projected:>10.1} {:>10.2}\n  \
         optimistic ratio: port/c = {:.2}x  ({})",
        per_term(c),
        per_term(projected),
        projected / c,
        if projected > c {
            "c still wins"
        } else {
            "port wins"
        },
    );
    println!(
        "  OPTIMISTIC because the saving is priced against row 6's pipeline, which\n  \
         is cheaper than the real one (see the gate). The profile puts strlen at\n  \
         ~14% of the real arm, so treat that share, not this row, as the ceiling\n  \
         on what removing the strlen alone can buy."
    );
}

/// Mean payload length of `terms`, terminator excluded.
fn mean_len(terms: &[&[u8]]) -> f64 {
    terms.iter().map(|t| t.len() - 1).sum::<usize>() as f64 / terms.len() as f64
}
