/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The one expansion form where C beats the port, taken apart.
//!
//! `benches/suffix_expand.rs` reports C at roughly twice the port's speed for
//! `suffix_expand/suffix/unique_tags=100000/*/matches=many`, while every other
//! group in the suite goes the other way. This example exists to explain that
//! single number, so it times the same call the bench does and then times the
//! pieces underneath it.
//!
//! Two structural facts frame the result. [`SuffixQuery::Suffix`] is *one exact
//! suffix-trie node lookup*, and both sides then have to deliver every term that
//! node holds:
//!
//! - C's `GetList_SuffixTrieMap` takes the `!prefix` branch, which is a
//!   `TrieMap_Find` plus a single `array_ensure_append_1` of the node's
//!   *already-built* `char **`. No term is touched during expansion — the caller
//!   in `src/query.c` walks them afterwards, `strlen` per term, which is what
//!   [`consume_suffix_matches`] replicates.
//! - The port yields terms one at a time through a `Box<dyn Iterator>` wrapping
//!   `Option::into_iter().flat_map(..)` over a `chain` of the node's own term and
//!   its refs, rebuilding each slice's length per term from a `strlen` of its own.
//!
//! So both arms pay one `strlen` per term, and the question is what else they pay.
//! The rows below separate that: `expand only` prices C's borrow-the-array
//! shortcut, and the two floors price work neither arm can avoid, leaving the
//! per-term remainder as the thing to attribute.
//!
//! `matches=few` is the control. It resolves to a node holding a single term, so
//! it prices the lookup alone — and the lookup is at parity, which is what points
//! at per-term delivery rather than at trie descent.
//!
//! Both `[bench arm]` rows land within a few percent of what
//! `benches/suffix_expand.rs` reports for the same configuration. That agreement is
//! the check that staging five measurements in one process has not distorted the
//! two that are supposed to be comparable.
//!
//! What the decomposition does *not* settle: the port's per-term remainder is
//! larger at `tag_len=48` than at `tag_len=8`, and neither iterator dispatch nor
//! slice construction should care how long a term is. Something in the port's
//! delivery scales with the bytes, and the table can only say that it does —
//! naming it is what `--profile` is for.
//!
//! For the complementary experiment — the same corpus, the same consume, only the
//! query form changed — see `examples/exact_node_vs_prefix_walk.rs`.
//!
//! # Usage
//!
//! ```sh
//! # The decomposition table, over the four interesting configurations.
//! cargo run --release --manifest-path src/redisearch_rs/Cargo.toml \
//!     -p tag_index_bencher --example suffix_exact_c_wins
//!
//! # A single arm in a hot loop, for a sampling profiler to attach to.
//! cargo run --release --manifest-path src/redisearch_rs/Cargo.toml \
//!     -p tag_index_bencher --example suffix_exact_c_wins -- --profile rust 20
//! ```
//!
//! Build with `--release`. The port's advantage everywhere else in the suite, and
//! its loss here, are both inlining-dependent, and a `dev` build measures neither.

use std::{
    ffi::{CStr, c_char},
    hint::black_box,
    time::Duration,
};

use tag_index::SuffixQuery;
use tag_index_bencher::{ExpandMode, Selectivity, SuffixFixture, ns_per_call};

/// Same seed as the benches, so this dissects the exact tries they measured.
const SEED: u64 = 42;

/// Per-row measurement window. Long enough to average out scheduler noise at the
/// hundreds-of-nanoseconds scale these rows live at, short enough to keep the
/// whole sweep interactive.
const MEASURE_FOR: Duration = Duration::from_millis(300);

/// The configurations worth pricing: the corpus size where the loss appears, both
/// term lengths, and both selectivities so the single-term lookup sits next to
/// the many-term walk.
const CONFIGS: &[(usize, usize, Selectivity)] = &[
    (100_000, 8, Selectivity::Many),
    (100_000, 48, Selectivity::Many),
    (100_000, 8, Selectivity::Few),
    (100_000, 48, Selectivity::Few),
];

/// Below this many terms, the one-off trie descent outweighs the term walk and
/// dividing by the term count says nothing about per-term cost.
const MIN_TERMS_FOR_ATTRIBUTION: usize = 8;

/// Default seconds to spin in `--profile` mode.
const PROFILE_SECS: u64 = 10;

/// The configuration `--profile` spins, being the one with the largest loss.
const PROFILE_CONFIG: (usize, usize) = (100_000, 8);

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("--profile") => {
            let arm = args.get(1).map(String::as_str).unwrap_or("rust");
            let secs = args
                .get(2)
                .and_then(|s| s.parse().ok())
                .unwrap_or(PROFILE_SECS);
            profile(arm, secs);
        }
        Some(other) => {
            eprintln!("unknown argument {other:?}; expected `--profile <c|rust> [seconds]`");
            std::process::exit(2);
        }
        None => {
            for &(unique_tags, tag_len, selectivity) in CONFIGS {
                dissect(unique_tags, tag_len, selectivity);
            }
        }
    }
}

/// Price every stage of one configuration and print the per-term attribution.
fn dissect(unique_tags: usize, tag_len: usize, selectivity: Selectivity) {
    let fixture = SuffixFixture::new(unique_tags, tag_len, SEED);
    let pattern = fixture.pattern(ExpandMode::Suffix, selectivity);
    let query = SuffixQuery::Suffix(&pattern);

    // The node's members, collected once: ground truth for the term count every
    // per-term column divides by, and the input to the two floor rows.
    let terms = fixture.rust_terms(query);
    let n = terms.len();
    assert!(n > 0, "the pattern is carved out of a tag the corpus holds");

    // Every yielded slice carries its trailing NUL, so its pointer is already a
    // valid C string over the same bytes C's array points at.
    let term_ptrs: Vec<*const c_char> = terms.iter().map(|t| t.as_ptr().cast()).collect();
    let mean_len = terms.iter().map(|t| t.len() - 1).sum::<usize>() as f64 / n as f64;

    println!(
        "\n=== suffix / unique_tags={unique_tags} / tag_len={tag_len} / matches={} ===",
        selectivity.as_str()
    );
    println!(
        "pattern {:?} ({} bytes) resolves to {n} term(s), mean length {mean_len:.1} bytes",
        String::from_utf8_lossy(&pattern),
        pattern.len(),
    );

    let c_only = ns_per_call(MEASURE_FOR, || fixture.c_expand_only(&pattern, false));
    let c_full = ns_per_call(MEASURE_FOR, || fixture.c_expand_and_walk(&pattern, false));
    let rust_full = ns_per_call(MEASURE_FOR, || fixture.rust_expand_and_walk(query));
    let rust_exact = ns_per_call(MEASURE_FOR, || fixture.rust_exact_and_walk(&pattern));

    // The `strlen` both arms pay per term: C's caller does it while walking the
    // array, the port does it while rebuilding each slice.
    let strlen_floor = ns_per_call(MEASURE_FOR, || {
        for &p in &term_ptrs {
            // SAFETY: `p` points at a NUL-terminated term owned by the suffix
            // trie in `fixture`, which is alive for the whole measurement.
            let len = unsafe { CStr::from_ptr(p) }.count_bytes();
            black_box(len);
        }
        term_ptrs.len()
    });

    // Touching the same terms with the length already known: no `strlen`, no
    // iterator, no expansion. A bound on how cheap delivery could ever get.
    let touch_floor = ns_per_call(MEASURE_FOR, || {
        for term in &terms {
            black_box(term);
        }
        terms.len()
    });

    let per_term = |total: f64| total / n as f64;
    println!("\n  {:<38} {:>12} {:>12}", "stage", "ns/call", "ns/term");
    for (label, ns) in [
        ("c: expand only (borrows the array)", c_only),
        ("c: expand + walk  [bench arm]", c_full),
        ("rust: expand + walk  [bench arm]", rust_full),
        ("rust: suffix_exact (no trait object)", rust_exact),
        ("floor: strlen over the same terms", strlen_floor),
        ("floor: touch pre-collected slices", touch_floor),
    ] {
        println!("  {label:<38} {ns:>12.1} {:>12.2}", per_term(ns));
    }

    // What each arm spends per term beyond the `strlen` they share. Only worth
    // printing once the walk outweighs the one-off trie descent: at a handful of
    // terms the per-term columns are mostly that descent, divided by `n`.
    if n >= MIN_TERMS_FOR_ATTRIBUTION {
        let c_walk = per_term(c_full - c_only) - per_term(strlen_floor);
        let rust_walk = per_term(rust_full) - per_term(strlen_floor);
        println!(
            "\n  per-term delivery cost above the shared strlen:\n    \
             c    {c_walk:>6.2} ns   indexed walk of a contiguous char **\n    \
             rust {rust_walk:>6.2} ns   Box<dyn Iterator> -> flat_map -> chain, slice rebuilt per term"
        );
        println!(
            "  c's expansion is {c_only:.1} ns/call for the whole node, \
             so {:.0}% of c's total is the term walk",
            (c_full - c_only) / c_full * 100.0
        );
    } else {
        println!(
            "\n  {n} term(s): the fixed trie descent dominates, so per-term\n  \
             attribution is not meaningful here. This row is the lookup control —\n  \
             read the ratio below, not the ns/term column."
        );
    }
    println!(
        "  bench arm ratio: rust/c = {:.2}x  ({})\n  \
         suffix_exact:    rust/c = {:.2}x  ({})",
        rust_full / c_full,
        if rust_full > c_full {
            "c wins"
        } else {
            "rust wins"
        },
        rust_exact / c_full,
        if rust_exact > c_full {
            "c wins"
        } else {
            "rust wins"
        }
    );
}

/// Spin one arm so a sampling profiler can attribute its time.
///
/// Only the requested arm runs, so the other implementation's frames stay out of
/// the profile.
fn profile(arm: &str, secs: u64) {
    let (unique_tags, tag_len) = PROFILE_CONFIG;
    let fixture = SuffixFixture::new(unique_tags, tag_len, SEED);
    let pattern = fixture.pattern(ExpandMode::Suffix, Selectivity::Many);
    let query = SuffixQuery::Suffix(&pattern);
    let window = Duration::from_secs(secs);

    eprintln!(
        "profiling `{arm}` for {secs}s: suffix / unique_tags={unique_tags} / \
         tag_len={tag_len} / matches=many"
    );

    // `ns_per_call` is the same hot loop the table rows use, so the profile is of
    // the code the table priced.
    let ns = match arm {
        "c" => ns_per_call(window, || fixture.c_expand_and_walk(&pattern, false)),
        "rust" => ns_per_call(window, || fixture.rust_expand_and_walk(query)),
        other => {
            eprintln!("unknown arm {other:?}; expected `c` or `rust`");
            std::process::exit(2);
        }
    };

    eprintln!("{ns:.1} ns/call");
}
