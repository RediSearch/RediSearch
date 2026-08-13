/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The control for `examples/suffix_exact_c_wins.rs`: same corpus, same consume,
//! only the query form changed.
//!
//! Taken on its own, C winning the exact-suffix expansion admits a dull
//! explanation — that the port simply delivers terms more slowly than a C loop
//! does. This example is what rules that out. `*foo` and `*foo*` differ in exactly
//! one respect, the branch they take inside `GetList_SuffixTrieMap`:
//!
//! - `*foo` ([`SuffixQuery::Suffix`], `!prefix`) finds one node and appends its
//!   already-built `char **` to the result. Expansion is a lookup; the array is
//!   *borrowed*, so its size does not matter.
//! - `*foo*` ([`SuffixQuery::Contains`], `prefix`) iterates every node under the
//!   prefix and appends one entry per node. Expansion now scales with the walk,
//!   and eagerly, before the caller sees a single term.
//!
//! The port's delivery machinery is the same in both — the same
//! `Box<dyn Iterator>`, the same per-term slice rebuild — and it is lazy in both.
//! So if the port lost because its iteration is slow, it would lose both forms.
//! The `nodes` column is what makes the difference legible: it is the length of
//! the outer array C builds, one entry for the exact form and one per visited node
//! for the prefix form.
//!
//! # One form per process
//!
//! The two forms cannot share a process. Measuring the exact form first costs the
//! port's prefix-form arm about 25% — 719 µs against 574 µs at `tag_len=48` on the
//! machine this was written on — while leaving C's unchanged, which is enough to
//! flip the reported winner. C's number is stable across the two orderings and the
//! port's is not, so this is the port's sensitivity to the state the earlier rows
//! leave behind, not a property of either query form. Until that is understood, the
//! only trustworthy reading is one form per process, which is what the default mode
//! does by re-running itself once per form.
//!
//! # Usage
//!
//! ```sh
//! # Both forms, each in its own process.
//! cargo run --release --manifest-path src/redisearch_rs/Cargo.toml \
//!     -p tag_index_bencher --example exact_node_vs_prefix_walk
//!
//! # One form, in this process.
//! cargo run --release --manifest-path src/redisearch_rs/Cargo.toml \
//!     -p tag_index_bencher --example exact_node_vs_prefix_walk -- --form contains
//! ```
//!
//! Build with `--release`, for the reason given in the sibling example.

use std::{process::Command, time::Duration};

use tag_index::SuffixQuery;
use tag_index_bencher::{ExpandMode, Selectivity, SuffixFixture, ns_per_call};

/// Same seed as the benches, so this compares the tries they measured.
const SEED: u64 = 42;

/// Per-row measurement window. The prefix form runs three orders of magnitude
/// slower than the exact one, so this is sized for the cheap rows; the expensive
/// ones overshoot it by at most one batch.
const MEASURE_FOR: Duration = Duration::from_millis(300);

/// Both term lengths at the corpus size where the exact form's loss appears.
const CONFIGS: &[(usize, usize)] = &[(100_000, 8), (100_000, 48)];

/// Always the short token: the point is to compare the two forms with a match set
/// large enough that per-term delivery dominates.
const SELECTIVITY: Selectivity = Selectivity::Many;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [] => delegate_per_form(),
        [flag, form] if flag == "--form" => match form.as_str() {
            "suffix" => measure(ExpandMode::Suffix),
            "contains" => measure(ExpandMode::Contains),
            other => {
                eprintln!("unknown form {other:?}; expected `suffix` or `contains`");
                std::process::exit(2);
            }
        },
        _ => {
            eprintln!("unexpected arguments; expected `--form <suffix|contains>`");
            std::process::exit(2);
        }
    }
}

/// Re-run this example once per form, so neither form is measured in a process
/// the other has already run in.
fn delegate_per_form() {
    let exe = std::env::current_exe().expect("the running example has a path");

    for form in ["suffix", "contains"] {
        let status = Command::new(&exe)
            .arg("--form")
            .arg(form)
            .status()
            .expect("re-running this example");
        if !status.success() {
            eprintln!("child for form {form:?} failed: {status}");
            std::process::exit(1);
        }
    }

    println!(
        "\nThe port delivers terms the same way in both forms. Only C changes shape:\n\
         one borrowed array for the exact form, one entry per visited node for the\n\
         prefix form. C's win is the borrow, not faster iteration."
    );
}

/// Time both arms on `mode` across every configuration.
fn measure(mode: ExpandMode) {
    for &(unique_tags, tag_len) in CONFIGS {
        let fixture = SuffixFixture::new(unique_tags, tag_len, SEED);
        let pattern = fixture.pattern(mode, SELECTIVITY);
        let prefix = matches!(mode, ExpandMode::Contains);
        let query = if prefix {
            SuffixQuery::Contains(&pattern)
        } else {
            SuffixQuery::Suffix(&pattern)
        };

        // C's outer array: how many trie nodes this form's expansion visited.
        let nodes = fixture.c_expand_only(&pattern, prefix);
        let terms = fixture.rust_terms(query).len();

        let c = ns_per_call(MEASURE_FOR, || fixture.c_expand_and_walk(&pattern, prefix));
        let rust = ns_per_call(MEASURE_FOR, || fixture.rust_expand_and_walk(query));

        println!(
            "\n=== {} / unique_tags={unique_tags} / tag_len={tag_len} / matches={} ===",
            mode.as_str(),
            SELECTIVITY.as_str()
        );
        println!(
            "  {:>7} {:>9} {:>12} {:>14} {:>9} {:>9}",
            "nodes", "terms", "c ns/call", "rust ns/call", "rust/c", "winner"
        );
        println!(
            "  {nodes:>7} {terms:>9} {c:>12.1} {rust:>14.1} {:>8.2}x {:>9}",
            rust / c,
            if rust > c { "c" } else { "rust" },
        );
    }
}
