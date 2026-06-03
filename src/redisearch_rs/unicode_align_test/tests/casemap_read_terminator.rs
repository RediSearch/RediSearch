/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Sweep every Unicode scalar's `nu_tofold` and `nu_tolower` mapping by
//! walking the result through `nu_casemap_read` until a 0 codepoint is
//! read — the exact loop shape used by production code at
//! `src/util/strconv.h:160` (`unicode_tolower`) and
//! `src/trie/rune_util.c:97` (`strToLowerRunes`).
//!
//! # What this guards against
//!
//! A failure here means libnu's generated case-mapping data
//! (`deps/libnu/gen/_tofold.c` or `_tolower.c`) ships an entry without a
//! terminating 0 codepoint. The production C loop has no iteration cap; on
//! a malformed entry it would read OOB into adjacent static memory and
//! either return garbage codepoints or eventually segfault. Catching the
//! malformation here turns an indexer crash into a deterministic test
//! failure that names the offending input codepoint.
//!
//! # Bound choice
//!
//! Unicode full case mapping caps at 4 codepoints per input scalar (e.g.
//! `U+FB03` ﬃ → "ffi"). Setting `CAP = 16` gives roughly 4× headroom
//! over any spec-defined mapping length while still being a hard upper
//! bound that catches a runaway read.
//!
//! # Diagnostic value
//!
//! Even on a passing run the test prints expansion-length histograms and
//! the longest mappings observed per table — useful baselines for spotting
//! future regenerations of libnu's tables that introduce unexpectedly long
//! entries.

use std::collections::BTreeMap;

use unicode_align_test::{CaseMapTable, CasemapWalk, walk_casemap_expansion};
use rayon::prelude::*;

const CAP: usize = 16;

#[test]
fn nu_tofold_mappings_terminate_within_cap() {
    sweep("nu_tofold", CaseMapTable::Fold);
}

#[test]
fn nu_tolower_mappings_terminate_within_cap() {
    sweep("nu_tolower", CaseMapTable::Lower);
}

fn sweep(label: &str, table: CaseMapTable) {
    let results: Vec<(u32, CasemapWalk)> = (0u32..=0x10FFFF)
        .into_par_iter()
        .filter(|cp| !(0xD800..=0xDFFF).contains(cp))
        .filter(|cp| char::from_u32(*cp).is_some())
        .map(|cp| (cp, walk_casemap_expansion(cp, table, CAP)))
        .collect();

    let mut histogram: BTreeMap<usize, usize> = BTreeMap::new();
    let mut max_len = 0usize;
    let mut longest: Vec<(u32, Vec<u32>)> = Vec::new();
    let mut violations: Vec<(u32, Vec<u32>)> = Vec::new();
    let mut no_mapping = 0usize;

    for (cp, walk) in &results {
        match walk {
            CasemapWalk::NoMapping => no_mapping += 1,
            CasemapWalk::Terminated(v) => {
                *histogram.entry(v.len()).or_default() += 1;
                match v.len().cmp(&max_len) {
                    std::cmp::Ordering::Greater => {
                        max_len = v.len();
                        longest.clear();
                        longest.push((*cp, v.clone()));
                    }
                    std::cmp::Ordering::Equal if longest.len() < 8 => {
                        longest.push((*cp, v.clone()));
                    }
                    _ => {}
                }
            }
            CasemapWalk::CapReached(prefix) => violations.push((*cp, prefix.clone())),
        }
    }

    let mapped = results.len() - no_mapping;
    println!(
        "{label}: {mapped} mapping(s) across {} scalars ({no_mapping} passthrough)",
        results.len()
    );
    println!("{label}: expansion length histogram");
    for (len, count) in &histogram {
        println!("  {len:>2} codepoint(s): {count} mapping(s)");
    }
    println!(
        "{label}: {} longest mapping(s) (max len = {})",
        longest.len(),
        max_len
    );
    for (cp, seq) in &longest {
        let rendered: String = seq.iter().filter_map(|&c| char::from_u32(c)).collect();
        println!("  U+{cp:06X} -> {seq:?}  ({rendered:?})");
    }

    if !violations.is_empty() {
        eprintln!(
            "{label}: {} mapping(s) failed to reach a codepoint-0 terminator within {CAP} reads:",
            violations.len()
        );
        for (cp, prefix) in violations.iter().take(16) {
            eprintln!("  U+{cp:06X}  first {CAP} codepoints = {prefix:?}");
        }
        if violations.len() > 16 {
            eprintln!("  ... ({} more)", violations.len() - 16);
        }
    }

    assert!(
        violations.is_empty(),
        "{label}: {} mapping(s) exceeded the {CAP}-codepoint iteration cap — \
         likely a malformed entry in libnu's generated data; inspect \
         deps/libnu/gen/_tofold.c or deps/libnu/gen/_tolower.c. \
         In production the equivalent C loop has no cap and would read OOB.",
        violations.len()
    );
}
