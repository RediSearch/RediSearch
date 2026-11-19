/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{collections::BTreeMap, env, fmt::Write, fs, path::PathBuf};

fn main() {
    #[cfg(feature = "unittest")]
    build_utils::bind_foreign_c_symbols();

    generate_case_fold_table();
}

/// Writes `case_fold.rs` to `OUT_DIR`: the first codepoint of each codepoint's
/// case folding, for every codepoint it changes, sorted by codepoint.
///
/// The table is read from the same Unicode Character Database file the
/// vendored libnu builds its `nu_tofold` table from, and picks a mapping per
/// codepoint the way libnu's `casefolding-tofolding` tool does: full (`F`)
/// over common (`C`) over simple (`S`), never Turkic (`T`).
fn generate_case_fold_table() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let source = manifest_dir.join("../../../deps/libnu/regen/unicode.org/CaseFolding.txt");
    println!("cargo::rerun-if-changed={}", source.display());
    let text = fs::read_to_string(&source)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", source.display()));

    const STATUS_PRIORITY: [&str; 3] = ["F", "C", "S"];
    // Codepoint -> (priority, first codepoint of its folding).
    let mut folds = BTreeMap::<u32, (usize, u32)>::new();
    for line in text.lines() {
        let data = line.split('#').next().unwrap().trim();
        if data.is_empty() {
            continue;
        }
        let fields: Vec<&str> = data.split(';').map(str::trim).collect();
        let Some(priority) = STATUS_PRIORITY.iter().position(|s| *s == fields[1]) else {
            continue;
        };
        let codepoint = u32::from_str_radix(fields[0], 16).unwrap();
        let first = fields[2].split_whitespace().next().unwrap();
        let first = u32::from_str_radix(first, 16).unwrap();
        let entry = folds.entry(codepoint).or_insert((priority, first));
        if priority < entry.0 {
            *entry = (priority, first);
        }
    }

    let mut out = String::from("[\n");
    for (codepoint, (_, first)) in folds {
        if codepoint != first {
            writeln!(out, "    (0x{codepoint:04X}, 0x{first:04X}),").unwrap();
        }
    }
    out.push(']');

    let dest = PathBuf::from(env::var("OUT_DIR").unwrap()).join("case_fold.rs");
    fs::write(dest, out).unwrap();
}
