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
    generate_lowercase_table();
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

    let pairs = folds
        .into_iter()
        .map(|(codepoint, (_, first))| (codepoint, first))
        .filter(|(codepoint, first)| codepoint != first);
    write_block_map("case_fold.rs", "CASE_FOLD", pairs);
}

/// Writes `lowercase.rs` to `OUT_DIR`: the first codepoint of each
/// [`char::to_lowercase`] result, for every [`char`] it changes.
///
/// Taken from the `std` this script is built with, which is the `std` the
/// crate is built with, so the table matches [`char::to_lowercase`] exactly.
fn generate_lowercase_table() {
    let mut pairs = Vec::new();
    for c in (0..=char::MAX as u32).filter_map(char::from_u32) {
        let mut lowered = c.to_lowercase();
        let first = lowered
            .next()
            .expect("lowercasing yields at least one char");
        let rest: Vec<char> = lowered.collect();
        assert!(
            rest.is_empty() || (c == '\u{130}' && rest == ['\u{307}']),
            "{c:?} lowercases to several chars; runes::lowercase only expects U+0130 to"
        );
        if first != c {
            pairs.push((u32::from(c), u32::from(first)));
        }
    }
    write_block_map("lowercase.rs", "LOWERCASE", pairs);
}

/// Writes `file` to `OUT_DIR`, declaring `static {name}: BlockMap<N>` that
/// maps each codepoint in `pairs` to its non-zero value.
fn write_block_map(file: &str, name: &str, pairs: impl IntoIterator<Item = (u32, u32)>) {
    const BLOCK: u32 = 256;
    let mut blocks = BTreeMap::<u32, Vec<u32>>::new();
    for (codepoint, mapped) in pairs {
        assert_ne!(mapped, 0, "0 marks an unmapped codepoint");
        blocks
            .entry(codepoint / BLOCK)
            .or_insert_with(|| vec![0; BLOCK as usize])[(codepoint % BLOCK) as usize] = mapped;
    }
    assert!(
        blocks.len() < usize::from(u8::MAX),
        "u8::MAX marks a block with no mappings"
    );

    let mut index = vec![u8::MAX; (char::MAX as u32 / BLOCK + 1) as usize];
    let mut out = format!(
        "static {name}: BlockMap<{}> = BlockMap {{\n    blocks: [\n",
        blocks.len()
    );
    for (position, (block, values)) in blocks.iter().enumerate() {
        index[*block as usize] = position as u8;
        writeln!(out, "        {values:?},").unwrap();
    }
    writeln!(out, "    ],\n    index: {index:?},\n}};").unwrap();

    let dest = PathBuf::from(env::var("OUT_DIR").unwrap()).join(file);
    fs::write(dest, out).unwrap();
}
