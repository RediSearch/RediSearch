/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Run both folders against real-world corpora used elsewhere in this repo.
//!
//! We reuse the same files referenced by `trie_bencher::corpus::CorpusType`
//! (`1984.txt` and the Wikipedia abstracts CSV), but with a lighter-weight
//! download helper so this crate doesn't drag in the trie_bencher dependency
//! graph (which pulls in C linking).
//!
//! The tests gracefully skip when network is unavailable.

use std::path::PathBuf;

use unicode_align_test::diff::run_corpus;

const GUTENBERG_URL: &str = "https://gutenberg.net.au/ebooks01/0100021.txt";
const GUTENBERG_CRC32: u32 = 3817457071;
const GUTENBERG_FILE: &str = "1984.txt";

const WIKI_URL: &str = "https://s3.amazonaws.com/benchmarks.redislabs/redisearch/datasets/enwiki_abstract-hashes-contains/enwiki_abstract-hashes-contains.redisearch.commands.SETUP.csv";
const WIKI_CRC32: u32 = 0x65ed64eb;
const WIKI_FILE: &str = "enwiki_abstract-hashes-contains.redisearch.commands.SETUP.csv";

fn cache_dir() -> PathBuf {
    let base = std::env::var("CARGO_TARGET_TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir().join("unicode_align_test"));
    let dir = base.join("corpora");
    if !dir.exists() {
        let _ = fs_err::create_dir_all(&dir);
    }
    dir
}

/// Returns Ok(contents) on success, Err with reason on network/IO failure.
///
/// If a cached copy exists but fails the CRC check, it is discarded and
/// re-downloaded — otherwise a corrupted cache would wedge the test
/// indefinitely until manual deletion.
fn fetch(url: &str, expected_crc32: u32, filename: &str) -> Result<String, String> {
    let path = cache_dir().join(filename);
    if path.exists() {
        let body = fs_err::read_to_string(&path)
            .map_err(|e| format!("read cache {}: {e}", path.display()))?;
        if crc32fast::hash(body.as_bytes()) == expected_crc32 {
            return Ok(body);
        }
        eprintln!("[fetch] cached {filename} failed CRC; re-downloading from {url}");
        let _ = fs_err::remove_file(&path);
    }

    let response = ureq::get(url)
        .call()
        .map_err(|e| format!("download {url}: {e}"))?;
    if !response.status().is_success() {
        return Err(format!("download {url}: status {}", response.status()));
    }
    let body = response
        .into_body()
        .read_to_string()
        .map_err(|e| format!("read body {url}: {e}"))?;

    let checksum = crc32fast::hash(body.as_bytes());
    if checksum != expected_crc32 {
        return Err(format!(
            "checksum mismatch for {filename}: got 0x{checksum:08x}, expected 0x{expected_crc32:08x}"
        ));
    }

    fs_err::write(&path, body.as_bytes())
        .map_err(|e| format!("write cache {}: {e}", path.display()))?;
    Ok(body)
}

fn run_lines(label: &str, source: &str, max_lines: Option<usize>) {
    let inputs: Vec<String> = match max_lines {
        Some(limit) => source.lines().take(limit).map(str::to_owned).collect(),
        None => source.lines().map(str::to_owned).collect(),
    };
    println!("\n=== {label} ({} lines) ===", inputs.len());
    let report = run_corpus(inputs);
    print!("{}", report.render());
}

fn skip_if<E: std::fmt::Display>(label: &str, err: E) {
    eprintln!("[skip] {label}: {err}");
}

#[test]
fn gutenberg_1984() {
    match fetch(GUTENBERG_URL, GUTENBERG_CRC32, GUTENBERG_FILE) {
        Ok(text) => run_lines("Gutenberg 1984", &text, None),
        Err(e) => skip_if("Gutenberg 1984", e),
    }
}

#[test]
fn wikipedia_abstracts() {
    match fetch(WIKI_URL, WIKI_CRC32, WIKI_FILE) {
        // CSV is large (~tens of MB). Cap to 5000 rows to keep the test snappy
        // — that's still plenty of multilingual material.
        Ok(text) => run_lines("Wikipedia abstracts (capped)", &text, Some(5000)),
        Err(e) => skip_if("Wikipedia abstracts", e),
    }
}
