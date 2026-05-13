/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Side-by-side case-mapping comparison between libnu (used by RediSearch's
//! C code) and icu_casemap (used by the Rust sorting vector).
//!
//! Two operations are exposed, each as a pair of mappers + a [`compare`]-shape
//! wrapper:
//!
//! - **Case folding** ([`fold_libnu`], [`fold_icu`], [`compare`], [`run_corpus`]):
//!   matches RediSearch's `runeFold` in `src/trie/rune_util.c` against
//!   `CaseMapper::fold_string` (used by `try_insert_string_normalize` in
//!   `src/redisearch_rs/sorting_vector/src/lib.rs`).
//! - **Lowercase** ([`lower_libnu`], [`lower_icu`], [`compare_lower`],
//!   [`run_corpus_lower`]): matches RediSearch's `unicode_tolower` in
//!   `src/util/strconv.h` (the hot path for indexed/queried text — every
//!   token flows through `nu_tolower` + `nu_casemap_read`) against
//!   `CaseMapper::lowercase_to_string` at the root locale ("und").
//!
//! The two libraries are known to diverge on full Unicode case folding
//! (e.g. ß → "ss"), final sigma, Turkish I, and many script-specific
//! mappings. This crate is a test utility, not a production component:
//! it exists only to characterise where, and how, the two implementations
//! disagree.
//!
//! Note: libnu's `nu_tolower` is unconditional (no context-sensitivity),
//! while ICU's `lowercase_to_string` at root locale *is* context-sensitive
//! for Greek final sigma. The lowercase comparison therefore surfaces both
//! table-content divergences and the structural gap that libnu's `_nu_tolower`
//! variant would close but which RediSearch does not currently call.

use std::collections::{BTreeMap, HashMap};
use std::ffi::CStr;
use std::fmt::Write as _;

use icu_casemap::CaseMapper;
use icu_locale_core::LanguageIdentifier;
use rayon::prelude::*;

mod libnu_ffi;

/// Fold `s` using libnu's per-codepoint `nu_tofold`.
///
/// For each input codepoint:
///
/// - If `nu_tofold(cp)` returns non-NULL, the result is a null-terminated
///   UTF-8 string of zero or more codepoints which replaces the input.
/// - Otherwise (no mapping), the codepoint is copied verbatim.
///
/// This exercises libnu's fold tables (the same tables `runeFold` in
/// `src/trie/rune_util.c` uses) but skips the buggy `nu_utf8_write`
/// re-encode — we use Rust's native UTF-8 handling instead so that
/// divergence numbers reflect *fold-table* differences against ICU and
/// are not contaminated by the `b4_utf8` encoder bug. Use
/// [`fold_libnu_raw`] or [`encode_codepoint_with_libnu`] to observe the
/// encoder bug directly.
pub fn fold_libnu(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        let cp = ch as u32;
        // SAFETY: `nu_tofold` is a pure function; any u32 input is accepted
        // and the returned pointer is either NULL or points to a static,
        // null-terminated UTF-8 buffer owned by libnu's data tables.
        let encoded = unsafe { libnu_ffi::nu_tofold(cp) };
        if encoded.is_null() {
            out.push(ch);
            continue;
        }
        // SAFETY: `encoded` is non-NULL and points to a null-terminated
        // UTF-8 byte sequence inside libnu's static data.
        let cstr = unsafe { CStr::from_ptr(encoded) };
        match cstr.to_str() {
            Ok(mapped) => out.push_str(mapped),
            Err(_) => out.push('\u{FFFD}'),
        }
    }
    out
}

/// Like [`fold_libnu`] but returns the raw bytes copied out of libnu's
/// `nu_tofold` table, without any UTF-8 validation.
///
/// [`fold_libnu`] silently substitutes U+FFFD when libnu hands back bytes that
/// don't decode as UTF-8, which is the wrong behaviour for the question
/// "does libnu ever emit invalid UTF-8?". This function preserves whatever
/// libnu actually wrote, so callers can validate it explicitly with
/// `std::str::from_utf8`.
///
/// Passthrough codepoints (those where `nu_tofold` returns NULL) are
/// re-encoded with Rust's standard UTF-8 encoder, which is by construction
/// valid — any invalid bytes in the result therefore came from libnu's
/// folding tables.
pub fn fold_libnu_raw(s: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(s.len());
    for ch in s.chars() {
        let cp = ch as u32;
        // SAFETY: see `fold_libnu` — `nu_tofold` is a pure function and the
        // returned pointer is either NULL or points to a static,
        // null-terminated buffer owned by libnu.
        let encoded = unsafe { libnu_ffi::nu_tofold(cp) };
        if encoded.is_null() {
            let mut buf = [0u8; 4];
            out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
            continue;
        }
        // SAFETY: `encoded` is non-NULL and points to a null-terminated byte
        // sequence inside libnu's static data.
        let cstr = unsafe { CStr::from_ptr(encoded) };
        out.extend_from_slice(cstr.to_bytes());
    }
    out
}

/// Encode `cp` to UTF-8 using libnu's *runtime* encoder (`nu_utf8_write`),
/// returning whatever bytes libnu wrote without any validation.
///
/// `nu_utf8_write` dispatches to `b{1..4}_utf8` based on the codepoint's
/// natural UTF-8 length. The 4-byte path (`b4_utf8` in
/// `deps/libnu/utf8_internal.h`) is what RediSearch invokes for any
/// supplementary-plane codepoint via two live call sites:
///
/// - `unicode_tolower()` in `src/util/strconv.h` — lowercases a UTF-8
///   string in place, re-encoding each lowered codepoint through
///   `nu_utf8_write` (used during query/document text normalisation).
/// - `runesToStr()` in `src/trie/rune_util.c` — converts a trie rune
///   array back to UTF-8 via `nu_writestr`/`nu_utf8_write` (used when
///   returning matched terms from the trie).
///
/// Both paths produce corrupted bytes for every supplementary-plane
/// codepoint with bit 12 set. This is the function to sweep when asking
/// "does libnu *ever* emit invalid UTF-8 from its runtime encoder?".
pub fn encode_codepoint_with_libnu(cp: u32) -> Vec<u8> {
    let mut buf = [0u8; 4];
    let begin = buf.as_mut_ptr() as *mut std::ffi::c_char;
    // SAFETY: `nu_utf8_write` writes at most 4 bytes (the UTF-8 maximum for
    // any codepoint <= U+10FFFF) into the buffer at `begin` and returns a
    // pointer to one past the last byte written. The buffer is exactly 4
    // bytes, so the writer cannot overrun.
    let end = unsafe { libnu_ffi::nu_utf8_write(cp, begin) };
    // SAFETY: `end` is derived from `begin` (returned by `nu_utf8_write`
    // pointing into the same 4-byte allocation), so both pointers share an
    // origin and the difference fits in `isize`.
    let written = unsafe { end.offset_from(begin) } as usize;
    buf[..written].to_vec()
}

/// Decode the first UTF-8 codepoint at the start of `bytes` using libnu's
/// `nu_utf8_read` (via the in-crate `nu_utf8_read_shim` wrapper around the
/// `static inline` upstream symbol).
///
/// Returns `(codepoint, advance)` where `codepoint` is the decoded scalar
/// (as `u32`, since the caller may want to inspect malformed values that
/// don't fit `char`) and `advance` is the number of bytes consumed.
///
/// `bytes` must be non-empty. The function inspects the lead byte to decide
/// how many bytes to read (1–4) and only touches that many — `nu_utf8_read`
/// does no bounds checking, so the caller must ensure the slice has enough
/// content for the lead byte's class. For the round-trip sweep this is
/// trivially true because we always feed it a canonical encoding.
pub fn decode_codepoint_with_libnu(bytes: &[u8]) -> (u32, usize) {
    let mut decoded: u32 = 0;
    let begin = bytes.as_ptr() as *const std::ffi::c_char;
    // SAFETY: `nu_utf8_read` reads 1–4 bytes from `begin` depending on the
    // lead byte's range and writes one `u32` to `&mut decoded`. The caller
    // contract requires `bytes` to hold a full codepoint, so the read stays
    // in bounds.
    let end = unsafe { libnu_ffi::nu_utf8_read_shim(begin, &mut decoded) };
    // SAFETY: `end` is derived from `begin` (both point into `bytes`), so
    // they share an origin and the difference fits in `isize`.
    let consumed = unsafe { end.offset_from(begin) } as usize;
    (decoded, consumed)
}

/// Predicted codepoint count after lowercasing `bytes` via libnu's
/// `nu_strtransformnlen` with `nu_tolower` + `nu_casemap_read`. Mirrors
/// the `nu_strtransformnlen` call at `src/util/strconv.h:132` and
/// `src/trie/rune_util.c:68`.
pub fn predict_lower_len(bytes: &[u8]) -> isize {
    let begin = bytes.as_ptr() as *const std::ffi::c_char;
    // SAFETY: libnu reads up to `bytes.len()` bytes starting at `begin` and
    // stops at the first NUL byte. The slice provides exactly that much
    // valid memory.
    unsafe { libnu_ffi::nu_strtransformnlen_lower_shim(begin, bytes.len()) }
}

/// Predicted codepoint count for the NUL-terminated UTF-8 buffer `s` via
/// libnu's `nu_strlen`.
///
/// `s` must end with a NUL byte. The function reads codepoints until the
/// NUL terminator.
pub fn predict_strlen(s: &[u8]) -> isize {
    debug_assert!(
        s.contains(&0),
        "predict_strlen requires NUL-terminated input"
    );
    let begin = s.as_ptr() as *const std::ffi::c_char;
    // SAFETY: libnu reads codepoints starting at `begin` until it hits a
    // NUL byte; the caller-provided NUL terminator within `s` guarantees a
    // bounded read.
    unsafe { libnu_ffi::nu_strlen_shim(begin) }
}

/// Predicted codepoint count for the first `bytes.len()` bytes of `bytes`
/// via libnu's `nu_strnlen` (stops early at NUL).
pub fn predict_strnlen(bytes: &[u8]) -> isize {
    let begin = bytes.as_ptr() as *const std::ffi::c_char;
    // SAFETY: libnu reads at most `bytes.len()` bytes starting at `begin`.
    unsafe { libnu_ffi::nu_strnlen_shim(begin, bytes.len()) }
}

/// Predicted UTF-8 byte count to encode the NUL-terminated `unicode` array
/// via libnu's `nu_bytelen` with `nu_utf8_write`.
pub fn predict_bytelen(unicode: &[u32]) -> isize {
    debug_assert!(
        unicode.contains(&0),
        "predict_bytelen requires a NUL-terminated codepoint slice"
    );
    // SAFETY: libnu reads codepoints starting at `unicode.as_ptr()` until
    // it hits a 0 sentinel; the caller-provided 0 within `unicode`
    // guarantees a bounded read.
    unsafe { libnu_ffi::nu_bytelen_shim(unicode.as_ptr()) }
}

/// Predicted UTF-8 byte count to encode the first `unicode.len()` codepoints
/// of `unicode` via libnu's `nu_bytenlen` (stops early at NUL).
pub fn predict_bytenlen(unicode: &[u32]) -> isize {
    // SAFETY: libnu reads at most `unicode.len()` codepoints from
    // `unicode.as_ptr()`.
    unsafe { libnu_ffi::nu_bytenlen_shim(unicode.as_ptr(), unicode.len()) }
}

/// Sum the per-codepoint UTF-8 byte counts that libnu's `nu_utf8_write`
/// would emit for `unicode`, stopping at the first 0 codepoint (the same
/// terminator semantics `nu_bytelen` / `nu_bytenlen` use).
///
/// This is the "actual" byte count to compare against `predict_bytelen` /
/// `predict_bytenlen`: both predictors compute the same value by invoking
/// the write iterator once per codepoint internally, so the sum here is the
/// reference value the predictors must agree with.
pub fn actual_encoded_byte_count(unicode: &[u32]) -> usize {
    let mut total = 0usize;
    for &cp in unicode {
        if cp == 0 {
            break;
        }
        total += encode_codepoint_with_libnu(cp).len();
    }
    total
}

/// Encode the first `unicode.len()` codepoints of `unicode` into a freshly
/// allocated UTF-8 buffer via libnu's `nu_writenstr`. Stops at the first 0
/// codepoint. The buffer is sized from [`actual_encoded_byte_count`] rather
/// than from the predictor under test, so a buggy predictor cannot trigger
/// an out-of-bounds write.
pub fn write_with_libnu(unicode: &[u32]) -> Vec<u8> {
    let expected = actual_encoded_byte_count(unicode);
    let mut buf = vec![0u8; expected];
    if expected == 0 {
        return buf;
    }
    let dst = buf.as_mut_ptr() as *mut std::ffi::c_char;
    // SAFETY: `buf` has `expected` bytes, sized from
    // `actual_encoded_byte_count` (independent of `nu_writenstr`). The
    // writer emits exactly that many bytes because both routines invoke
    // `nu_utf8_write` per codepoint and stop at the first 0.
    let rc = unsafe { libnu_ffi::nu_writenstr_shim(unicode.as_ptr(), unicode.len(), dst) };
    debug_assert_eq!(rc, 0, "nu_writenstr returned non-zero status: {rc}");
    buf
}

/// Fold `s` using ICU4X's full Unicode case folding (`icu_casemap::CaseMapper`).
///
/// This mirrors `try_insert_string_normalize()` at
/// `src/redisearch_rs/sorting_vector/src/lib.rs:131-141`.
pub fn fold_icu(s: &str) -> String {
    CaseMapper::new().fold_string(s).into_owned()
}

/// Lowercase `s` using libnu's per-codepoint `nu_tolower`.
///
/// This mirrors the production text-normalisation path in
/// `unicode_tolower()` at `src/util/strconv.h:121` and `strToLowerRunes()`
/// at `src/trie/rune_util.c:65`: for each input codepoint, `nu_tolower`
/// returns either NULL (passthrough) or a null-terminated UTF-8 mapping
/// that may be longer than one codepoint (e.g. multi-codepoint expansions
/// in some scripts). We iterate the mapping by decoding it natively as
/// UTF-8, which is equivalent to the `nu_casemap_read` loop in production
/// (`nu_casemap_read` aliases `nu_utf8_read`).
///
/// Unlike production, this function does *not* re-encode the result via
/// `nu_utf8_write`. That isolates lowercase-table divergence from the
/// `b4_utf8` encoder concerns covered by [`encode_codepoint_with_libnu`].
pub fn lower_libnu(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        let cp = ch as u32;
        // SAFETY: `nu_tolower` is a pure function; any u32 input is accepted
        // and the returned pointer is either NULL or points to a static,
        // null-terminated UTF-8 buffer owned by libnu's data tables.
        let encoded = unsafe { libnu_ffi::nu_tolower(cp) };
        if encoded.is_null() {
            out.push(ch);
            continue;
        }
        // SAFETY: `encoded` is non-NULL and points to a null-terminated
        // UTF-8 byte sequence inside libnu's static data.
        let cstr = unsafe { CStr::from_ptr(encoded) };
        match cstr.to_str() {
            Ok(mapped) => out.push_str(mapped),
            Err(_) => out.push('\u{FFFD}'),
        }
    }
    out
}

/// Lowercase `s` using ICU4X (`CaseMapper::lowercase_to_string`) at the
/// root locale ("und").
///
/// The root locale gives the spec-default behaviour: context-sensitive
/// (Greek final sigma) but not language-specific (no Turkish dotless-I).
/// That makes it the right reference point for a non-locale-aware C library
/// like libnu.
pub fn lower_icu(s: &str) -> String {
    CaseMapper::new()
        .lowercase_to_string(s, &LanguageIdentifier::UNKNOWN)
        .into_owned()
}

/// Per-codepoint divergence record, used when both folders happen to produce
/// outputs of the same codepoint count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodepointDiff {
    pub position: usize,
    pub libnu: char,
    pub icu: char,
}

/// The result of folding a single input string with both folders.
#[derive(Debug, Clone)]
pub struct Diff {
    pub input: String,
    pub libnu: String,
    pub icu: String,
    pub differs: bool,
    /// Populated only when [`Self::libnu`] and [`Self::icu`] have the same
    /// codepoint count — gives a precise position-by-position view of the
    /// divergence. For length-mismatched diffs this is empty (the byte-length
    /// histogram in [`Report`] captures the bulk shape instead).
    pub codepoint_diff: Vec<CodepointDiff>,
}

impl Diff {
    /// Render a human-readable summary suitable for stdout.
    pub fn render(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "input  : {:?}", self.input);
        let _ = writeln!(out, "         codepoints: {:?}", codepoints(&self.input));
        let _ = writeln!(out, "libnu  : {:?}", self.libnu);
        let _ = writeln!(out, "         codepoints: {:?}", codepoints(&self.libnu));
        let _ = writeln!(out, "icu    : {:?}", self.icu);
        let _ = writeln!(out, "         codepoints: {:?}", codepoints(&self.icu));
        let _ = writeln!(
            out,
            "differs: {}  (libnu_bytes={}, icu_bytes={}, delta={:+})",
            self.differs,
            self.libnu.len(),
            self.icu.len(),
            self.icu.len() as i64 - self.libnu.len() as i64,
        );
        if !self.codepoint_diff.is_empty() {
            let _ = writeln!(out, "per-codepoint divergences:");
            for d in &self.codepoint_diff {
                let _ = writeln!(
                    out,
                    "  pos {}: libnu=U+{:04X} ({:?}) icu=U+{:04X} ({:?})",
                    d.position, d.libnu as u32, d.libnu, d.icu as u32, d.icu,
                );
            }
        }
        out
    }
}

fn codepoints(s: &str) -> Vec<String> {
    s.chars().map(|c| format!("U+{:04X}", c as u32)).collect()
}

/// Fold `s` with both folders and produce a [`Diff`].
pub fn compare(s: &str) -> Diff {
    diff_from_outputs(s.to_owned(), fold_libnu(s), fold_icu(s))
}

/// Lowercase `s` with both implementations and produce a [`Diff`].
///
/// Mirrors [`compare`] but uses [`lower_libnu`] / [`lower_icu`] instead of
/// the fold variants. This is the comparison that matches production text
/// normalisation (`unicode_tolower` in `src/util/strconv.h`).
pub fn compare_lower(s: &str) -> Diff {
    diff_from_outputs(s.to_owned(), lower_libnu(s), lower_icu(s))
}

/// Build a [`Diff`] from already-computed libnu / ICU outputs. Shared between
/// [`compare`] (fold) and [`compare_lower`] (lowercase) so the codepoint-diff
/// logic and the [`Report`] aggregator stay operation-agnostic.
fn diff_from_outputs(input: String, libnu: String, icu: String) -> Diff {
    let differs = libnu != icu;

    let codepoint_diff = if differs && libnu.chars().count() == icu.chars().count() {
        libnu
            .chars()
            .zip(icu.chars())
            .enumerate()
            .filter_map(|(position, (l, i))| {
                (l != i).then_some(CodepointDiff {
                    position,
                    libnu: l,
                    icu: i,
                })
            })
            .collect()
    } else {
        Vec::new()
    };

    Diff {
        input,
        libnu,
        icu,
        differs,
        codepoint_diff,
    }
}

/// Per-Unicode-block aggregate statistics.
#[derive(Debug, Clone, Default)]
pub struct BlockStats {
    pub total: usize,
    pub diverged: usize,
}

/// Aggregate report across a corpus of inputs.
#[derive(Debug, Clone, Default)]
pub struct Report {
    pub total: usize,
    pub matched: usize,
    pub diverged: usize,
    /// First N diverging diffs in full (capped to keep output readable).
    pub diff_samples: Vec<Diff>,
    /// Single-codepoint inputs that diverge: input cp → (libnu output, icu output).
    pub codepoint_table: BTreeMap<u32, (String, String)>,
    /// Bucketed distribution of `len(icu) - len(libnu)` in bytes.
    pub byte_len_histogram: HashMap<i64, usize>,
    /// Per-Unicode-block totals (keyed by the block of the *first* codepoint
    /// of the input, as a coarse routing heuristic).
    pub block_breakdown: HashMap<&'static str, BlockStats>,
}

impl Report {
    /// Cap on the number of full-diff samples retained.
    pub const SAMPLE_CAP: usize = 50;

    fn record(&mut self, diff: Diff) {
        self.total += 1;

        let bucket = bucket_byte_delta(diff.icu.len() as i64 - diff.libnu.len() as i64);
        *self.byte_len_histogram.entry(bucket).or_default() += 1;

        let block = diff
            .input
            .chars()
            .next()
            .map(|c| unicode_block(c as u32))
            .unwrap_or("Empty");
        let entry = self.block_breakdown.entry(block).or_default();
        entry.total += 1;

        if diff.differs {
            self.diverged += 1;
            entry.diverged += 1;

            // If the input is a single codepoint, record into the codepoint table.
            let mut chars = diff.input.chars();
            if let (Some(c), None) = (chars.next(), chars.next()) {
                self.codepoint_table
                    .insert(c as u32, (diff.libnu.clone(), diff.icu.clone()));
            }

            if self.diff_samples.len() < Self::SAMPLE_CAP {
                self.diff_samples.push(diff);
            }
        } else {
            self.matched += 1;
        }
    }

    /// Merge `other` into `self`. Used by the rayon fold/reduce pipeline:
    /// each worker accumulates into a thread-local `Report` and the reduce
    /// step folds them together into a single combined report.
    fn merge(&mut self, other: Self) {
        self.total += other.total;
        self.matched += other.matched;
        self.diverged += other.diverged;

        for (delta, count) in other.byte_len_histogram {
            *self.byte_len_histogram.entry(delta).or_default() += count;
        }

        for (name, stats) in other.block_breakdown {
            let entry = self.block_breakdown.entry(name).or_default();
            entry.total += stats.total;
            entry.diverged += stats.diverged;
        }

        // The codepoint table is keyed by a unique codepoint, so collisions
        // are only possible when the same single-codepoint input shows up in
        // both halves — and in that case both halves produced identical
        // (libnu, icu) tuples, so first-wins is correct.
        for (cp, mappings) in other.codepoint_table {
            self.codepoint_table.entry(cp).or_insert(mappings);
        }

        for sample in other.diff_samples {
            if self.diff_samples.len() >= Self::SAMPLE_CAP {
                break;
            }
            self.diff_samples.push(sample);
        }
    }

    /// Render the aggregate report as human-readable text.
    pub fn render(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "=== casemap_compare report ===");
        let pct = if self.total > 0 {
            100.0 * self.diverged as f64 / self.total as f64
        } else {
            0.0
        };
        let _ = writeln!(
            out,
            "total={}  matched={}  diverged={}  ({:.2}%)",
            self.total, self.matched, self.diverged, pct
        );

        let _ = writeln!(out, "\n-- byte-length delta histogram (icu - libnu) --");
        let mut hist: Vec<_> = self.byte_len_histogram.iter().collect();
        hist.sort_by_key(|(k, _)| **k);
        for (delta, count) in hist {
            let label = match *delta {
                i64::MIN..=-2 => "<= -2".to_owned(),
                -1 => "-1".to_owned(),
                0 => "0".to_owned(),
                1 => "+1".to_owned(),
                2 => "+2".to_owned(),
                3 => "+3".to_owned(),
                _ => ">= +4".to_owned(),
            };
            let _ = writeln!(out, "  {:>6}: {}", label, count);
        }

        let _ = writeln!(out, "\n-- per-block breakdown --");
        let mut blocks: Vec<_> = self.block_breakdown.iter().collect();
        blocks.sort_by_key(|(name, _)| *name);
        for (name, stats) in blocks {
            let _ = writeln!(
                out,
                "  {:<30} total={:>6} diverged={:>6}",
                name, stats.total, stats.diverged
            );
        }

        let _ = writeln!(
            out,
            "\n-- single-codepoint divergence table ({} entries) --",
            self.codepoint_table.len()
        );
        for (cp, (libnu, icu)) in &self.codepoint_table {
            let _ = writeln!(
                out,
                "  U+{:04X} [{}]  libnu={:?}  icu={:?}",
                cp,
                unicode_block(*cp),
                libnu,
                icu
            );
        }

        let _ = writeln!(
            out,
            "\n-- first {} diverging inputs --",
            self.diff_samples.len()
        );
        for (i, diff) in self.diff_samples.iter().enumerate() {
            let _ = writeln!(out, "\n[diff {}]", i + 1);
            out.push_str(&diff.render());
        }

        out
    }
}

/// Bucket a byte-length delta into `[-2..=3]` plus saturating bins.
fn bucket_byte_delta(delta: i64) -> i64 {
    delta.clamp(-2, 4)
}

/// Compare every input in `inputs` (in parallel via rayon) under the fold
/// operation and return an aggregate [`Report`].
///
/// The pipeline is a classic fold/reduce: each rayon worker maintains a
/// thread-local [`Report`] that absorbs results via [`Report::record`], and
/// the reduce step folds the per-thread reports together via
/// [`Report::merge`]. This keeps the worker hot path lock-free.
///
/// The input is materialised into a `Vec<String>` before parallel splitting
/// because rayon needs random-access access to balance work across cores. For
/// streaming use cases this is the wrong fit, but for the comparison harness
/// the corpora are already finite and fit comfortably in memory.
pub fn run_corpus<I>(inputs: I) -> Report
where
    I: IntoParallelIterator<Item = String>,
{
    run_corpus_with(inputs, compare)
}

/// Compare every input in `inputs` under the lowercase operation and return
/// an aggregate [`Report`]. See [`run_corpus`] for the pipeline shape; this
/// variant differs only in the per-input comparison ([`compare_lower`]).
pub fn run_corpus_lower<I>(inputs: I) -> Report
where
    I: IntoParallelIterator<Item = String>,
{
    run_corpus_with(inputs, compare_lower)
}

fn run_corpus_with<I, F>(inputs: I, compare_fn: F) -> Report
where
    I: IntoParallelIterator<Item = String>,
    F: Fn(&str) -> Diff + Sync + Send,
{
    inputs
        .into_par_iter()
        .map(|s| compare_fn(&s))
        .fold(Report::default, |mut acc, diff| {
            acc.record(diff);
            acc
        })
        .reduce(Report::default, |mut a, b| {
            a.merge(b);
            a
        })
}

/// Return a coarse Unicode-block name for `cp`. Covers the blocks that
/// historically exhibit case-fold divergences plus a few common ones; falls
/// back to "Other" for everything else. This is a debug aid, not a complete
/// Unicode block table.
pub const fn unicode_block(cp: u32) -> &'static str {
    match cp {
        0x0000..=0x007F => "Basic Latin",
        0x0080..=0x00FF => "Latin-1 Supplement",
        0x0100..=0x017F => "Latin Extended-A",
        0x0180..=0x024F => "Latin Extended-B",
        0x0250..=0x02AF => "IPA Extensions",
        0x02B0..=0x02FF => "Spacing Modifiers",
        0x0300..=0x036F => "Combining Diacriticals",
        0x0370..=0x03FF => "Greek and Coptic",
        0x0400..=0x04FF => "Cyrillic",
        0x0500..=0x052F => "Cyrillic Supplement",
        0x0530..=0x058F => "Armenian",
        0x0590..=0x05FF => "Hebrew",
        0x0600..=0x06FF => "Arabic",
        0x0700..=0x074F => "Syriac",
        0x0900..=0x097F => "Devanagari",
        0x10A0..=0x10FF => "Georgian",
        0x13A0..=0x13FF => "Cherokee",
        0x1C80..=0x1C8F => "Cyrillic Extended-C",
        0x1C90..=0x1CBF => "Georgian Extended",
        0x1D00..=0x1D7F => "Phonetic Extensions",
        0x1E00..=0x1EFF => "Latin Extended Additional",
        0x1F00..=0x1FFF => "Greek Extended",
        0x2100..=0x214F => "Letterlike Symbols",
        0x2160..=0x218F => "Number Forms",
        0x24B0..=0x24E9 => "Enclosed Alphanumerics",
        0x2C00..=0x2C5F => "Glagolitic",
        0x2C60..=0x2C7F => "Latin Extended-C",
        0x2C80..=0x2CFF => "Coptic",
        0x2D00..=0x2D2F => "Georgian Supplement",
        0xA640..=0xA69F => "Cyrillic Extended-B",
        0xA720..=0xA7FF => "Latin Extended-D",
        0xAB30..=0xAB6F => "Latin Extended-E",
        0xAB70..=0xABBF => "Cherokee Supplement",
        0xFB00..=0xFB4F => "Alphabetic Presentation Forms",
        0xFF00..=0xFFEF => "Halfwidth and Fullwidth",
        0x10400..=0x1044F => "Deseret",
        0x104B0..=0x104FF => "Osage",
        0x10500..=0x1052F => "Elbasan",
        0x10C80..=0x10CFF => "Old Hungarian",
        0x118A0..=0x118FF => "Warang Citi",
        0x16E40..=0x16E9F => "Medefaidrin",
        0x1E900..=0x1E95F => "Adlam",
        _ => "Other",
    }
}
