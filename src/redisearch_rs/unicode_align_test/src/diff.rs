/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Side-by-side libnu-vs-ICU comparison machinery.
//!
//! Tests under `tests/diff_*.rs` consume these helpers to characterise
//! where libnu's case-folding / lowercase tables disagree with ICU. The
//! comparison is *reporting*, not gating: differences are expected (full
//! Unicode case folding for ß, final sigma, Turkish I, ...), and the
//! tests print divergence tables rather than assert their absence. For
//! gating libnu self-checks see [`crate::checks`].
//!
//! Note: libnu's `nu_tolower` is unconditional (no context-sensitivity),
//! while ICU's `lowercase_to_string` at root locale *is* context-sensitive
//! for Greek final sigma. The lowercase comparison therefore surfaces both
//! table-content divergences and the structural gap that libnu's
//! `_nu_tolower` variant would close but which RediSearch does not
//! currently call.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;

use icu_casemap::CaseMapper;
use icu_locale_core::LanguageIdentifier;
use rayon::prelude::*;

use crate::checks::{fold_libnu, lower_libnu};

/// Fold `s` using ICU4X's full Unicode case folding (`icu_casemap::CaseMapper`).
///
/// This mirrors `try_insert_string_normalize()` at
/// `src/redisearch_rs/sorting_vector/src/lib.rs:131-141`.
pub fn fold_icu(s: &str) -> String {
    CaseMapper::new().fold_string(s).into_owned()
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
/// Mirrors [`compare`] but uses libnu / ICU lowercase instead of the fold
/// variants. This is the comparison that matches production text
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
    /// each worker accumulates into a thread-local [`Report`] and the reduce
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
        let _ = writeln!(out, "=== unicode_align_test report ===");
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
