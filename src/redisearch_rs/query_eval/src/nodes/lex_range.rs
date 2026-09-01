/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Evaluation of `QN_LEXRANGE` query nodes over a TEXT field.
//!
//! A tag-field lex range never reaches here: `Query_EvalTagNode` walks the tag
//! index's own value trie and dispatches its children itself.

use std::ops::{Bound, ControlFlow};

use c_trie::{QueryRequestTimeoutHandle, TermsTrie};
use query_error::QueryErrorCode;
use query_types::QueryNodeType;
use rqe_iterators::union_opaque::build_union;
use string_utils::runes::runes_to_bytes;

use crate::{
    Config, Evaluated, QueryEvalContext, QueryNodeRef, expansion::Expansion,
    expansion_needs_offsets,
};

/// The markers the indexer prefixes a derived term with: a stem, a phonetic
/// form, or a synonym id.
///
/// They share the terms trie with the terms a document actually holds, and
/// bracket the letters (`+` and `<` below, `~` above), so a range that does not
/// exclude them sweeps them in: a document whose stem falls inside the interval
/// did not put that text in the field. A term the tokenizer produced never
/// starts with one, since all three are separators; `entryWantsSuffixTrie`
/// relies on the same property.
///
/// [`allowed_subranges`] turns this into the walk's bounds.
const DERIVED_TERM_MARKERS: [ffi::rune; 3] = [
    ffi::STEM_PREFIX as ffi::rune,
    ffi::PHONETIC_PREFIX as ffi::rune,
    ffi::SYNONYM_PREFIX_CHAR as ffi::rune,
];

/// One side of a lex range, lowered to the runes the terms trie is keyed by.
#[derive(Clone)]
struct LoweredBound {
    /// `None` for an unbounded side.
    runes: Option<Vec<ffi::rune>>,
    /// Whether the bound itself is in range.
    inclusive: bool,
}

impl LoweredBound {
    /// Whether the empty term is at or above this bound, read as a range minimum.
    const fn min_covers_empty(&self) -> bool {
        match &self.runes {
            None => true,
            Some(b) => b.is_empty() && self.inclusive,
        }
    }

    /// Whether the empty term is at or below this bound, read as a range maximum.
    /// Every non-empty bound is above it.
    const fn max_covers_empty(&self) -> bool {
        match &self.runes {
            None => true,
            Some(b) => !b.is_empty() || self.inclusive,
        }
    }
}

/// One segment of the key space, as an inclusive start and an exclusive end.
/// `None` on either side is unbounded.
type Segment = (Option<Vec<ffi::rune>>, Option<Vec<ffi::rune>>);

/// The sub-ranges of `[min, max]` that hold no derived term, in ascending order.
///
/// A derived term's key starts with one of [`DERIVED_TERM_MARKERS`], so each
/// marker removes the half-open slice `[marker, marker + 1)` from the key space.
/// Walking the complement rather than filtering in the callback is what keeps a
/// range whose bounds straddle a marker namespace from descending into it: on a
/// stemmed index `@t:<("0")` spans every `+`-prefixed stem, and filtering would
/// visit them all to produce nothing.
fn allowed_subranges(min: &LoweredBound, max: &LoweredBound) -> Vec<(LoweredBound, LoweredBound)> {
    let mut markers = DERIVED_TERM_MARKERS;
    markers.sort_unstable();

    // The complement of the marker slices, as (inclusive start, exclusive end)
    // with `None` unbounded.
    let mut segments: Vec<Segment> = Vec::new();
    let mut start: Option<Vec<ffi::rune>> = None;
    for marker in markers {
        segments.push((start, Some(vec![marker])));
        start = Some(vec![marker + 1]);
    }
    segments.push((start, None));

    segments
        .into_iter()
        .filter_map(|(seg_start, seg_end)| {
            let lo = tighter_min(min, seg_start.as_deref());
            let hi = tighter_max(max, seg_end.as_deref());
            (!is_empty(&lo, &hi)).then_some((lo, hi))
        })
        .collect()
}

/// The more restrictive of a requested minimum and a segment's inclusive start.
fn tighter_min(requested: &LoweredBound, segment: Option<&[ffi::rune]>) -> LoweredBound {
    let Some(segment) = segment else {
        return requested.clone();
    };
    match &requested.runes {
        Some(r) if r.as_slice() >= segment => requested.clone(),
        _ => LoweredBound {
            runes: Some(segment.to_vec()),
            inclusive: true,
        },
    }
}

/// The more restrictive of a requested maximum and a segment's exclusive end.
fn tighter_max(requested: &LoweredBound, segment: Option<&[ffi::rune]>) -> LoweredBound {
    let Some(segment) = segment else {
        return requested.clone();
    };
    match &requested.runes {
        Some(r) if r.as_slice() < segment => requested.clone(),
        _ => LoweredBound {
            runes: Some(segment.to_vec()),
            inclusive: false,
        },
    }
}

/// Whether a range holds no key at all.
fn is_empty(min: &LoweredBound, max: &LoweredBound) -> bool {
    let (Some(lo), Some(hi)) = (&min.runes, &max.runes) else {
        return false;
    };
    match lo.cmp(hi) {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Equal => !(min.inclusive && max.inclusive),
        std::cmp::Ordering::Less => false,
    }
}

/// Lower one bound to the runes a term is keyed by.
///
/// TEXT terms are indexed lowercased, so a bound is lowercased too, and decoded
/// unvalidated for the same reason a prefix pattern is: a term's bytes were
/// decoded the same way when indexed.
///
/// Returns `None`, after recording the error, for a bound longer than the trie's
/// maximum rune-string length. Such a bound cannot be compared against any
/// stored term, so answering the query would mean ignoring half of it.
fn lower_bound(ctx: &mut QueryEvalContext, bound: Bound<&[u8]>) -> Option<LoweredBound> {
    let (bytes, inclusive) = match bound {
        Bound::Unbounded => {
            return Some(LoweredBound {
                runes: None,
                inclusive: false,
            });
        }
        Bound::Included(s) => (s, true),
        Bound::Excluded(s) => (s, false),
    };

    let Some(runes) = rs_token::bytes_to_lower_runes(bytes) else {
        ctx.status().set_error(
            QueryErrorCode::Limit,
            &format!(
                "LEXRANGE query string is too long. Maximum allowed length is {}",
                ffi::MAX_RUNE_STR_LEN
            ),
        );
        return None;
    };

    Some(LoweredBound {
        runes: Some(runes),
        inclusive,
    })
}

/// `QN_LEXRANGE`: expand every term of a TEXT field between the node's bounds
/// into a union of per-term readers.
///
/// Capped by [`Config::max_prefix_expansions`], which a range shares with the
/// other expanding node types.
pub(crate) fn eval<'index>(
    ctx: &'index mut QueryEvalContext,
    node: &QueryNodeRef,
    begin: Bound<&[u8]>,
    end: Bound<&[u8]>,
    config: Config,
) -> Option<Evaluated<'index>> {
    let min = lower_bound(ctx, begin)?;
    let max = lower_bound(ctx, end)?;

    let weight = node.opts().weight;
    let field_mask = node.opts().field_mask & ctx.opts().fieldmask;
    let is_disk = !ctx.spec().diskSpec.is_null();
    let needs_offsets = expansion_needs_offsets(ctx, node.opts(), config);

    // SAFETY: the reference is confined to the walk below, which the query, and
    // so the spec owning the trie, outlives.
    let terms: &TermsTrie = unsafe { ctx.terms_trie() };
    // Read here, with everything else off `ctx`, because `Expansion` borrows it
    // mutably for the rest of the expansion.
    // SAFETY: the request timeout outlives query evaluation. Its source is fixed
    // for this execution cycle; only the blocked-client flag may change
    // concurrently, through the C atomic API.
    let timeout = unsafe { QueryRequestTimeoutHandle::from_raw(ctx.sctx().timeout) };

    let subranges = allowed_subranges(&min, &max);

    let mut expansion = Expansion {
        ctx,
        children: Vec::new(),
        field_mask,
        is_disk,
        needs_offsets,
        max_expansions: config.max_prefix_expansions,
    };

    // The cap is consulted before the key is rebuilt, not left to `push_child`:
    // the bound recursion ignores the stop request, so every remaining term
    // would otherwise pay for a reconstruction that is thrown away.
    // The empty term sorts below every other, so it is admitted first and through
    // the capped path: it is one of the terms in the range, not an extra. A
    // zero-length key is refused on insertion, so an indexed empty value lives
    // only in its own inverted index and no walk reaches it; open that index
    // directly, as the wildcard and fuzzy expansions do. A field without
    // `INDEXEMPTY` has no such index, so this adds nothing.
    if min.min_covers_empty() && max.max_covers_empty() {
        let _ = expansion.push_child(0, b"");
    }

    let mut on_runes = |runes: &[ffi::rune], num_docs: usize| {
        if expansion.cap_reached() {
            return ControlFlow::Break(());
        }
        // Runes must be re-encoded into the term's key byte for byte as the index
        // stored it: WTF-8 rather than UTF-8 where a rune is a lone surrogate.
        let Ok(key) = runes_to_bytes(runes) else {
            return ControlFlow::Continue(());
        };
        expansion.push_child(num_docs, &key)
    };
    // One walk per sub-range that holds no derived term, so none is traversed.
    for (lo, hi) in subranges {
        terms.iterate_range(
            lo.runes.as_deref(),
            lo.inclusive,
            hi.runes.as_deref(),
            hi.inclusive,
            timeout,
            &mut on_runes,
        );
    }

    // Quick-exit like the other expansions: only the matching id set is needed.
    // No profiling query string, since the bounds are two strings rather than the
    // single pattern `q_str` names.
    let iter = build_union(
        expansion.children,
        true,
        config.min_union_iter_heap,
        QueryNodeType::LexRange,
        weight,
    );
    Some(Evaluated::RustCompound(iter))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bound(runes: Option<&str>, inclusive: bool) -> LoweredBound {
        LoweredBound {
            runes: runes.map(|s| s.encode_utf16().collect()),
            inclusive,
        }
    }

    /// `(start, inclusive, end, inclusive)` of each sub-range, as strings.
    fn split(
        min: LoweredBound,
        max: LoweredBound,
    ) -> Vec<(Option<String>, bool, Option<String>, bool)> {
        allowed_subranges(&min, &max)
            .into_iter()
            .map(|(lo, hi)| {
                let render =
                    |b: &LoweredBound| b.runes.as_ref().map(|r| String::from_utf16_lossy(r));
                (render(&lo), lo.inclusive, render(&hi), hi.inclusive)
            })
            .collect()
    }

    /// An unbounded range becomes the four segments the three markers leave.
    #[test]
    fn unbounded_range_excludes_every_marker() {
        let got = split(bound(None, false), bound(None, false));
        assert_eq!(
            got,
            vec![
                (None, false, Some("+".to_owned()), false),
                (Some(",".to_owned()), true, Some("<".to_owned()), false),
                (Some("=".to_owned()), true, Some("~".to_owned()), false),
                (Some("\u{7f}".to_owned()), true, None, false),
            ]
        );
    }

    /// A range wholly between two markers is left alone.
    #[test]
    fn range_clear_of_the_markers_is_unchanged() {
        let got = split(bound(Some("apple"), false), bound(Some("banana"), true));
        assert_eq!(
            got,
            vec![(
                Some("apple".to_owned()),
                false,
                Some("banana".to_owned()),
                true
            )]
        );
    }

    /// The case the walk used to scan in full. `@t:<("0")` spans the whole stem
    /// namespace, which is now cut out, leaving only the two slivers around it:
    /// everything below `+`, which holds at most the empty term, and everything
    /// from `,` up to the bound. Neither can hold a stem, so the walk no longer
    /// descends into one.
    #[test]
    fn range_over_a_marker_namespace_keeps_only_the_slivers_around_it() {
        let got = split(bound(None, false), bound(Some("0"), false));
        assert_eq!(
            got,
            vec![
                (None, false, Some("+".to_owned()), false),
                (Some(",".to_owned()), true, Some("0".to_owned()), false),
            ]
        );
    }

    /// A range straddling a marker keeps the parts on either side of it.
    #[test]
    fn range_straddling_a_marker_is_cut_around_it() {
        let got = split(bound(Some("*"), true), bound(Some("0"), true));
        assert_eq!(
            got,
            vec![
                (Some("*".to_owned()), true, Some("+".to_owned()), false),
                (Some(",".to_owned()), true, Some("0".to_owned()), true),
            ]
        );
    }
}
