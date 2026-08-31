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

use std::{
    ffi::CStr,
    ops::{Bound, ControlFlow},
};

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
/// bracket the letters (`+` and `<` below, `~` above), so an unfiltered range on
/// either side sweeps them in. A term the tokenizer produced never starts with
/// one; all three are separators. `entryWantsSuffixTrie` relies on the same
/// property.
const DERIVED_TERM_MARKERS: [ffi::rune; 3] = [
    ffi::STEM_PREFIX as ffi::rune,
    ffi::PHONETIC_PREFIX as ffi::rune,
    ffi::SYNONYM_PREFIX_CHAR as ffi::rune,
];

/// One side of a lex range, lowered to the runes the terms trie is keyed by.
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

/// Lower one bound to the runes a term is keyed by.
///
/// TEXT terms are indexed lowercased, so a bound is lowercased too, and decoded
/// unvalidated for the same reason a prefix pattern is: a term's bytes were
/// decoded the same way when indexed.
///
/// Returns `None`, after recording the error, for a bound longer than the trie's
/// maximum rune-string length. Such a bound cannot be compared against any
/// stored term, so answering the query would mean ignoring half of it.
fn lower_bound(ctx: &mut QueryEvalContext, bound: Bound<&CStr>) -> Option<LoweredBound> {
    let (bytes, inclusive) = match bound {
        Bound::Unbounded => {
            return Some(LoweredBound {
                runes: None,
                inclusive: false,
            });
        }
        Bound::Included(s) => (s.to_bytes(), true),
        Bound::Excluded(s) => (s.to_bytes(), false),
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
    begin: Bound<&CStr>,
    end: Bound<&CStr>,
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
    let on_runes = |runes: &[ffi::rune], num_docs: usize| {
        if expansion.cap_reached() {
            return ControlFlow::Break(());
        }
        if runes
            .first()
            .is_some_and(|r| DERIVED_TERM_MARKERS.contains(r))
        {
            return ControlFlow::Continue(());
        }
        // Runes must be re-encoded into the term's key byte for byte as the index
        // stored it: WTF-8 rather than UTF-8 where a rune is a lone surrogate.
        let Ok(key) = runes_to_bytes(runes) else {
            return ControlFlow::Continue(());
        };
        expansion.push_child(num_docs, &key)
    };
    terms.iterate_range(
        min.runes.as_deref(),
        min.inclusive,
        max.runes.as_deref(),
        max.inclusive,
        timeout,
        on_runes,
    );

    // A zero-length key is refused on insertion, so an indexed empty value lives
    // only in its own inverted index and no walk reaches it. Open that index
    // directly when the range covers the empty term, as the wildcard and fuzzy
    // expansions do. A field without `INDEXEMPTY` has no such index, so this
    // adds nothing.
    if min.min_covers_empty() && max.max_covers_empty() {
        expansion.push_child_ignoring_cap(0, b"");
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
