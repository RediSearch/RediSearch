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
//! index's own value trie instead, and dispatches its children itself.

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

/// The markers the indexer prefixes a *derived* term with: a stem, a phonetic
/// form, or a synonym id. They live in the same terms trie as the terms a
/// document actually contains, and a range must skip them - a document whose
/// stem happens to sort inside the range did not put that text in the field, and
/// the marker characters bracket the letters (`+` and `<` below them, `~` above)
/// so an unfiltered range on either side sweeps them all in.
///
/// A term the tokenizer produced can never start with one of these: all three
/// are separators, so they never survive into a token. `entryWantsSuffixTrie`
/// relies on the same property.
const DERIVED_TERM_MARKERS: [ffi::rune; 3] = [
    ffi::STEM_PREFIX as ffi::rune,
    ffi::PHONETIC_PREFIX as ffi::rune,
    ffi::SYNONYM_PREFIX_CHAR as ffi::rune,
];

/// One side of a lex range, lowered to the runes the terms trie is keyed by.
struct LoweredBound {
    /// `None` for an unbounded side.
    runes: Option<Vec<ffi::rune>>,
    /// Whether the bound itself is part of the range. Meaningless, but harmless,
    /// when the side is unbounded — the C walk ignores it there.
    inclusive: bool,
}

/// Lower one bound to the runes a term is keyed by, or report that it cannot be
/// one.
///
/// TEXT terms are indexed lowercased, so a bound is lowercased too — otherwise
/// `@name:>(Bob)` would compare an uppercase bound against lowercase keys and
/// answer a different range than `@name:>(bob)`.
///
/// Returns `None` — after recording the error — when the bound is longer than
/// the trie's maximum rune-string length. A bound that long cannot be compared
/// against any stored term, so answering the query would mean silently ignoring
/// half of it.
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

    // The bound is a byte string that need not be valid UTF-8, and is decoded
    // unvalidated for the same reason a prefix pattern is: a term's bytes were
    // decoded the same way when indexed, so a client that stores and queries
    // text in one non-UTF-8 encoding compares against the keys it created.
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

/// `QN_LEXRANGE` — expand every term of a TEXT field that falls between the
/// node's bounds into a union of per-term readers.
///
/// The number of expansions is capped by the configured
/// [`Config::max_prefix_expansions`], which a range shares with the other
/// expanding node types: it bounds how many readers one query node may open,
/// whatever produced the terms.
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

    // SAFETY: the reference is confined to the walk below, which the query — and
    // so the spec owning the trie — outlives.
    let terms: &TermsTrie = unsafe { ctx.terms_trie() };
    // Resolved here, with every other read of `ctx`, because `Expansion` borrows
    // it mutably for the rest of the expansion.
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

    // The walk hands terms back as runes (with their document count, used for
    // the disk IDF), which must be encoded back into the term's key byte for
    // byte as the index stored it — WTF-8 rather than UTF-8 where a rune is a
    // lone surrogate.
    //
    // The walk cannot be stopped once the cap is reached, so the cap is checked
    // before the key is rebuilt rather than left to `push_child`: otherwise every
    // remaining term in the range would pay for a reconstruction that is thrown
    // away.
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
        let Ok(key) = runes_to_bytes(runes) else {
            // The term key cannot be reconstructed; skip this expansion.
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

    // A range union only needs the matching id set, never per-child scores, so
    // it takes the quick-exit path like the other expansions. It carries no
    // profiling query string: the bounds are two strings, not the single pattern
    // `q_str` names.
    let iter = build_union(
        expansion.children,
        true,
        config.min_union_iter_heap,
        QueryNodeType::LexRange,
        weight,
    );
    Some(Evaluated::RustCompound(iter))
}
