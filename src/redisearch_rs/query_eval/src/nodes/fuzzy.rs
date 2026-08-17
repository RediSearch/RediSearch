/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Evaluation of `QN_FUZZY` query nodes.

use std::ops::ControlFlow;

use c_trie::{FuzzyWalk, InvalidFuzzyDistance, TermsTrie};
use query_types::QueryNodeType;
use rqe_iterators::union_opaque::build_union_with_q_str;
use rs_token::RSTokenRefNulTerminated;
use string_utils::runes::runes_to_bytes;

use crate::{
    Config, Evaluated, QueryEvalContext, QueryNodeRef, expansion::Expansion,
    expansion_needs_offsets,
};

/// The `apiVersion` from which a fuzzy expansion also covers the empty term.
/// Below it the empty term is never expanded to, whatever the distance.
const EMPTY_TERM_API_VERSION: u32 = 2;

/// `QN_FUZZY` — expand a token to every term within `max_dist` edits of it over
/// the spec's terms trie, and union the per-term readers.
///
/// The distance is a Levenshtein distance counted in runes, and the pattern is
/// lowercased before the walk, since the trie stores folded keys.
///
/// Returns `None`, and sets no error, when the pattern is too long for the trie
/// to walk: a fuzzy query that names nothing is well-formed and simply matches
/// nothing, unlike an over-long prefix pattern, which is reported as an error.
/// Every other outcome yields a union, empty if no term is within the distance.
/// The number of expansions is capped by the configured
/// [`Config::max_prefix_expansions`], with the empty term below exempt from it.
///
/// # Panics
///
/// Panics if the walk refuses `max_dist` with [`InvalidFuzzyDistance`]. That is
/// a caller bug rather than a query the grammar can express — it spells the
/// distance as the number of `%` delimiters and has productions for only one,
/// two and three — and no expansion the node could stand for is known at that
/// point, so there is no result to return in its place.
pub(crate) fn eval<'index>(
    ctx: &'index mut QueryEvalContext,
    node: &QueryNodeRef,
    tok: RSTokenRefNulTerminated,
    max_dist: i32,
    config: Config,
) -> Option<Evaluated<'index>> {
    // The reader field mask narrows the node's mask to the query-wide one.
    let node_field_mask = node.opts().field_mask;
    let weight = node.opts().weight;
    let field_mask = node_field_mask & ctx.opts().fieldmask;
    let is_disk = !ctx.spec().diskSpec.is_null();
    let needs_offsets = expansion_needs_offsets(ctx, node.opts(), config);
    // Read with every other use of `ctx`, because `Expansion` borrows it mutably
    // for the rest of the expansion.
    let api_version = ctx.sctx().apiVersion;
    let terms_trie = ctx.spec().terms;

    debug_assert!(!terms_trie.is_null(), "terms trie should be initialized");
    // SAFETY: `terms_trie` is the spec's terms `Trie`, valid for and unmutated
    // during the query (`QueryEvalContext` invariants 1/2).
    let terms = unsafe { TermsTrie::from_raw(terms_trie) };

    let q_str = tok.as_c_str().expect("fuzzy token must carry a string");
    let pattern = q_str.to_bytes();

    let mut expansion = Expansion {
        ctx,
        children: Vec::new(),
        field_mask,
        is_disk,
        needs_offsets,
        max_expansions: config.max_prefix_expansions,
    };

    match expansion.expand_via_fuzzy_walk(terms, pattern, max_dist) {
        Ok(FuzzyWalk::Walked) => {}
        // A pattern the trie declines was never walked, so nothing is known
        // about what the index holds within the distance: yield no iterator at
        // all, rather than the empty union a walk that found nothing produces.
        Ok(FuzzyWalk::PatternRejected) => return None,
        // The distance reaches C as the size of a stack allocation, so the walk
        // bounds it rather than trusting it the whole way down. Nothing a query
        // can express is refused here, and the node builder asserts the range
        // too, so a refusal means the AST was built by something that does not
        // respect the grammar's distances.
        Err(err) => panic!("{err}"),
    }

    // A token no longer than the distance could be deleted entirely and still be
    // within budget, so it also matches the empty term — which no walk can find,
    // since a zero-length key is refused on insertion and an indexed empty value
    // exists only as an inverted index.
    //
    // The gate measures the token in bytes even though the distance counts
    // runes, so a single multibyte character does not reach the empty term at
    // distance 1.
    if api_version >= EMPTY_TERM_API_VERSION && pattern.len() <= max_dist as usize {
        // Structurally zero: the trie cannot hold a zero-length key, so the
        // lookup has nothing to find. Only the disk path consumes the count, for
        // the term's IDF, so the in-memory path does not pay for it.
        let num_docs = if is_disk { terms.num_docs(b"") } else { 0 };
        expansion.push_child_ignoring_cap(num_docs, b"");
    }

    let children = expansion.children;

    // Fuzzy unions always take the quick-exit path — they only need the matching
    // id set, never per-child scores — and carry the pattern token as their
    // profiling query string.
    //
    // SAFETY: the union iterator retains the `CStr`, escaping the handle's
    // borrow, so the token's string must stay put for as long as the iterator
    // does. A `QN_FUZZY` token is written once when the node is built (or when
    // its query parameter is resolved) and never rewritten afterwards — the one
    // evaluator that rewrites a token in place belongs to the tag expansion,
    // which is dispatched separately and never re-enters this one. Both the
    // token and the iterator are owned by the query AST, so the string also
    // outlives the iterator.
    let iter = unsafe {
        build_union_with_q_str(
            children,
            true,
            config.min_union_iter_heap,
            QueryNodeType::Fuzzy,
            q_str,
            weight,
        )
    };
    Some(Evaluated::RustCompound(iter))
}

impl Expansion<'_> {
    /// Walk the primary terms trie for every term within `max_dist` edits of
    /// `pattern`, accumulating one reader per term.
    ///
    /// `terms` wraps the spec's primary terms trie and `pattern` is the raw token
    /// bytes, which the trie lowercases and decodes itself.
    ///
    /// The walk carries no deadline and cannot be interrupted — matching the
    /// behaviour it replaces, and unlike the prefix walk, which threads a
    /// timeout through. The edit-distance filter prunes the trie but is no bound
    /// on the work: as `max_dist` approaches the pattern's rune length the
    /// automaton accepts an ever larger share of it. The only thing that cuts
    /// the walk short is the expansion cap, via
    /// [`push_child`](Self::push_child) breaking — and that bounds the readers
    /// opened, not the terms visited, since a term with no inverted index
    /// consumes no cap slot.
    ///
    /// [`FuzzyWalk::PatternRejected`] is not an empty expansion — the walk never
    /// ran — so a caller must not answer it with the empty union it would build
    /// for a walk that matched nothing. The same holds of
    /// [`InvalidFuzzyDistance`], which the walk returns without visiting
    /// anything.
    fn expand_via_fuzzy_walk(
        &mut self,
        terms: &TermsTrie,
        pattern: &[u8],
        max_dist: i32,
    ) -> Result<FuzzyWalk, InvalidFuzzyDistance> {
        // The trie hands terms back as runes (with their document count, used for
        // the disk IDF), which must be encoded back into the term's key, byte for
        // byte as the index stored it — WTF-8 rather than UTF-8 where a rune is a
        // lone surrogate.
        let on_runes = |runes: &[ffi::rune], num_docs: usize| {
            let Ok(key) = runes_to_bytes(runes) else {
                // The term key cannot be reconstructed; skip this expansion.
                return ControlFlow::Continue(());
            };
            self.push_child(num_docs, &key)
        };
        // SAFETY: `terms` wraps the spec's valid primary terms `Trie`, which is
        // not mutated or re-iterated for the duration of the call: `on_runes`
        // only opens per-term readers, which read the spec's inverted indexes
        // rather than the terms trie being walked.
        unsafe { terms.iterate_fuzzy(pattern, max_dist, on_runes) }
    }
}
