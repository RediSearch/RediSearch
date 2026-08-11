/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Evaluation of `QN_FUZZY` query nodes.

use std::str;

use query_types::QueryNodeType;
use rqe_iterators::union_opaque::build_union_with_q_str;
use rs_token::RSTokenRefNulTerminated;
use term_dictionary::TermDictionary;

use crate::{
    Config, Evaluated, QueryEvalContext, QueryNodeRef, expansion::Expansion,
    expansion_needs_offsets,
};

/// The `apiVersion` from which a fuzzy expansion also covers the empty term.
/// Below it the empty term is never expanded to, whatever the distance.
const EMPTY_TERM_API_VERSION: u32 = 2;

/// `QN_FUZZY` — expand a token to every term within `max_dist` edits of it over
/// the spec's terms dictionary, and union the per-term readers.
///
/// The distance is a Levenshtein distance counted in codepoints, and the pattern
/// is case-folded before the walk, since the dictionary stores folded keys.
///
/// Always yields a union, empty if no term is within the distance, and never
/// sets an error: a fuzzy query that names nothing is well-formed and simply
/// matches nothing, unlike an over-long prefix pattern. The number of
/// expansions is capped by the configured [`Config::max_prefix_expansions`],
/// with the empty term below exempt from it.
///
/// # Panics
///
/// Panics if `max_dist` is negative. That is a caller bug rather than a query
/// the grammar can express — it spells the distance as the number of `%`
/// delimiters and has productions for only one, two and three — and no
/// expansion the node could stand for is known at that point, so there is no
/// result to return in its place.
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
    // The dictionary counts the distance in codepoints, so it takes it unsigned;
    // converted before anything compares against it, so no gate below can be
    // reached with a negative distance reinterpreted as a huge one.
    let max_dist = u32::try_from(max_dist).expect("fuzzy distance must not be negative");
    // SAFETY: the reference is confined to this evaluation — the walk below and
    // the empty-term lookup after it — which the query, and so the spec owning
    // the dictionary, outlives.
    let terms = unsafe { ctx.terms_dict() };

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

    // A token is a byte string and nothing between the query text and here
    // validates its encoding, but indexing refuses a term whose bytes are not
    // valid UTF-8, so a pattern that is not valid UTF-8 names no stored term:
    // the walk matches nothing rather than erroring. The empty term below is
    // still reached, since it is a matter of the pattern's length alone.
    if let Ok(pattern) = str::from_utf8(pattern) {
        expansion.expand_via_fuzzy_walk(terms, pattern, max_dist);
    }

    // A token no longer than the distance could be deleted entirely and still be
    // within budget, so it also matches the empty term. It is pushed
    // unconditionally of what the walk found, and exempt from the expansion cap,
    // so that a query whose expansions filled the cap still reaches the empty
    // term's documents.
    //
    // The gate measures the token in bytes even though the distance counts
    // codepoints, so a single multibyte character does not reach the empty term
    // at distance 1.
    if api_version >= EMPTY_TERM_API_VERSION && pattern.len() <= max_dist as usize {
        // An indexed empty text value does register in the dictionary, so the
        // count the disk path scores the term by is looked up rather than
        // assumed; a spec that never indexed one answers zero.
        let num_docs = terms.get("").map_or(0, |entry| entry.num_docs);
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
    /// Walk the primary terms dictionary for every term within `max_dist` edits
    /// of `pattern`, accumulating one reader per term.
    ///
    /// `terms` is the spec's primary terms dictionary and `pattern` is the raw
    /// token text, which the dictionary case-folds itself.
    ///
    /// The walk carries no deadline and cannot be interrupted — matching the
    /// behaviour it replaces, and unlike the prefix walk, which threads a
    /// timeout through. The edit-distance filter prunes the walk but is no bound
    /// on the work: as `max_dist` approaches the pattern's codepoint length the
    /// automaton accepts an ever larger share of the dictionary. The only thing
    /// that cuts the walk short is the expansion cap, via
    /// [`push_child`](Self::push_child) breaking — and that bounds the readers
    /// opened, not the terms visited, since a term with no inverted index
    /// consumes no cap slot.
    ///
    /// The empty term is never among the accumulated readers, whatever the
    /// distance: it is expanded to separately, under the API-version gate its
    /// caller applies.
    fn expand_via_fuzzy_walk(&mut self, terms: &TermDictionary, pattern: &str, max_dist: u32) {
        // The dictionary hands each match back as the term's stored key, with the
        // document count used for the disk IDF.
        for (term, entry) in terms.fuzzy_iter(pattern, max_dist) {
            // An indexed empty text value registers as the empty term, which the
            // automaton accepts from any pattern no longer than the distance —
            // the same condition the caller's API-gated expansion covers. Left in
            // here it would reach a query whose API version excludes it, and
            // spend a cap slot the gated expansion is deliberately exempt from.
            if term.is_empty() {
                continue;
            }
            if self.push_child(entry.num_docs, term.as_bytes()).is_break() {
                break;
            }
        }
    }
}
