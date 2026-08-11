/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Evaluation of `QN_WILDCARD_QUERY` query nodes.

use std::{ops::ControlFlow, str};

use c_trie::{LoweredPattern, TermsTrie};
use query_error::QueryErrorCode;
use query_types::QueryNodeType;
use rqe_iterators::{union_opaque::build_union_with_q_str, utils::deadline_passed};
use string_utils::runes::runes_to_bytes;
use term_suffix_index::TermSuffixIndex;

use crate::{
    Config, Evaluated, QueryEvalContext, QueryNodeMut, expansion::Expansion,
    expansion_needs_offsets,
};

/// `QN_WILDCARD_QUERY` — expand a verbatim wildcard pattern (`w'he?l*o'`, where
/// `*` matches any run of characters and `?` exactly one) over the spec's terms
/// trie into a union of per-term readers.
///
/// Unlike a prefix pattern, a wildcard pattern is not anchored: the spec's suffix
/// index is consulted whenever it exists, and the primary terms trie is walked
/// only when the suffix index has no literal run to anchor on.
///
/// An empty pattern is answered without either walk: it matches exactly the empty
/// term, whose inverted index is opened directly.
///
/// Returns `None` when the pattern is too long to name any term (reported via
/// [`status`](QueryEvalContext::status)); every other outcome — including no
/// matches and the unsupported-fields error — yields a union, possibly empty.
/// The number of expansions is capped by the configured
/// [`Config::max_prefix_expansions`].
pub(crate) fn eval<'index>(
    ctx: &'index mut QueryEvalContext,
    mut node: QueryNodeMut<'_>,
    config: Config,
) -> Option<Evaluated<'index>> {
    // Read the header first: the token handle below borrows the node exclusively
    // until the very end, where the profiling query string is read through it, so
    // nothing may consult `opts` once it is live.
    //
    // The reader field mask narrows the node's mask to the query-wide one; the
    // suffix-index support check below uses the node's own mask.
    let node_field_mask = node.opts().field_mask;
    let weight = node.opts().weight;
    let is_disk = !ctx.spec().diskSpec.is_null();
    let needs_offsets = expansion_needs_offsets(ctx, node.opts(), config);
    let field_mask = node_field_mask & ctx.opts().fieldmask;

    // Before anything reads the token: it doubles as the union's profiling query
    // string, which must be the unescaped form.
    let Some(mut tok) = node.token_mut() else {
        unreachable!("wildcard_query::eval is only reached for a wildcard-query node")
    };
    tok.remove_wildcard_escapes();

    // As for a prefix pattern, the token is *not* guaranteed to be valid UTF-8, and
    // the lowering decodes it without validating — which is what makes the pattern
    // name the same runes the index does, since a term's bytes are decoded the same
    // unvalidated way when it is indexed.
    let Some(pattern) = tok.as_ref().as_lower_runes() else {
        ctx.status().set_error(
            QueryErrorCode::Limit,
            &format!(
                "Wildcard query string is too long. Maximum allowed length is {}",
                ffi::MAX_RUNE_STR_LEN
            ),
        );
        return None;
    };
    let tok = tok.as_ref();

    let suffix_index = ctx.spec().suffix;
    let suffix_mask = ctx.spec().suffixMask;
    // SAFETY: the reference is confined to this evaluation — the suffix-trie
    // walk and the terms-trie fallback below — which the query, and so the spec
    // owning the trie, outlives.
    let terms = unsafe { ctx.terms_trie() };
    // Resolved here, with every other read of `ctx`, because `Expansion` borrows
    // it mutably for the rest of the expansion.
    let time = &ctx.sctx().time;
    let timeout = (!time.skipTimeoutChecks).then_some(time.timeout);

    let mut expansion = Expansion {
        ctx,
        children: Vec::new(),
        field_mask,
        is_disk,
        needs_offsets,
        max_expansions: config.max_prefix_expansions,
    };

    // A spec with a suffix index may only answer a pattern when every queried field
    // is covered by it. An unsupported field set is an error that does *not* fall
    // back to a walk — it yields an empty union — and it is reported for any
    // pattern, so it is decided before the empty-pattern case below.
    let fields_unsupported = !suffix_index.is_null()
        && node_field_mask != rqe_core::RS_FIELDMASK_ALL
        && (suffix_mask & node_field_mask) != node_field_mask;

    if fields_unsupported {
        expansion.ctx.status().set_error(
            QueryErrorCode::Generic,
            "Contains query on fields without WITHSUFFIXTRIE support",
        );
    } else if pattern.is_empty() {
        // A pattern that lowercases to nothing wildcard-matches exactly the empty
        // term, which neither walk below can find: a zero-length key is refused on
        // insertion, so an indexed empty value exists only as an inverted index.
        // Open that index directly, as an empty-string query would. A field without
        // `INDEXEMPTY` has no such index and the pattern matches nothing.
        //
        // The IDF count is zero even on the disk path: no zero-length key is ever
        // inserted, so the terms trie cannot supply one, and the empty term is
        // scored as one never seen whatever its inverted index holds.
        let _ = expansion.push_child(0, b"");
    } else if let Some(lowered_pattern) = LoweredPattern::new(&pattern) {
        // The suffix index is preferred whenever the spec has one; the brute-force
        // terms-trie scan is the fallback for a pattern it has no literal run to
        // anchor on.
        let mut walked = false;
        if !suffix_index.is_null() {
            // Reached only with the fields supported, ruled on above.
            // SAFETY: `suffix_index` is non-null (checked above) and is the spec's
            // `TermSuffixIndex`, built by `TermSuffixIndex_New` and held behind an
            // opaque C typedef, so the cast recovers the Rust type it was created
            // as. It is valid for and unmutated during the query
            // (`QueryEvalContext` invariants 1/2), which satisfies the index's
            // readers-writer contract.
            let suffix = unsafe { &*suffix_index.cast::<TermSuffixIndex>() };
            walked = expansion.expand_wildcard_via_suffix_index(suffix, terms, &pattern, timeout);
        }
        if !walked {
            expansion.expand_wildcard_via_terms_trie(terms, &lowered_pattern, timeout);
        }
    }
    // A pattern `LoweredPattern::new` declines falls through to an empty union.
    // Neither reason it can be declined is reachable from here — the length limit is
    // the one `as_lower_runes` already applied, and the lowering never yields a zero
    // rune — but "matches nothing" is the right answer for both anyway.

    let children = expansion.children;

    // Wildcard unions always take the quick-exit path — they only need the matching
    // id set, never per-child scores — and carry the pattern token as their
    // profiling query string.
    let q_str = tok
        .as_c_str()
        .expect("wildcard-query token must carry a string");
    // SAFETY: the union iterator retains the `CStr`, escaping the handle's borrow,
    // so the token's string must stay put for as long as the iterator does. It was
    // rewritten once above, before the iterator existed, and only shortened in
    // place — the pointer never moved — and nothing rewrites it afterwards: the tag
    // wildcard path, which also unescapes its token, is dispatched separately and
    // never re-enters this evaluator. Both the token and the iterator are owned by
    // the query AST, so the string also outlives the iterator.
    let iter = unsafe {
        build_union_with_q_str(
            children,
            true,
            config.min_union_iter_heap,
            QueryNodeType::WildcardQuery,
            q_str,
            weight,
        )
    };
    Some(Evaluated::RustCompound(iter))
}

impl Expansion<'_> {
    /// Expand a wildcard `pattern` through the spec's suffix index, accumulating one
    /// reader per matching term.
    ///
    /// `suffix` is the spec's suffix index and `terms` wraps its primary terms trie.
    /// Reports whether the index answered the pattern: `false` leaves it for
    /// [`expand_wildcard_via_terms_trie`](Self::expand_wildcard_via_terms_trie),
    /// either because no token in the pattern can anchor the scan or because the
    /// pattern names runes no indexed term can carry.
    ///
    /// `timeout` bounds the scan (`None` runs it to completion). The scan gathers
    /// its whole candidate set before reporting any term, so the deadline is polled
    /// inside the index rather than by the per-term accumulation here.
    ///
    /// Terms may repeat: one term can sit under several matching suffix keys, and
    /// each occurrence opens its own reader and counts against the expansion cap.
    /// The union deduplicates by document, so this affects cost and the cap, not
    /// results.
    fn expand_wildcard_via_suffix_index(
        &mut self,
        suffix: &TermSuffixIndex,
        terms: &TermsTrie,
        pattern: &[ffi::rune],
        timeout: Option<ffi::timespec>,
    ) -> bool {
        // The suffix index is keyed by `str`, so the pattern goes back to the term's
        // key bytes and from there to UTF-8.
        let Ok(needle) = runes_to_bytes(pattern) else {
            return false;
        };
        let Ok(needle) = str::from_utf8(&needle) else {
            // A pattern that is not valid UTF-8 encodes a lone surrogate, which no
            // indexed term can carry — the index refuses such a term on the way in.
            // Matching nothing is the answer, and the terms-trie scan would only
            // reach the same one the slow way.
            return true;
        };
        let should_stop = || match timeout {
            Some(deadline) => deadline_passed(deadline),
            None => false,
        };
        let Some(matches) = suffix.iter_wildcard(needle, should_stop) else {
            return false;
        };
        // The suffix index hands terms back already as stored key bytes but carries
        // no document count, so on the disk path the term's count is looked up in
        // the primary terms trie for the IDF; the in-memory path ignores it.
        for term in matches {
            let num_docs = if self.is_disk {
                terms.num_docs(term.as_bytes())
            } else {
                0
            };
            if self.push_child(num_docs, term.as_bytes()).is_break() {
                break;
            }
        }
        true
    }

    /// Brute-force expand a wildcard `pattern` over the primary terms trie,
    /// accumulating one reader per matching term.
    ///
    /// `terms` wraps the spec's primary terms trie and `timeout` bounds the walk
    /// (`None` runs it to completion).
    fn expand_wildcard_via_terms_trie(
        &mut self,
        terms: &TermsTrie,
        pattern: &LoweredPattern,
        timeout: Option<ffi::timespec>,
    ) {
        // The primary trie hands terms back as runes (with their document count,
        // used for the disk IDF), which must be encoded back into the term's key,
        // byte for byte as the index stored it — WTF-8 rather than UTF-8 where a
        // rune is a lone surrogate.
        let on_runes = |runes: &[ffi::rune], num_docs: usize| {
            // The walk may keep calling back after a `Break`, so check the cap
            // before reconstructing the key: otherwise each such call pays for an
            // allocation and an encode whose result is about to be discarded.
            if self.cap_reached() {
                return ControlFlow::Break(());
            }
            let Ok(key) = runes_to_bytes(runes) else {
                // The term key cannot be reconstructed; skip this expansion.
                return ControlFlow::Continue(());
            };
            self.push_child(num_docs, &key)
        };
        terms.iterate_wildcard(pattern, timeout, on_runes);
    }
}
