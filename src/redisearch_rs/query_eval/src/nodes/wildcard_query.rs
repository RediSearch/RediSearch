/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Evaluation of `QN_WILDCARD_QUERY` query nodes.

use std::{ops::ControlFlow, ptr::NonNull};

use c_trie::{CTrieRef, LoweredPattern, SuffixWalk};
use query_error::QueryErrorCode;
use query_types::QueryNodeType;
use rqe_iterators::union_opaque::build_union_with_q_str;
use string_utils::runes::runes_to_bytes;

use crate::{
    Config, Evaluated, QueryEvalContext, QueryNodeMut, expansion::Expansion,
    expansion_needs_offsets,
};

/// `QN_WILDCARD_QUERY` — expand a verbatim wildcard pattern (`w'he?l*o'`, where
/// `*` matches any run of characters and `?` exactly one) over the spec's terms
/// trie into a union of per-term readers.
///
/// Unlike a prefix pattern, a wildcard pattern is not anchored: the spec's suffix
/// trie is consulted whenever it exists, and the primary terms trie is walked
/// only when the suffix trie has no literal run to anchor on.
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
    // suffix-trie support check below uses the node's own mask.
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

    let suffix_trie = ctx.spec().suffix;
    let suffix_mask = ctx.spec().suffixMask;
    let terms_trie = ctx.spec().terms;
    // Resolved here, with every other read of `ctx`, because `Expansion` borrows
    // it mutably for the rest of the expansion.
    let time = &ctx.sctx().time;
    let timeout = (!time.skipTimeoutChecks).then(|| NonNull::from(&time.timeout));

    let mut expansion = Expansion {
        ctx,
        children: Vec::new(),
        field_mask,
        is_disk,
        needs_offsets,
        max_expansions: config.max_prefix_expansions,
    };

    debug_assert!(!terms_trie.is_null(), "terms trie should be initialized");
    // SAFETY: `terms_trie` is the spec's terms `Trie`, valid for and unmutated
    // during the query (`QueryEvalContext` invariants 1/2).
    let terms = unsafe { CTrieRef::from_raw(terms_trie) };

    // A spec with a suffix trie may only answer a pattern when every queried field
    // is covered by it. An unsupported field set is an error that does *not* fall
    // back to a walk — it yields an empty union — and it is reported for any
    // pattern, so it is decided before the empty-pattern case below.
    let fields_unsupported = !suffix_trie.is_null()
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
    } else if let Some(pattern) = LoweredPattern::new(&pattern) {
        // The suffix trie is preferred whenever the spec has one; the brute-force
        // terms-trie scan is the fallback for a pattern it has no literal run to
        // anchor on.
        //
        // Carries the pattern while it is still un-walked; `None` once a walk has
        // consumed it.
        let mut brute_force = Some(pattern);
        if !suffix_trie.is_null() {
            // Reached only with the fields supported, ruled on above.
            let pattern = brute_force
                .take()
                .expect("the pattern is un-walked on the first walk");
            // SAFETY: `suffix_trie` is non-null (checked above) and is the spec's
            // suffix `Trie`, whose nodes carry the suffix-data payload the walk
            // expects; it is valid for and unmutated during the query
            // (`QueryEvalContext` invariants 1/2).
            let suffix = unsafe { CTrieRef::from_raw(suffix_trie) };
            brute_force = match expansion
                .expand_wildcard_via_suffix_trie(&suffix, &terms, pattern, timeout)
            {
                // The walk answered the pattern, and consumed it doing so.
                SuffixWalk::Walked => None,
                // Nothing to anchor on, so the pattern is still un-walked and the
                // brute-force scan below has to answer it instead.
                SuffixWalk::NoAnchor(pattern) => Some(pattern),
            };
        }
        if let Some(pattern) = brute_force {
            expansion.expand_wildcard_via_terms_trie(&terms, &pattern, timeout);
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
    /// Expand a wildcard `pattern` through the spec's suffix trie, accumulating one
    /// reader per matching term.
    ///
    /// `suffix` wraps the spec's suffix trie and `terms` its primary terms trie.
    /// A [`SuffixWalk::NoAnchor`] hands the pattern back for the caller to retry
    /// with [`expand_wildcard_via_terms_trie`](Self::expand_wildcard_via_terms_trie).
    ///
    /// Terms may repeat: one term can sit under several matching suffix keys, and
    /// each occurrence opens its own reader and counts against the expansion cap.
    /// The union deduplicates by document, so this affects cost and the cap, not
    /// results.
    fn expand_wildcard_via_suffix_trie(
        &mut self,
        suffix: &CTrieRef,
        terms: &CTrieRef,
        pattern: LoweredPattern,
        timeout: Option<NonNull<ffi::timespec>>,
    ) -> SuffixWalk {
        // The suffix trie hands terms back already as stored key bytes but carries
        // no document count, so on the disk path the term's count is looked up in
        // the primary terms trie for the IDF; the in-memory path ignores it.
        //
        // That lookup refuses a key that is not valid UTF-8, which here is the right
        // answer rather than a lost count: a disk index only stages a term whose
        // bytes are valid UTF-8, and the terms trie is only updated for terms that
        // staged, so a refused key was never counted there in the first place.
        let on_term = |term_bytes: &[u8]| {
            // The walk may keep calling back after a `Break`, so check the cap
            // before the document-count lookup, which is a trie walk of its own.
            if self.cap_reached() {
                return ControlFlow::Break(());
            }
            let num_docs = if self.is_disk {
                terms.num_docs(term_bytes)
            } else {
                0
            };
            self.push_child(num_docs, term_bytes)
        };
        // SAFETY: `suffix` wraps the spec's valid suffix `Trie`, whose nodes carry
        // the suffix-data payload the walk expects (caller contract), and `timeout`,
        // if set, points to the sctx's live timeout `timespec`. The trie is not
        // mutated or re-iterated for the duration of the call: `on_term` only opens
        // per-term readers (which read the spec's inverted indexes) and, on the disk
        // path, looks up the separate primary terms trie — never the suffix trie
        // being walked.
        unsafe { suffix.iterate_suffix_wildcard(pattern, timeout, on_term) }
    }

    /// Brute-force expand a wildcard `pattern` over the primary terms trie,
    /// accumulating one reader per matching term.
    ///
    /// `terms` wraps the spec's primary terms trie and `timeout` bounds the walk
    /// (`None` runs it to completion).
    fn expand_wildcard_via_terms_trie(
        &mut self,
        terms: &CTrieRef,
        pattern: &LoweredPattern,
        timeout: Option<NonNull<ffi::timespec>>,
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
        // SAFETY: `terms` wraps the spec's valid primary terms `Trie`; `timeout`, if
        // set, points to the sctx's live timeout `timespec`. The trie is not mutated
        // or re-iterated for the duration of the call: `on_runes` only opens per-term
        // readers, which read the spec's inverted indexes rather than the terms trie
        // being walked.
        unsafe {
            terms.iterate_wildcard(pattern, timeout, on_runes);
        }
    }
}
