/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Evaluation of `QN_PREFIX` query nodes.

use std::{ptr::NonNull, str};

use query::WildcardMode;
use query_error::QueryErrorCode;
use query_types::QueryNodeType;
use rqe_core::FieldMask;
use rqe_iterators::{
    c2rust::CRQEIterator,
    union_opaque::build_union_with_q_str,
    utils::{AnyTimeoutContext, TimeoutContext},
};
use rs_token::RSTokenRefNulTerminated;
use string_utils::unicode::tolower_capped;
use term_dictionary::{TermDictionary, TermEntry};
use term_suffix_index::TermSuffixIndex;

use crate::{
    Config, Evaluated, QueryEvalContext, QueryNodeRef, expansion::Expansion,
    expansion_needs_offsets,
};

/// `QN_PREFIX` — expand a prefix, suffix, or contains pattern over the spec's
/// terms dictionary into a union of per-term readers.
///
/// Returns `None` both when the pattern is shorter than the configured minimum
/// — silently, since such a query is well-formed and simply matches nothing —
/// and when it is too long, which is reported as an error via
/// [`status`](QueryEvalContext::status).
/// The number of expansions is capped by the configured
/// [`Config::max_prefix_expansions`].
pub(crate) fn eval<'index>(
    ctx: &'index mut QueryEvalContext,
    node: &QueryNodeRef,
    tok: RSTokenRefNulTerminated,
    mode: WildcardMode,
    config: Config,
) -> Option<Evaluated<'index>> {
    // A pattern shorter than the configured minimum is rejected to avoid
    // over-broad expansions (e.g. a bare `a*`). Not setting a ctx error: the minimum
    // is a resource guard rather than a validation rule, so the query stays
    // well-formed and simply matches nothing. Contrast the too-long case below,
    // which cannot be looked up at all and so is an error.
    if tok.len() < config.min_term_prefix as usize {
        return None;
    }

    // The terms dictionary and the suffix index are both keyed by case-folded
    // UTF-8, so the pattern is folded in UTF-8 rather than decoded to runes.
    // Folding it here rather than leaving it to the dictionary also hands the
    // lookups an already-lowercase needle, which they can borrow instead of
    // copying.
    //
    // The fold is capped at the same codepoint count the rune representation
    // is: a pattern past it cannot be looked up at all, which is an error.
    let too_long = |ctx: &mut QueryEvalContext| {
        ctx.status().set_error(
            QueryErrorCode::Limit,
            &format!(
                "{} query string is too long. Maximum allowed length is {}",
                mode.type_str(),
                ffi::MAX_RUNE_STR_LEN
            ),
        );
    };
    let Some(bytes) = tok.as_bytes() else {
        // A prefix node without a token string names nothing to look up, which
        // the C evaluator also reports as a too-long pattern.
        too_long(ctx);
        return None;
    };
    // `tok` is *not* guaranteed to be valid UTF-8: a token is a byte string, and
    // nothing between the query text and here validates its encoding — the
    // grammar admits any byte that is not ASCII punctuation, whitespace or a
    // control character, and a token taken from a query parameter is binary-safe
    // besides. Indexing refuses a term whose bytes are not valid UTF-8, so such
    // a pattern names no stored term: it matches nothing rather than erroring.
    let pattern = match str::from_utf8(bytes) {
        Ok(text) => match tolower_capped(text, ffi::MAX_RUNE_STR_LEN as usize) {
            Some(folded) => Some(folded),
            None => {
                too_long(ctx);
                return None;
            }
        },
        Err(_) => None,
    };

    // The reader field mask narrows the node's mask to the query-wide one; the
    // suffix-trie support check below uses the node's own mask.
    let node_field_mask = node.opts().field_mask;
    let weight = node.opts().weight;

    let (match_prefix, match_suffix) = match mode {
        WildcardMode::Prefix => (true, false),
        WildcardMode::Suffix => (false, true),
        WildcardMode::Contains => (true, true),
    };

    let field_mask = node_field_mask & ctx.opts().fieldmask;
    let is_disk = !ctx.spec().diskSpec.is_null();
    let needs_offsets = expansion_needs_offsets(ctx, node.opts(), config);

    let suffix_index = ctx.spec().suffix;
    let suffix_mask = ctx.spec().suffixMask;
    let terms_dict = ctx.spec().terms;
    // Enforce the search deadline unless timeout checks are disabled for this
    // request, in which case the brute-force walk below runs to completion.
    // Resolved here, with every other read of `ctx`, because `Expansion` borrows
    // it mutably for the rest of the expansion.
    let sctx = NonNull::new(ctx.sctx_ptr().cast_mut()).expect("sctx must be non-null");
    // SAFETY: invariant (2) of `QueryEvalContext::new` keeps `sctx` valid, at a
    // stable address, for as long as the iterators built here are used, which is
    // what `from_sctx` needs to read the deadline back on each probe. Writes to
    // the deadline never overlap a probe (see `TimeoutContextDeadline::new`).
    let timeout = unsafe { AnyTimeoutContext::from_sctx(sctx, ffi::TIMEOUT_COUNTER_LIMIT) };

    let expansion = Expansion {
        ctx,
        children: Vec::new(),
        field_mask,
        is_disk,
        needs_offsets,
        max_expansions: config.max_prefix_expansions,
    };

    debug_assert!(
        !terms_dict.is_null(),
        "terms dictionary should be initialized"
    );
    // SAFETY: `terms_dict` is the spec's `TermDictionary`, built by
    // `NewTermDictionary` and held behind an opaque C typedef, so the cast
    // recovers the Rust type it was created as. It is valid for and unmutated
    // during the query (`QueryEvalContext` invariants 1/2).
    let terms = unsafe { &*terms_dict.cast::<TermDictionary>() };

    let children = match pattern {
        // A non-UTF-8 pattern matches no indexable term; the union stays empty.
        None => Vec::new(),
        Some(pattern) if match_suffix && !suffix_index.is_null() => {
            // The spec maintains a suffix index for this pattern's fields: expand
            // through it.
            // SAFETY: `suffix_index` is non-null (checked above) and is the spec's
            // `TermSuffixIndex`, built by `TermSuffixIndex_New` and held behind an
            // opaque C typedef, so the cast recovers the Rust type it was created
            // as. It is valid for and unmutated during the query
            // (`QueryEvalContext` invariants 1/2), which satisfies the index's
            // readers-writer contract.
            let suffix = unsafe { &*suffix_index.cast::<TermSuffixIndex>() };
            expansion.expand_via_suffix_index(
                suffix,
                terms,
                &pattern,
                match_prefix,
                node_field_mask,
                suffix_mask,
            )
        }
        // Brute-force expansion over the primary terms dictionary.
        Some(pattern) => {
            expansion.expand_via_terms_dict(terms, &pattern, match_prefix, match_suffix, timeout)
        }
    };

    // Prefix unions always take the quick-exit path — they only need the
    // matching id set, never per-child scores — and carry the pattern token as
    // their profiling query string.
    let q_str = tok.as_c_str().expect("prefix token must carry a string");
    // The `CStr` borrows the node's token for the handle's lifetime, but the
    // union iterator retains it and outlives this call, escaping that borrow —
    // which is why `build_union_with_q_str` is `unsafe`.
    //
    // SAFETY: the token's string stays put for as long as the iterator does. A
    // `QN_PREFIX` node reaching here is a text-field prefix, whose token is
    // written once when the node is built (or when its query parameter is
    // resolved) and never rewritten afterwards. The one place that mutates a
    // prefix token in place, `tag_strtolower`, belongs to the tag expansion,
    // which C dispatches through `Query_EvalTagNode` without ever re-entering
    // this evaluator. Both the token and the iterator are owned by the query
    // AST, so the string also outlives the iterator.
    let iter = unsafe {
        build_union_with_q_str(
            children,
            true,
            config.min_union_iter_heap,
            QueryNodeType::Prefix,
            q_str,
            weight,
        )
    };
    Some(Evaluated::RustCompound(iter))
}

impl Expansion<'_> {
    /// Expand `pattern` through the spec's suffix index, returning one reader per
    /// matching term.
    ///
    /// `suffix` is the spec's suffix index and `terms` its primary terms
    /// dictionary. `pattern` is already case-folded. `contains` selects a
    /// contains (both-anchored) walk over a suffix (end-anchored) one. When the
    /// queried fields are not all covered by the suffix index, a `Generic` error
    /// is set and no reader is returned.
    fn expand_via_suffix_index(
        mut self,
        suffix: &TermSuffixIndex,
        terms: &TermDictionary,
        pattern: &str,
        contains: bool,
        node_field_mask: FieldMask,
        suffix_mask: FieldMask,
    ) -> Vec<CRQEIterator> {
        // Use the suffix index only when every queried field is covered by it,
        // otherwise the contains query is unsupported.
        let fields_supported = node_field_mask == rqe_core::RS_FIELDMASK_ALL
            || (suffix_mask & node_field_mask) == node_field_mask;
        if !fields_supported {
            self.ctx.status().set_error(
                QueryErrorCode::Generic,
                "Contains query on fields without WITHSUFFIXTRIE support",
            );
            return self.children;
        }

        if contains {
            self.push_suffix_matches(suffix.iter_contains(pattern), terms);
        } else {
            self.push_suffix_matches(suffix.iter_suffix(pattern), terms);
        }
        self.children
    }

    /// Open a reader per term in `matches`, stopping at the expansion cap.
    ///
    /// `terms` is the spec's primary terms dictionary.
    fn push_suffix_matches<'t>(
        &mut self,
        matches: impl Iterator<Item = &'t str>,
        terms: &TermDictionary,
    ) {
        // The suffix index hands terms back already as stored keys but carries no
        // document count, so on the disk path the term's count is looked up in the
        // primary terms dictionary for the IDF; the in-memory path ignores it.
        for term in matches {
            let num_docs = if self.is_disk {
                terms.get(term).map_or(0, |entry| entry.num_docs)
            } else {
                0
            };
            if self.push_child(num_docs, term.as_bytes()).is_break() {
                break;
            }
        }
    }

    /// Brute-force expand `pattern` over the primary terms dictionary, returning
    /// one reader per matching term.
    ///
    /// `pattern` is already case-folded. `match_prefix`/`match_suffix` anchor the
    /// walk (prefix, suffix, or — both set — contains); `timeout` bounds it.
    fn expand_via_terms_dict(
        mut self,
        terms: &TermDictionary,
        pattern: &str,
        match_prefix: bool,
        match_suffix: bool,
        mut timeout: AnyTimeoutContext,
    ) -> Vec<CRQEIterator> {
        // The dictionary is keyed by the term's stored bytes and carries its
        // document count, used for the disk IDF.
        let matches: Box<dyn Iterator<Item = (String, &TermEntry)>> =
            match (match_prefix, match_suffix) {
                (true, true) => Box::new(terms.contains_iter(pattern)),
                (true, false) => Box::new(terms.prefixed_iter(pattern)),
                (false, _) => Box::new(terms.suffixed_iter(pattern)),
            };
        for (term, entry) in matches {
            // Abandoning the walk mid-way leaves the union with the readers
            // collected so far, which is what the C evaluator returns too.
            if timeout.check_timeout().is_err() {
                break;
            }
            if self.push_child(entry.num_docs, term.as_bytes()).is_break() {
                break;
            }
        }
        self.children
    }
}
