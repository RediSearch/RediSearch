/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Evaluation of `QN_PREFIX` query nodes.

use std::{ops::ControlFlow, ptr::NonNull};

use c_trie::{CTrieRef, SuffixMode};
use query::WildcardMode;
use query_error::QueryErrorCode;
use query_types::QueryNodeType;
use rqe_core::FieldMask;
use rqe_iterators::{c2rust::CRQEIterator, union_opaque::build_union_with_q_str};
use rs_token::RSTokenRefNulTerminated;

use crate::{
    Config, Evaluated, QueryEvalContext, QueryNodeRef,
    expansion::{Expansion, runes_to_key},
    expansion_needs_offsets,
};

/// `QN_PREFIX` — expand a prefix, suffix, or contains pattern over the spec's
/// terms trie into a union of per-term readers.
///
/// Returns `None` when the pattern is shorter than the configured minimum or is
/// too long (the length error is reported via
/// [`status`](QueryEvalContext::status)).
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
    // over-broad expansions (e.g. a bare `a*`).
    if tok.len() < config.min_term_prefix as usize {
        return None;
    }

    // Lowercase the pattern to runes for the trie lookup.
    //
    // `tok` is *not* guaranteed to be valid UTF-8: a token is a byte string, and
    // nothing between the query text and here validates its encoding — the
    // grammar admits any byte that is not ASCII punctuation, whitespace or a
    // control character, and a token taken from a query parameter is binary-safe
    // besides. The conversion decodes it without validating, which is what makes
    // the pattern name the same runes the index does: a term's bytes are decoded
    // the same unvalidated way when it is indexed, so a client that stores and
    // queries text in the same non-UTF-8 encoding resolves its own terms.
    // Validating instead would fold malformed bytes to replacement characters
    // and look up a key that was never stored.
    let Some(pattern) = tok.as_lower_runes() else {
        ctx.status().set_error(
            QueryErrorCode::Limit,
            &format!(
                "{} query string is too long. Maximum allowed length is {}",
                mode.type_str(),
                ffi::MAX_RUNE_STR_LEN
            ),
        );
        return None;
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

    let suffix_trie = ctx.spec().suffix;
    let suffix_mask = ctx.spec().suffixMask;
    let terms_trie = ctx.spec().terms;
    // Enforce the search deadline unless timeout checks are disabled for this
    // request, in which case the brute-force walk below runs to completion.
    // Resolved here, with every other read of `ctx`, because `Expansion` borrows
    // it mutably for the rest of the expansion.
    let time = &ctx.sctx().time;
    let timeout = (!time.skipTimeoutChecks).then(|| NonNull::from(&time.timeout));

    let expansion = Expansion {
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

    let children = if match_suffix && !suffix_trie.is_null() {
        // The spec maintains a suffix trie for this pattern's fields: expand
        // through it.
        // SAFETY: `suffix_trie` is non-null (checked above) and is the spec's
        // suffix `Trie`, valid for and unmutated during the query.
        let suffix = unsafe { CTrieRef::from_raw(suffix_trie) };
        expansion.expand_via_suffix_trie(
            suffix,
            terms,
            &pattern,
            match_prefix,
            node_field_mask,
            suffix_mask,
        )
    } else {
        // Brute-force expansion over the primary terms trie.
        expansion.expand_via_terms_trie(terms, &pattern, match_prefix, match_suffix, timeout)
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
    /// Expand `pattern` through the spec's suffix trie, returning one reader per
    /// matching term.
    ///
    /// `suffix` wraps the spec's suffix trie and `terms` its primary terms trie.
    /// `contains` selects a contains (both-anchored) walk over a suffix
    /// (end-anchored) one. When the queried fields are not all covered by the
    /// suffix trie, a `Generic` error is set and no reader is returned.
    fn expand_via_suffix_trie(
        mut self,
        suffix: CTrieRef,
        terms: CTrieRef,
        pattern: &[ffi::rune],
        contains: bool,
        node_field_mask: FieldMask,
        suffix_mask: FieldMask,
    ) -> Vec<CRQEIterator> {
        // Use the suffix trie only when every queried field is covered by it,
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

        let suffix_mode = if contains {
            SuffixMode::Contains
        } else {
            SuffixMode::Suffix
        };

        // The suffix trie hands terms back already as stored key bytes but carries
        // no document count, so on the disk path the term's count is looked up in the
        // primary terms trie for the IDF; the in-memory path ignores it.
        let on_term = |term_bytes: &[u8]| {
            let num_docs = if self.is_disk {
                terms.num_docs(term_bytes)
            } else {
                0
            };
            self.push_child(num_docs, term_bytes)
        };
        // SAFETY: `suffix` wraps the spec's valid suffix `Trie`, whose nodes carry
        // the suffix-data payload the walk expects (caller contract); the suffix
        // walk carries no timeout. The trie is not mutated or re-iterated for the
        // duration of the call: `on_term` only opens per-term readers (which read
        // the spec's inverted indexes) and, on the disk path, looks up the
        // separate primary terms trie — never the suffix trie being walked.
        unsafe {
            suffix.iterate_suffix(pattern, suffix_mode, on_term);
        }
        self.children
    }

    /// Brute-force expand `pattern` over the primary terms trie, returning one
    /// reader per matching term.
    ///
    /// `terms` wraps the spec's primary terms trie. `match_prefix`/`match_suffix`
    /// anchor the walk (prefix, suffix, or — both set — contains); `timeout`
    /// bounds it (`None` runs it to completion).
    fn expand_via_terms_trie(
        mut self,
        terms: CTrieRef,
        pattern: &[ffi::rune],
        match_prefix: bool,
        match_suffix: bool,
        timeout: Option<NonNull<ffi::timespec>>,
    ) -> Vec<CRQEIterator> {
        // The primary trie hands terms back as runes (with their document count, used
        // for the disk IDF), which must be encoded back into the term's key, byte for
        // byte as the index stored it — WTF-8 rather than UTF-8 where a rune is a
        // lone surrogate.
        let on_runes = |runes: &[ffi::rune], num_docs: usize| {
            let Some(key) = runes_to_key(runes) else {
                // The term key cannot be reconstructed; skip this expansion.
                return ControlFlow::Continue(());
            };
            self.push_child(num_docs, &key)
        };
        // SAFETY: `terms` wraps the spec's valid primary terms `Trie`; `timeout`,
        // if set, points to the sctx's live timeout `timespec`. The trie is not
        // mutated or re-iterated for the duration of the call: `on_runes` only
        // opens per-term readers, which read the spec's inverted indexes rather
        // than the terms trie being walked.
        unsafe {
            terms.iterate_contains(pattern, match_prefix, match_suffix, timeout, on_runes);
        }
        self.children
    }
}
