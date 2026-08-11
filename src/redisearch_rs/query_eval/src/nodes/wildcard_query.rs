/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Evaluation of `QN_WILDCARD_QUERY` query nodes.

use std::{borrow::Cow, ptr::NonNull, str};

use query_error::QueryErrorCode;
use query_types::QueryNodeType;
use rqe_iterators::{
    union_opaque::build_union_with_q_str,
    utils::{AnyTimeoutContext, TimeoutContext, deadline_passed},
};
use string_utils::unicode::tolower_capped;
use term_dictionary::TermDictionary;
use term_suffix_index::TermSuffixIndex;

use crate::{
    Config, Evaluated, QueryEvalContext, QueryNodeMut, expansion::Expansion,
    expansion_needs_offsets,
};

/// `QN_WILDCARD_QUERY` — expand a verbatim wildcard pattern (`w'he?l*o'`, where
/// `*` matches any run of characters and `?` exactly one) over the spec's terms
/// dictionary into a union of per-term readers.
///
/// Unlike a prefix pattern, a wildcard pattern is not anchored: the spec's suffix
/// index is consulted whenever it exists, and the primary terms dictionary is
/// walked only when the suffix index has no literal run to anchor on.
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
    let tok = tok.as_ref();

    // As for a prefix pattern, the terms dictionary and the suffix index are both
    // keyed by case-folded UTF-8, so the pattern is folded in UTF-8 rather than
    // decoded to runes, and capped at the same codepoint count the rune
    // representation is: a pattern past it cannot be looked up at all.
    let too_long = |ctx: &mut QueryEvalContext| {
        ctx.status().set_error(
            QueryErrorCode::Limit,
            &format!(
                "Wildcard query string is too long. Maximum allowed length is {}",
                ffi::MAX_RUNE_STR_LEN
            ),
        );
    };
    let Some(bytes) = tok.as_bytes() else {
        // A wildcard node without a token string names nothing to look up, which
        // the C evaluator also reports as a too-long pattern.
        too_long(ctx);
        return None;
    };
    // `tok` is *not* guaranteed to be valid UTF-8, for the reasons spelled out in
    // the prefix evaluator. Indexing refuses a term whose bytes are not valid
    // UTF-8, so such a pattern names no stored term: it matches nothing rather
    // than erroring.
    let folded = match str::from_utf8(bytes) {
        Ok(text) => match tolower_capped(text, ffi::MAX_RUNE_STR_LEN as usize) {
            Some(folded) => Some(folded),
            None => {
                too_long(ctx);
                return None;
            }
        },
        Err(_) => None,
    };

    let suffix_index = ctx.spec().suffix;
    let suffix_mask = ctx.spec().suffixMask;
    let terms_dict = ctx.spec().terms;
    // Both scans below are bounded by the search deadline, and each takes it in
    // the form that fits how it polls: the suffix index polls once per candidate
    // batch, so it gets the raw deadline to read, while the dictionary walk polls
    // per term and so amortizes the clock read over `TIMEOUT_COUNTER_LIMIT`
    // probes. Resolved here, with every other read of `ctx`, because `Expansion`
    // borrows it mutably for the rest of the expansion.
    let time = &ctx.sctx().time;
    let deadline = (!time.skipTimeoutChecks).then(|| NonNull::from(&time.timeout));
    let sctx = NonNull::new(ctx.sctx_ptr().cast_mut()).expect("sctx must be non-null");
    // SAFETY: invariant (2) of `QueryEvalContext::new` keeps `sctx` valid, at a
    // stable address, for as long as the iterators built here are used, which is
    // what `from_sctx` needs to read the deadline back on each probe. Writes to
    // the deadline never overlap a probe (see `TimeoutContextDeadline::new`).
    let timeout = unsafe { AnyTimeoutContext::from_sctx(sctx, ffi::TIMEOUT_COUNTER_LIMIT) };

    let mut expansion = Expansion {
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
    } else {
        match folded {
            // A non-UTF-8 pattern matches no indexable term; the union stays empty.
            None => {}
            // A pattern that folds to nothing wildcard-matches exactly the empty
            // term, which neither walk below can find: a zero-length key is refused
            // on insertion, so an indexed empty value exists only as an inverted
            // index. Open that index directly, as an empty-string query would. A
            // field without `INDEXEMPTY` has no such index and the pattern matches
            // nothing.
            //
            // The IDF count is zero even on the disk path: no zero-length key is
            // ever inserted, so the terms dictionary cannot supply one, and the
            // empty term is scored as one never seen whatever its inverted index
            // holds.
            Some(folded) if folded.is_empty() => {
                let _ = expansion.push_child(0, b"");
            }
            Some(folded) => {
                let pattern = escape_backslashes(&folded);
                // The suffix index is preferred whenever the spec has one; the
                // brute-force dictionary scan is the fallback for a pattern it has
                // no literal run to anchor on.
                let mut walked = false;
                if !suffix_index.is_null() {
                    // Reached only with the fields supported, ruled on above.
                    // SAFETY: `suffix_index` is non-null (checked above) and is the
                    // spec's `TermSuffixIndex`, built by `TermSuffixIndex_New` and
                    // held behind an opaque C typedef, so the cast recovers the Rust
                    // type it was created as. It is valid for and unmutated during
                    // the query (`QueryEvalContext` invariants 1/2), which satisfies
                    // the index's readers-writer contract.
                    let suffix = unsafe { &*suffix_index.cast::<TermSuffixIndex>() };
                    walked = expansion
                        .expand_wildcard_via_suffix_index(suffix, terms, &pattern, deadline);
                }
                if !walked {
                    expansion.expand_wildcard_via_terms_dict(terms, &pattern, timeout);
                }
            }
        }
    }

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

/// Double every backslash in `pattern` so both wildcard engines read it as the
/// literal it now is.
///
/// The token reaching here has had one backslash-stripping pass run over it
/// already, which leaves a literal backslash bare — while the engines re-read a
/// bare backslash as escaping whatever follows it. A pattern without a backslash,
/// the common case, is handed back borrowed.
fn escape_backslashes(pattern: &str) -> Cow<'_, str> {
    if !pattern.contains('\\') {
        return Cow::Borrowed(pattern);
    }
    Cow::Owned(pattern.replace('\\', r"\\"))
}

impl Expansion<'_> {
    /// Expand a wildcard `pattern` through the spec's suffix index, accumulating one
    /// reader per matching term.
    ///
    /// `suffix` is the spec's suffix index and `terms` its primary terms dictionary.
    /// Reports whether the index answered the pattern: `false` leaves it for
    /// [`expand_wildcard_via_terms_dict`](Self::expand_wildcard_via_terms_dict),
    /// because no token in the pattern can anchor the scan.
    ///
    /// `deadline` bounds the scan (`None` runs it to completion). The scan gathers
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
        terms: &TermDictionary,
        pattern: &str,
        deadline: Option<NonNull<ffi::timespec>>,
    ) -> bool {
        let should_stop = || match deadline {
            // SAFETY: `deadline`, when set, points to the sctx's live timeout
            // `timespec`, which nothing writes while the query reads it.
            Some(deadline) => deadline_passed(unsafe { *deadline.as_ptr() }),
            None => false,
        };
        let Some(matches) = suffix.iter_wildcard(pattern, should_stop) else {
            return false;
        };
        // The suffix index hands terms back already as stored keys but carries no
        // document count, so on the disk path the term's count is looked up in the
        // primary terms dictionary for the IDF; the in-memory path ignores it.
        for term in matches {
            let num_docs = if self.is_disk {
                terms.get(&term).map_or(0, |entry| entry.num_docs)
            } else {
                0
            };
            if self.push_child(num_docs, term.as_bytes()).is_break() {
                break;
            }
        }
        true
    }

    /// Brute-force expand a wildcard `pattern` over the primary terms dictionary,
    /// accumulating one reader per matching term.
    ///
    /// `terms` is the spec's primary terms dictionary and `timeout` bounds the walk.
    fn expand_wildcard_via_terms_dict(
        &mut self,
        terms: &TermDictionary,
        pattern: &str,
        mut timeout: AnyTimeoutContext,
    ) {
        // The dictionary is keyed by the term's stored bytes and carries its
        // document count, used for the disk IDF.
        for (term, entry) in terms.wildcard_iter(pattern) {
            // Abandoning the walk mid-way leaves the union with the readers
            // collected so far, which is what the C evaluator returns too.
            if timeout.check_timeout().is_err() {
                break;
            }
            if self.push_child(entry.num_docs, term.as_bytes()).is_break() {
                break;
            }
        }
    }
}
