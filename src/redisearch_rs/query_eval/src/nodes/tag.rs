/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Evaluation of `QN_TAG` query nodes.

use std::{
    ffi::{CStr, c_void},
    ops::Bound,
    ptr::NonNull,
    time::Instant,
};

use c_trie::QueryRequestLimitsHandle;
use fat_array::{FatArray, FatArrayRef};
use field::{FieldExpirationPredicate, FieldFilterContext, FieldMaskOrIndex};
use index_result::RSIndexResult;
use inverted_index::{IndexReader, opaque::InvertedIndex};
use lending_iterator::LendingIterator as _;
use query::{QueryNode, WildcardMode};
use query_flags::QEFlag;
use query_types::QueryNodeType;
use rqe_core::{DocId, FieldIndex};
use rqe_iterators::{
    ExpirationChecker as _, FieldExpirationChecker, RQEIterator as _,
    c2rust::CRQEIterator,
    lex_range::LexRange,
    union_opaque::{build_union, build_union_with_q_str},
    utils::duration_from_redis_timespec,
};
use string_utils::{tag::unescape, unicode::tolower_bytes};
use trie_rs::{
    TrieMap, TrieMapOpaque,
    iter::{PatternMode, RangeBoundary, RangeFilter, RangeLendingIter},
};

use super::wildcard_query::below_min_term_prefix;
use crate::{Config, Evaluated, QueryEvalContext, QueryNodeMut, into_child_iterator};

/// The request's clock deadline, or `None` when it has no clock-based timeout.
fn clock_deadline(ctx: &QueryEvalContext) -> Option<ffi::timespec> {
    // SAFETY: a non-null request timeout is an initialized C object, aligned by its allocation,
    // that outlives query evaluation; the handle lives only for the single deadline read below,
    // so it never outlives the request. Its source is fixed for this execution cycle; only the
    // blocked-client marker and the memory stop flag may change concurrently, through the C
    // atomic API. Nothing in the evaluator holds a Rust reference to the timeout object.
    let timeout = unsafe { QueryRequestLimitsHandle::from_raw(ctx.sctx().limits) }?;
    timeout.clock_deadline()
}

/// Whether the request has been cancelled by the blocked-client callback.
///
/// A background query under `ON_TIMEOUT FAIL` or `RETURN_STRICT` runs a
/// blocked-client cycle, which publishes a flag instead of a clock deadline, so
/// [`clock_deadline`] reports nothing for it and a deadline-driven walk would
/// run on past the client's timeout while holding the spec read lock.
fn blocked_client_cancelled(ctx: &QueryEvalContext) -> bool {
    // SAFETY: as in `clock_deadline` -- a non-null request timeout is an
    // initialized C object that outlives query evaluation, and the handle lives
    // only for the single flag read below.
    let timeout = unsafe { QueryRequestLimitsHandle::from_raw(ctx.sctx().limits) };
    timeout.is_some_and(QueryRequestLimitsHandle::blocked_client_timed_out)
}

/// The `(timeout, skip_timeout_checks)` pair the suffix trie lookups take: the
/// clock deadline when there is one, otherwise a zeroed deadline with checks
/// skipped.
fn suffix_lookup_timeout(ctx: &QueryEvalContext) -> (ffi::timespec, bool) {
    match clock_deadline(ctx) {
        Some(deadline) => (deadline, false),
        None => (
            ffi::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            },
            true,
        ),
    }
}

/// `QN_TAG` — evaluate exact values and tag-specific expansions against a tag
/// field's own index.
pub(crate) fn eval<'index>(
    ctx: &'index mut QueryEvalContext,
    mut node: QueryNodeMut<'_>,
    field_index: FieldIndex,
    config: Config,
) -> Option<Evaluated<'index>> {
    let spec = ctx.spec();
    assert!(
        field_index < spec.numFields,
        "field_index must be within the spec's current field count"
    );
    // SAFETY: `field_index` is within `spec.numFields` (checked above), so this
    // stays within the bounds of the `numFields`-sized array `spec.fields` points to.
    let field_ptr = unsafe { spec.fields.add(field_index as usize) };
    // SAFETY: `field_ptr` points into the live spec's field array, which the
    // query's spec read lock keeps alive and unmodified throughout evaluation.
    // Going through the raw pointer detaches the borrow from `ctx`, which the
    // children's evaluation needs exclusively.
    let field = unsafe { &*field_ptr };
    debug_assert!(
        field.types() & ffi::FieldType_INDEXFLD_T_TAG != 0,
        "a tag node must reference a tag field"
    );
    // SAFETY: the field is a tag field, so `tagOpts` is the active union member.
    let index = NonNull::new(unsafe { field.__bindgen_anon_1.tagOpts.tagIndex })?;
    // SAFETY: the tag index belongs to the field; the query's spec read lock keeps
    // it alive and excludes writers, so it is not restructured during evaluation.
    let index = unsafe { TagIndexRef::new(index) };

    let weight = node.opts().weight;
    let min_term_prefix = ctx.config().min_term_prefix as usize;
    let max_prefix_expansions = ctx.config().max_prefix_expansions as usize;
    let num_children = node.num_children();

    if num_children == 1 {
        return eval_child(
            ctx,
            index,
            node.child_mut(0),
            weight,
            field,
            min_term_prefix,
            max_prefix_expansions,
            config,
        );
    }

    // We want results from every matching child (`quick_exit == false`) unless
    // either (1) we are inside a `NOT` subtree, where only the id set matters,
    // or (2) the node's weight is zero, so its subtree is irrelevant to scoring.
    let quick_exit = ctx.in_not_sub_tree() || weight == 0.0;
    let children = (0..num_children)
        .map(|i| {
            let child = node.child_mut(i);
            let child = eval_child(
                ctx,
                index,
                child,
                weight,
                field,
                min_term_prefix,
                max_prefix_expansions,
                config,
            );
            into_child_iterator(child)
        })
        .collect();
    let iter = build_union(
        children,
        quick_exit,
        config.min_union_iter_heap,
        QueryNodeType::Tag,
        weight,
    );
    Some(Evaluated::RustCompound(iter))
}

/// Evaluate one child of a `QN_TAG` node, dispatching on its node type.
#[expect(clippy::too_many_arguments)]
fn eval_child<'index>(
    ctx: &'index mut QueryEvalContext,
    index: TagIndexRef,
    child: QueryNodeMut<'_>,
    weight: f64,
    field: &ffi::FieldSpec,
    min_term_prefix: usize,
    max_prefix_expansions: usize,
    config: Config,
) -> Option<Evaluated<'index>> {
    // SAFETY: `tagOpts` is active for the tag field referenced by the parent.
    let case_sensitive = unsafe { field.__bindgen_anon_1.tagOpts.tagFlags() }
        & ffi::TagFieldFlags_TagField_CaseSensitive
        != 0;
    let hybrid = ctx
        .req_flags()
        .intersects(QEFlag::IsHybridSearchSubquery | QEFlag::IsHybridVectorAggregateSubquery);
    // For hybrid queries, use weight 0.0 to disable tag scoring.
    let effective_weight = if hybrid { 0.0 } else { weight };

    match child.node_type() {
        QueryNodeType::Token => {
            eval_token(ctx, index, child, effective_weight, field, case_sensitive)
        }
        QueryNodeType::Prefix => eval_prefix(
            ctx,
            index,
            child,
            effective_weight,
            field,
            case_sensitive,
            min_term_prefix,
            max_prefix_expansions,
            config,
        ),
        QueryNodeType::WildcardQuery => eval_wildcard(
            ctx,
            index,
            child,
            effective_weight,
            field,
            case_sensitive,
            min_term_prefix,
            max_prefix_expansions,
            config,
        ),
        QueryNodeType::Phrase => {
            eval_phrase(ctx, index, child, effective_weight, field, case_sensitive)
        }
        QueryNodeType::LexRange => {
            eval_lex_range(ctx, index, child, effective_weight, field, case_sensitive)
        }
        _ => unreachable!(
            "tag child grammar admits only token, prefix, wildcard, phrase and lex range nodes"
        ),
    }
}

/// Evaluate a `@tag:{value}` child: a single exact-value lookup.
fn eval_token<'index>(
    ctx: &'index mut QueryEvalContext,
    index: TagIndexRef,
    mut child: QueryNodeMut<'_>,
    effective_weight: f64,
    field: &ffi::FieldSpec,
    case_sensitive: bool,
) -> Option<Evaluated<'index>> {
    // SAFETY: invariant (5) of `QueryNodeMut::new` extends the token
    // string requirements to a tag's token children.
    let mut tok = unsafe { child.token_node_mut() }.expect("a tag token child must carry a token");
    // SAFETY: invariant (5) of `QueryNodeMut::new` puts this token in the
    // module allocator whenever the field is case-insensitive.
    unsafe { tok.normalize_tag(case_sensitive) };
    let value = tok
        .as_ref()
        .as_bytes()
        .expect("a tag token must carry a string");
    open_reader(ctx, index, value, effective_weight, field.index).map(Evaluated::C)
}

/// Evaluate a `@tag:{multi word}` child as one exact value, its words joined by a space.
fn eval_phrase<'index>(
    ctx: &'index mut QueryEvalContext,
    index: TagIndexRef,
    mut child: QueryNodeMut<'_>,
    effective_weight: f64,
    field: &ffi::FieldSpec,
    case_sensitive: bool,
) -> Option<Evaluated<'index>> {
    let mut value = Vec::new();
    for i in 0..child.num_children() {
        let mut term = child.child_mut(i);
        debug_assert_eq!(
            term.node_type(),
            QueryNodeType::Token,
            "tag phrase children must be tokens"
        );
        // SAFETY: invariant (5) of `QueryNodeMut::new` extends the token
        // string requirements to the terms of a tag's phrase children.
        let mut tok =
            unsafe { term.token_node_mut() }.expect("a tag phrase term must carry a token");
        // SAFETY: invariant (5) of `QueryNodeMut::new` puts this token in the
        // module allocator whenever the field is case-insensitive.
        unsafe { tok.normalize_tag(case_sensitive) };
        if i != 0 {
            value.push(b' ');
        }
        // Join by length: a term may embed a NUL.
        value.extend_from_slice(tok.as_ref().as_bytes().unwrap_or_default());
    }
    // Keep the value NUL-terminated past its length, like the parser's
    // own token strings, for readers that forward it as a token.
    value.push(0);
    let value = &value[..value.len() - 1];
    open_reader(ctx, index, value, effective_weight, field.index).map(Evaluated::C)
}

/// Evaluate a `@tag:{pat*}`, `{*pat}` or `{*pat*}` child as a union of every matching value.
#[expect(clippy::too_many_arguments)]
fn eval_prefix<'index>(
    ctx: &'index mut QueryEvalContext,
    index: TagIndexRef,
    mut child: QueryNodeMut<'_>,
    effective_weight: f64,
    field: &ffi::FieldSpec,
    case_sensitive: bool,
    min_term_prefix: usize,
    max_prefix_expansions: usize,
    config: Config,
) -> Option<Evaluated<'index>> {
    let mode = match child.as_enum() {
        QueryNode::Prefix { mode, .. } => mode,
        _ => unreachable!("prefix evaluation requires a prefix node"),
    };
    let mut tok = child
        .token_mut()
        .expect("a tag prefix child must carry a token");
    // SAFETY: invariant (5) of `QueryNodeMut::new` puts this token in the
    // module allocator whenever the field is case-insensitive.
    unsafe { tok.normalize_tag(case_sensitive) };
    let tok_ref = tok.as_ref();
    if tok_ref.len() < min_term_prefix {
        return None;
    }

    let value = tok_ref
        .as_bytes()
        .expect("a tag prefix token must carry a string");
    let with_suffix_trie = field.options() & ffi::FieldSpecOptions_FieldSpec_WithSuffixTrie != 0;
    let children = if mode == WildcardMode::Prefix || !with_suffix_trie {
        let iter_mode = match mode {
            WildcardMode::Prefix => PatternMode::Prefix,
            WildcardMode::Suffix => PatternMode::Suffix,
            WildcardMode::Contains => PatternMode::Contains,
        };
        collect_filtered_readers(
            ctx,
            index,
            value,
            iter_mode,
            field.index,
            max_prefix_expansions,
        )
    } else {
        collect_suffix_readers(
            ctx,
            index,
            value,
            mode == WildcardMode::Contains,
            field.index,
            max_prefix_expansions,
        )?
    };

    let q_str = tok_ref
        .as_c_str()
        .expect("a tag prefix token must carry a string");
    // SAFETY: the normalized token is owned by the AST and is not rewritten
    // again, so it outlives the query iterator that retains it for profiling.
    let iter = unsafe {
        build_union_with_q_str(
            children,
            true,
            config.min_union_iter_heap,
            QueryNodeType::Prefix,
            q_str,
            effective_weight,
        )
    };
    Some(Evaluated::RustCompound(iter))
}

/// Evaluate a `@tag:>{bound}` child, and its `>=`, `<` and `<=` spellings, as
/// the sorted set of documents carrying any indexed value inside the range.
fn eval_lex_range<'index>(
    ctx: &'index mut QueryEvalContext,
    index: TagIndexRef,
    child: QueryNodeMut<'_>,
    effective_weight: f64,
    field: &ffi::FieldSpec,
    case_sensitive: bool,
) -> Option<Evaluated<'index>> {
    let QueryNode::LexRange { begin, end } = child.as_enum() else {
        unreachable!("lex range evaluation requires a lex range node")
    };

    // The bound this clause carries, for profiling output. The grammar builds
    // one node per clause and leaves the other side unbounded, so naming it
    // tells two clauses on the same field apart, which the node type alone does
    // not. Which side it bounds is not in here; `FT.EXPLAIN` spells that out.
    let clause_bound = match (begin, end) {
        (Bound::Included(bound) | Bound::Excluded(bound), _)
        | (Bound::Unbounded, Bound::Included(bound) | Bound::Excluded(bound)) => Some(bound),
        (Bound::Unbounded, Bound::Unbounded) => None,
    };

    // Compare against the values as they were indexed, which is what
    // `RSTokenMut::normalize_tag` arranges for a tag token. A bound is a plain
    // string rather than a token, so it takes the same two passes directly. Both
    // outlive the filter that borrows them.
    let begin = normalize_bound(begin, case_sensitive);
    let end = normalize_bound(end, case_sensitive);

    let (mut doc_ids, values_walked) = collect_range_doc_ids(
        ctx,
        index,
        RangeFilter {
            min: range_boundary(&begin),
            max: range_boundary(&end),
        },
        field.index,
    );
    // A multi-valued document is reached once per value it carries.
    doc_ids.sort_unstable();
    doc_ids.dedup();
    // Each document matches with the unit frequency a tag value's reader gives
    // it, so that frequency-based scorers such as DISMAX still credit the match.
    let result = RSIndexResult::build_virt()
        .frequency(1)
        .weight(effective_weight)
        .build();
    Some(Evaluated::RustLeaf(Box::new(LexRange::new(
        doc_ids,
        result,
        clause_bound,
        values_walked,
    ))))
}

/// Normalize one bound the way tag indexing normalized the values it is compared
/// against: escape removal, then case folding unless the field is CASESENSITIVE.
fn normalize_bound(bound: Bound<&CStr>, case_sensitive: bool) -> Bound<Vec<u8>> {
    fn normalize(value: &CStr, case_sensitive: bool) -> Vec<u8> {
        let unescaped = unescape(value.to_bytes());
        if case_sensitive {
            unescaped.into_owned()
        } else {
            tolower_bytes(&unescaped).into_owned()
        }
    }

    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(value) => Bound::Included(normalize(value, case_sensitive)),
        Bound::Excluded(value) => Bound::Excluded(normalize(value, case_sensitive)),
    }
}

/// The trie's boundary for a normalized bound. `None` leaves that side open,
/// which an empty bound does not: `""` is a value of its own.
fn range_boundary(bound: &Bound<Vec<u8>>) -> Option<RangeBoundary<'_>> {
    match bound {
        Bound::Unbounded => None,
        Bound::Included(value) => Some(RangeBoundary::included(value)),
        Bound::Excluded(value) => Some(RangeBoundary::excluded(value)),
    }
}

/// Values walked between two blocked-client cancellation probes, matching the
/// cadence `trie_rs` uses between its own clock probes.
const CANCELLATION_CHECK_GRANULARITY: u32 = 100;

/// Collect the documents of every value the range covers.
///
/// A range over a unique field covers one value per document, so a union of
/// per-value readers would cost an iterator per document and is what the
/// expansion cap exists to bound. Nothing here outlives its value, so the range
/// is not subject to that cap: the walk carries the request deadline as the
/// pattern walk in [`collect_filtered_readers`] does, and observes a
/// blocked-client cancellation, which carries no deadline.
fn collect_range_doc_ids(
    ctx: &mut QueryEvalContext,
    index: TagIndexRef,
    filter: RangeFilter<'_>,
    field_index: ffi::t_fieldIndex,
) -> (Vec<DocId>, usize) {
    let values = index.values().range_iter(filter);
    let mut values = RangeLendingIter::from(values);
    values.set_timeout(
        clock_deadline(ctx)
            .and_then(duration_from_redis_timespec)
            .map(|remaining| Instant::now() + remaining),
    );

    let mut doc_ids = Vec::new();
    let mut until_cancellation_check = CANCELLATION_CHECK_GRANULARITY;
    let in_memory = !index.has_disk_spec();
    let mut values_walked = 0;
    while let Some((value, postings)) = values.next() {
        values_walked += 1;
        // The deadline installed above covers a clock cycle. A blocked-client
        // cycle carries no deadline, so without this the walk would not observe
        // that the client has already timed out.
        until_cancellation_check -= 1;
        if until_cancellation_check == 0 {
            until_cancellation_check = CANCELLATION_CHECK_GRANULARITY;
            if blocked_client_cancelled(ctx) {
                break;
            }
        }
        if in_memory {
            drain_postings(ctx, *postings, field_index, &mut doc_ids);
            continue;
        }
        let Some(mut reader) = open_reader_child(ctx, index, value, field_index) else {
            continue;
        };
        // A read error is the reader's deadline, which the pipeline's own
        // deadline check reports; the documents drained so far stand.
        while let Ok(Some(result)) = reader.read() {
            doc_ids.push(result.doc_id);
        }
    }
    (doc_ids, values_walked)
}

/// Append the documents of one in-memory tag value's postings, skipping those
/// whose field has expired, as the value's reader would.
fn drain_postings(
    ctx: &QueryEvalContext,
    postings: *mut c_void,
    field_index: ffi::t_fieldIndex,
    doc_ids: &mut Vec<DocId>,
) {
    // SAFETY: in memory, a tag index's values trie maps each value to its
    // `InvertedIndex`, which the query's spec read lock keeps alive and unchanged.
    let postings = unsafe { &*postings.cast::<InvertedIndex>() };
    match postings {
        InvertedIndex::DocIdsOnly(ii) => drain_reader(ctx, ii.reader(), field_index, doc_ids),
        InvertedIndex::RawDocIdsOnly(ii) => drain_reader(ctx, ii.reader(), field_index, doc_ids),
        _ => unreachable!("tag values are indexed with doc-ids-only encodings"),
    }
}

fn drain_reader<'index>(
    ctx: &QueryEvalContext,
    mut reader: impl IndexReader<'index>,
    field_index: ffi::t_fieldIndex,
    doc_ids: &mut Vec<DocId>,
) {
    let sctx = NonNull::new(ctx.sctx_ptr() as *mut ffi::RedisSearchCtx)
        .expect("a query evaluation context always has a search context");
    let filter = FieldFilterContext {
        field: FieldMaskOrIndex::Index(field_index),
        predicate: FieldExpirationPredicate::Default,
    };
    // SAFETY: the search context and its spec outlive the evaluation, and so
    // this checker, which does not escape it.
    let checker = unsafe { FieldExpirationChecker::new(sctx, filter, reader.flags()) };
    let check_expiration = checker.has_expiration();
    let mut result = RSIndexResult::build_virt().build();
    while let Ok(true) = reader.next_record(&mut result) {
        if check_expiration && checker.is_expired_trusting_inline_bit(&result) {
            continue;
        }
        doc_ids.push(result.doc_id);
    }
}

/// Evaluate a `@tag:{w'pattern'}` child as a union of every value matching the pattern.
#[expect(clippy::too_many_arguments)]
fn eval_wildcard<'index>(
    ctx: &'index mut QueryEvalContext,
    index: TagIndexRef,
    mut child: QueryNodeMut<'_>,
    effective_weight: f64,
    field: &ffi::FieldSpec,
    case_sensitive: bool,
    min_term_prefix: usize,
    max_prefix_expansions: usize,
    config: Config,
) -> Option<Evaluated<'index>> {
    let mut tok = child
        .token_mut()
        .expect("a tag wildcard child must carry a token");
    // SAFETY: invariant (5) of `QueryNodeMut::new` puts this token in the
    // module allocator whenever the field is case-insensitive.
    unsafe { tok.normalize_tag(case_sensitive) };
    tok.remove_wildcard_escapes();
    let tok_ref = tok.as_ref();
    let pattern = tok_ref
        .as_bytes()
        .expect("a tag wildcard token must carry a string");
    if below_min_term_prefix(pattern, min_term_prefix) {
        return None;
    }

    let children = if pattern.is_empty() {
        // Not `b""`: an empty slice's pointer dangles, while this one points at
        // a real NUL, for readers that forward the value as a C string.
        open_reader_child(ctx, index, c"".to_bytes(), field.index)
            .into_iter()
            .collect()
    } else if index.has_suffix() {
        let (timeout, skip_timeout_checks) = suffix_lookup_timeout(ctx);
        // SAFETY: the index is live for evaluation and `pattern` is readable
        // for the call. The callee also reads the byte at `pattern[len]` while
        // scanning for `*` runs, so it relies on the NUL terminator that
        // `tok_ref`, an `RSTokenRefNulTerminated`, guarantees sits there.
        let matches = unsafe {
            ffi::TagIndex_GetSuffixWildcardMatches(
                index.as_ptr(),
                pattern.as_ptr().cast(),
                pattern.len() as u32,
                timeout,
                max_prefix_expansions as i64,
                skip_timeout_checks,
            )
        };
        let matches = NonNull::new(matches)?;
        // `TagIndex_GetSuffixWildcardMatches` returns the `BAD_POINTER_ADDR`
        // sentinel, rather than a match array, when the pattern has no literal
        // segment for the suffix trie to anchor on (it is empty or made only of
        // `*`). The trie cannot answer such a pattern, so fall back to scanning
        // every tag value, whereas a null result means the trie found no match.
        // FIXME: return a proper type when porting
        // `TagIndex_GetSuffixWildcardMatches` to Rust.
        if matches.addr().get() == ffi::BAD_POINTER_ADDR as usize {
            collect_filtered_readers(
                ctx,
                index,
                pattern,
                PatternMode::Wildcard,
                field.index,
                max_prefix_expansions,
            )
        } else {
            // SAFETY: any other non-null result is a fat array of `char *`
            // elements, built by the C array API and so aligned for them and
            // freeable by `array_free`, whose ownership the call hands to us.
            // Each element points to a NUL-terminated string owned by the
            // suffix trie.
            let matches = unsafe { FatArray::new(matches) };
            collect_wildcard_suffix_readers(
                ctx,
                index,
                matches.as_slice(),
                field.index,
                max_prefix_expansions,
            )
        }
    } else {
        collect_filtered_readers(
            ctx,
            index,
            pattern,
            PatternMode::Wildcard,
            field.index,
            max_prefix_expansions,
        )
    };

    let q_str = tok_ref
        .as_c_str()
        .expect("a tag wildcard token must carry a string");
    // SAFETY: the normalized token is owned by the AST and is not rewritten
    // again, so it outlives the query iterator that retains it for profiling.
    let iter = unsafe {
        build_union_with_q_str(
            children,
            true,
            config.min_union_iter_heap,
            QueryNodeType::WildcardQuery,
            q_str,
            effective_weight,
        )
    };
    Some(Evaluated::RustCompound(iter))
}

/// Open a reader for each tag value matching `pattern` in `mode`, by scanning the values trie.
fn collect_filtered_readers(
    ctx: &mut QueryEvalContext,
    index: TagIndexRef,
    pattern: &[u8],
    mode: PatternMode,
    field_index: ffi::t_fieldIndex,
    max_prefix_expansions: usize,
) -> Vec<CRQEIterator> {
    let mut values = index.values().pattern_lending_iter(pattern, mode);
    values.set_timeout(
        clock_deadline(ctx)
            .and_then(duration_from_redis_timespec)
            .map(|remaining| Instant::now() + remaining),
    );

    let mut children = Vec::new();
    while let Some((value, _)) = values.next() {
        if children.len() == max_prefix_expansions {
            ctx.status()
                .warnings_mut()
                .set_reached_max_prefix_expansions();
            break;
        }
        children.extend(open_reader_child(ctx, index, value, field_index));
    }
    children
}

/// Open a reader for each tag value ending with `pattern`, or containing it when `contains`,
/// via the suffix trie.
fn collect_suffix_readers(
    ctx: &mut QueryEvalContext,
    index: TagIndexRef,
    pattern: &[u8],
    contains: bool,
    field_index: ffi::t_fieldIndex,
    max_prefix_expansions: usize,
) -> Option<Vec<CRQEIterator>> {
    let (timeout, skip_timeout_checks) = suffix_lookup_timeout(ctx);
    // SAFETY: the index is live for evaluation and `pattern` is readable for
    // the call.
    let matches = unsafe {
        ffi::TagIndex_GetSuffixMatches(
            index.as_ptr(),
            pattern.as_ptr().cast(),
            pattern.len() as u32,
            contains,
            timeout,
            skip_timeout_checks,
        )
    };
    // SAFETY: a non-null result is a fat array of `char **` elements, built
    // by the C array API and so aligned for them and freeable by `array_free`,
    // whose ownership the call hands to us. Its elements, the inner arrays,
    // stay owned by the suffix trie.
    let matches = unsafe { FatArray::new(NonNull::new(matches)?) };
    let mut children = Vec::new();
    for &inner in matches.as_slice() {
        if children.len() >= max_prefix_expansions {
            break;
        }
        // SAFETY: each entry is a fat array of `char *` elements, aligned for
        // them by its allocation, each pointing to a NUL-terminated tag string.
        // The suffix trie owns it and keeps it alive and unmodified while the
        // query holds the spec read lock; it is consumed within this loop
        // iteration, well inside that lock.
        let arr = unsafe { FatArrayRef::new(inner) };
        for &value in arr.as_slice() {
            if children.len() >= max_prefix_expansions {
                ctx.status()
                    .warnings_mut()
                    .set_reached_max_prefix_expansions();
                break;
            }
            debug_assert!(!value.is_null(), "a suffix match must carry a string");
            // SAFETY: the suffix trie stores NUL-terminated tag strings.
            let value = unsafe { CStr::from_ptr(value) }.to_bytes();
            children.extend(open_reader_child(ctx, index, value, field_index));
        }
    }
    Some(children)
}

/// Open a reader for each tag value the suffix trie matched against a wildcard pattern.
fn collect_wildcard_suffix_readers(
    ctx: &mut QueryEvalContext,
    index: TagIndexRef,
    matches: &[*mut std::ffi::c_char],
    field_index: ffi::t_fieldIndex,
    max_prefix_expansions: usize,
) -> Vec<CRQEIterator> {
    let mut children = Vec::new();
    for &value in matches {
        if children.len() >= max_prefix_expansions {
            ctx.status()
                .warnings_mut()
                .set_reached_max_prefix_expansions();
            break;
        }
        debug_assert!(!value.is_null(), "a suffix match must carry a string");
        // SAFETY: the suffix trie stores NUL-terminated tag strings.
        let value = unsafe { CStr::from_ptr(value) }.to_bytes();
        children.extend(open_reader_child(ctx, index, value, field_index));
    }
    children
}

/// Open a reader over one tag value, or `None` when the value has no index.
fn open_reader(
    ctx: &mut QueryEvalContext,
    index: TagIndexRef,
    value: &[u8],
    weight: f64,
    field_index: ffi::t_fieldIndex,
) -> Option<NonNull<ffi::QueryIterator>> {
    // SAFETY: `index` is live by its type invariant, `ctx` supplies a live
    // search context and status, and `value` is length-delimited and readable
    // for the call.
    NonNull::new(unsafe {
        ffi::TagIndex_OpenReader(
            index.as_ptr(),
            ctx.sctx_ptr(),
            value.as_ptr().cast(),
            value.len(),
            weight,
            field_index,
            ctx.status_ptr(),
        )
    })
}

/// Open an expansion's reader over one tag value as a union child.
///
/// Expansion readers carry unit weight: the enclosing union applies the node's.
fn open_reader_child(
    ctx: &mut QueryEvalContext,
    index: TagIndexRef,
    value: &[u8],
    field_index: ffi::t_fieldIndex,
) -> Option<CRQEIterator> {
    let reader = open_reader(ctx, index, value, 1.0, field_index)?;
    // SAFETY: `TagIndex_OpenReader` returns an owning query iterator with all
    // the callbacks `CRQEIterator` requires.
    Some(unsafe { CRQEIterator::new(reader) })
}

/// A tag index that stays alive for the whole query evaluation.
#[derive(Clone, Copy)]
struct TagIndexRef(NonNull<ffi::TagIndex>);

impl TagIndexRef {
    /// Wrap a tag index pointer.
    ///
    /// # Safety
    ///
    /// `index` must point to a tag index that stays alive and is not
    /// restructured for as long as the query is being evaluated.
    const unsafe fn new(index: NonNull<ffi::TagIndex>) -> Self {
        Self(index)
    }

    /// The raw pointer to the tag index.
    const fn as_ptr(self) -> *mut ffi::TagIndex {
        self.0.as_ptr()
    }

    /// The index's values trie, mapping each tag value to its postings.
    fn values(&self) -> &TrieMap<*mut c_void> {
        // SAFETY: the index is live by the type invariant. Reading the field
        // through the raw pointer creates no reference to the C struct.
        let values = unsafe { (*self.as_ptr()).values };
        debug_assert!(!values.is_null(), "a tag index always has a values trie");
        // SAFETY: a tag index's values trie is a `TrieMapOpaque`, which the type
        // invariant keeps alive and unrestructured while `self` is borrowed.
        // Evaluation only reads it, C included (`TagIndex_OpenReader` looks tags
        // up), so this shared borrow aliases no mutation.
        let TrieMapOpaque(values) = unsafe { &*values.cast::<TrieMapOpaque>() };
        values
    }

    /// Whether the index is disk-backed, so its values trie holds no postings.
    fn has_disk_spec(self) -> bool {
        // SAFETY: the index is live by the type invariant. Reading the field
        // through the raw pointer creates no reference to the C struct.
        !unsafe { (*self.as_ptr()).diskSpec }.is_null()
    }

    /// Whether the index keeps a suffix trie.
    fn has_suffix(self) -> bool {
        // SAFETY: the index is live by the type invariant.
        unsafe { ffi::TagIndex_HasSuffix(self.as_ptr()) }
    }
}
