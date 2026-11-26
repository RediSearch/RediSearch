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
};

use fat_array::{FatArray, FatArrayRef};
use lending_iterator::LendingIterator as _;
use query::{QueryNode, WildcardMode};
use query_flags::QEFlag;
use query_types::QueryNodeType;
use rqe_core::FieldIndex;
use rqe_iterators::{
    c2rust::CRQEIterator,
    union_opaque::{build_union, build_union_with_q_str},
};
use string_utils::{tag::unescape, unicode::tolower_bytes};
use trie_rs::{
    TrieMap, TrieMapOpaque,
    iter::{PatternLendingIter, PatternMode, RangeBoundary, RangeFilter, RangeLendingIter},
};

use search_disk::SearchDiskHandle;

use super::wildcard_query::below_min_term_prefix;
use crate::{
    Config, Evaluated, QueryEvalContext, QueryNodeMut,
    constant_leaf_bound::ConstantLeaf,
    disk::{constant_bound_for, with_constant_bound_c},
    expansion::memory_stop,
    into_child_iterator,
};

/// Run a TAG walk with a stop callback that cannot outlive the evaluation context.
fn with_expansion_stop_check<R>(
    ctx: &mut QueryEvalContext,
    walk: impl FnOnce(&mut QueryEvalContext, &(dyn Fn() -> bool + Sync)) -> R,
) -> R {
    struct ExpansionLimits(*const ffi::QueryRequestLimits);

    // SAFETY: the exact check reads the fixed timeout source and deadline, and uses
    // atomic loads for the blocked-client and memory-stop flags. It does not touch
    // the shared clock poll counter.
    unsafe impl Sync for ExpansionLimits {}

    impl ExpansionLimits {
        fn should_stop(&self) -> bool {
            // SAFETY: the callback stays within synchronous evaluation, when the limits
            // are live in the same execution cycle.
            !self.0.is_null() && unsafe { ffi::QueryRequestLimits_ShouldStopExact(self.0) }
        }
    }

    let limits = ExpansionLimits(ctx.sctx().limits);
    walk(ctx, &move || limits.should_stop())
}

fn request_should_stop(ctx: &mut QueryEvalContext) -> bool {
    with_expansion_stop_check(ctx, |_, stop| stop())
}

/// Amortize request-limit checks while turning suffix matches into posting readers.
fn suffix_reader_should_stop(ctx: &mut QueryEvalContext, remaining: &mut u32) -> bool {
    *remaining -= 1;
    if *remaining != 0 {
        return false;
    }
    *remaining = ffi::QUERY_REQUEST_TIMEOUT_COUNTER_LIMIT;
    request_should_stop(ctx)
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
    // Both query parsers build a tag node only on a TAG field.
    debug_assert!(
        field.types() & ffi::FieldType_INDEXFLD_T_TAG != 0,
        "a tag node must reference a tag field"
    );
    // SAFETY: the field is a tag field (asserted above), so `tagOpts` is the active union member.
    let index = NonNull::new(unsafe { field.__bindgen_anon_1.tagOpts.tagIndex })?;
    // SAFETY: the tag index belongs to the field; the query's spec read lock keeps
    // it alive and excludes writers, so it is not restructured during evaluation.
    let index = unsafe { TagIndexRef::new(index) };
    // SAFETY: `ctx.spec().diskSpec` is either null or a valid
    // `RedisSearchDiskIndexSpec` that stays valid for `'index`
    // (`QueryEvalContext` invariants 1/2).
    let leaf_bound = unsafe { SearchDiskHandle::new(ctx.spec().diskSpec) }
        .and_then(|disk| constant_bound_for(ctx, config, &disk, ConstantLeaf::Tag));
    let index = index.with_leaf_bound(leaf_bound);

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
        .map_while(|i| {
            if request_should_stop(ctx) {
                return None;
            }
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
            Some(into_child_iterator(child))
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
        QueryNodeType::LexRange => eval_lex_range(
            ctx,
            index,
            child,
            effective_weight,
            field,
            case_sensitive,
            max_prefix_expansions,
            config,
        ),
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
        let values = index.values().pattern_lending_iter(value, iter_mode);
        collect_filtered_readers(ctx, index, values, field.index, max_prefix_expansions)
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

/// Evaluate a `@tag:>{bound}` child, and its `>=`, `<` and `<=` spellings, as a
/// union of every indexed value inside the range.
#[expect(clippy::too_many_arguments)]
fn eval_lex_range<'index>(
    ctx: &'index mut QueryEvalContext,
    index: TagIndexRef,
    child: QueryNodeMut<'_>,
    effective_weight: f64,
    field: &ffi::FieldSpec,
    case_sensitive: bool,
    max_prefix_expansions: usize,
    config: Config,
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

    let children = collect_range_readers(
        ctx,
        index,
        RangeFilter {
            min: range_boundary(&begin),
            max: range_boundary(&end),
        },
        field.index,
        max_prefix_expansions,
    );
    let iter = match clause_bound {
        // SAFETY: the bound is owned by the AST node and, unlike a token, is
        // never rewritten during evaluation, so it outlives the query iterator
        // that retains it for profiling.
        Some(q_str) => unsafe {
            build_union_with_q_str(
                children,
                true,
                config.min_union_iter_heap,
                QueryNodeType::LexRange,
                q_str,
                effective_weight,
            )
        },
        None => build_union(
            children,
            true,
            config.min_union_iter_heap,
            QueryNodeType::LexRange,
            effective_weight,
        ),
    };
    Some(Evaluated::RustCompound(iter))
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

/// Open one reader per matching value, subject to the request limits and expansion cap.
fn collect_range_readers(
    ctx: &mut QueryEvalContext,
    index: TagIndexRef,
    filter: RangeFilter<'_>,
    field_index: ffi::t_fieldIndex,
    max_prefix_expansions: usize,
) -> Vec<CRQEIterator> {
    with_expansion_stop_check(ctx, |ctx, stop| {
        let values = index.values().range_iter(filter);
        let mut values = RangeLendingIter::from(values);
        values.set_stop_check(stop);

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
    })
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
        // SAFETY: the index and any non-null request limits are live for evaluation.
        // The pattern is readable for the call. The callee also reads `pattern[len]` while
        // scanning for `*` runs, so it relies on the NUL terminator that
        // `tok_ref`, an `RSTokenRefNulTerminated`, guarantees sits there.
        let matches = unsafe {
            ffi::TagIndex_GetSuffixWildcardMatches(
                index.as_ptr(),
                pattern.as_ptr().cast(),
                pattern.len() as u32,
                max_prefix_expansions as i64,
                ctx.sctx().limits,
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
            let values = index
                .values()
                .pattern_lending_iter(pattern, PatternMode::Wildcard);
            collect_filtered_readers(ctx, index, values, field.index, max_prefix_expansions)
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
        let values = index
            .values()
            .pattern_lending_iter(pattern, PatternMode::Wildcard);
        collect_filtered_readers(ctx, index, values, field.index, max_prefix_expansions)
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

/// Open a reader for each matching value, subject to the request limits and expansion cap.
fn collect_filtered_readers(
    ctx: &mut QueryEvalContext,
    index: TagIndexRef,
    values: PatternLendingIter<'_, '_, *mut c_void>,
    field_index: ffi::t_fieldIndex,
    max_prefix_expansions: usize,
) -> Vec<CRQEIterator> {
    with_expansion_stop_check(ctx, |ctx, stop| {
        let mut values = values;
        values.set_stop_check(stop);

        let mut children = Vec::new();
        while let Some((value, _)) = values.next() {
            if children.len() == max_prefix_expansions {
                ctx.status()
                    .warnings_mut()
                    .set_reached_max_prefix_expansions();
                break;
            }
            if memory_stop(ctx) {
                break;
            }
            children.extend(open_reader_child(ctx, index, value, field_index));
        }
        children
    })
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
    // SAFETY: the index and any non-null request limits are live for evaluation,
    // and `pattern` is readable for the call.
    let matches = unsafe {
        ffi::TagIndex_GetSuffixMatches(
            index.as_ptr(),
            pattern.as_ptr().cast(),
            pattern.len() as u32,
            contains,
            ctx.sctx().limits,
        )
    };
    // SAFETY: a non-null result is a fat array of `char **` elements, built
    // by the C array API and so aligned for them and freeable by `array_free`,
    // whose ownership the call hands to us. Its elements, the inner arrays,
    // stay owned by the suffix trie.
    let matches = unsafe { FatArray::new(NonNull::new(matches)?) };
    let mut children = Vec::new();
    let mut remaining = ffi::QUERY_REQUEST_TIMEOUT_COUNTER_LIMIT;
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
        if !extend_suffix_readers(
            ctx,
            index,
            arr.as_slice(),
            field_index,
            max_prefix_expansions,
            &mut remaining,
            &mut children,
        ) {
            break;
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
    let mut remaining = ffi::QUERY_REQUEST_TIMEOUT_COUNTER_LIMIT;
    extend_suffix_readers(
        ctx,
        index,
        matches,
        field_index,
        max_prefix_expansions,
        &mut remaining,
        &mut children,
    );
    children
}

/// Open readers from one suffix match bucket. Return false once expansion must stop.
fn extend_suffix_readers(
    ctx: &mut QueryEvalContext,
    index: TagIndexRef,
    matches: &[*mut std::ffi::c_char],
    field_index: ffi::t_fieldIndex,
    max_prefix_expansions: usize,
    remaining: &mut u32,
    children: &mut Vec<CRQEIterator>,
) -> bool {
    for &value in matches {
        if children.len() >= max_prefix_expansions {
            ctx.status()
                .warnings_mut()
                .set_reached_max_prefix_expansions();
            return false;
        }
        if memory_stop(ctx) || suffix_reader_should_stop(ctx, remaining) {
            return false;
        }
        debug_assert!(!value.is_null(), "a suffix match must carry a string");
        // SAFETY: the suffix trie stores NUL-terminated tag strings.
        let value = unsafe { CStr::from_ptr(value) }.to_bytes();
        children.extend(open_reader_child(ctx, index, value, field_index));
    }
    true
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
    let reader = NonNull::new(unsafe {
        ffi::TagIndex_OpenReader(
            index.as_ptr(),
            ctx.sctx_ptr(),
            value.as_ptr().cast(),
            value.len(),
            weight,
            field_index,
            ctx.status_ptr(),
        )
    })?;
    // SAFETY: `TagIndex_OpenReader` returns an owning query iterator with all
    // the callbacks `CRQEIterator` requires.
    Some(unsafe { with_constant_bound_c(reader, index.leaf_bound) })
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
struct TagIndexRef {
    index: NonNull<ffi::TagIndex>,
    /// The bound every reader opened on the index reports, from
    /// [`constant_bound_for`], or `None` for readers that report none.
    leaf_bound: Option<f64>,
}

impl TagIndexRef {
    /// Wrap a tag index pointer.
    ///
    /// # Safety
    ///
    /// `index` must point to a tag index that stays alive and is not
    /// restructured for as long as the query is being evaluated.
    const unsafe fn new(index: NonNull<ffi::TagIndex>) -> Self {
        Self {
            index,
            leaf_bound: None,
        }
    }

    /// The same index, whose readers report `leaf_bound`.
    const fn with_leaf_bound(self, leaf_bound: Option<f64>) -> Self {
        Self { leaf_bound, ..self }
    }

    /// The raw pointer to the tag index.
    const fn as_ptr(self) -> *mut ffi::TagIndex {
        self.index.as_ptr()
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

    /// Whether the index keeps a suffix trie.
    fn has_suffix(self) -> bool {
        // SAFETY: the index is live by the type invariant.
        unsafe { ffi::TagIndex_HasSuffix(self.as_ptr()) }
    }
}
