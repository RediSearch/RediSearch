/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Evaluation of `QN_TAG` query nodes.

use std::{ffi::CStr, marker::PhantomData, ptr::NonNull};

use query::{QueryNode, WildcardMode};
use query_flags::QEFlag;
use query_types::QueryNodeType;
use rqe_iterators::{
    Empty,
    c2rust::CRQEIterator,
    interop::RQEIteratorWrapper,
    union_opaque::{build_union, build_union_with_q_str},
};

use crate::{Config, Evaluated, QueryEvalContext, QueryNodeMut};

/// Address of the C `BAD_POINTER` sentinel returned when a wildcard pattern
/// cannot use the suffix trie.
const BAD_POINTER_ADDR: usize = 0xBAAAAAAD;

fn clock_deadline(ctx: &QueryEvalContext) -> Option<ffi::timespec> {
    let timeout = ctx.sctx().timeout;
    if timeout.is_null() {
        return None;
    }

    // SAFETY: the request timeout outlives query evaluation, and its active
    // source cannot change during an execution cycle.
    let kind = unsafe { (*timeout).kind };
    if kind != ffi::QueryRequestTimeoutKind_QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE {
        return None;
    }

    // SAFETY: `kind` established that `clock` is the active union member.
    Some(unsafe { (*timeout).source.clock.deadline })
}

/// `QN_TAG` — evaluate exact values and tag-specific expansions against a tag
/// field's own index.
///
/// Tag expansion limits come from [`QueryEvalContext::config`], preserving the
/// per-query snapshot and any request-local overrides. The supplied [`Config`]
/// contributes only evaluator-wide settings used to construct unions.
pub(crate) fn eval<'index>(
    ctx: &'index mut QueryEvalContext,
    mut node: QueryNodeMut<'_>,
    field: *const ffi::FieldSpec,
    config: Config,
) -> Option<Evaluated<'index>> {
    // SAFETY: tag-node construction guarantees a non-null field spec, and the
    // query's spec read lock keeps it and its tag index alive throughout
    // evaluation. Mutating child nodes cannot replace the field spec.
    let field = unsafe { &*field };
    // SAFETY: this is the active union member for the tag field referenced by a
    // well-formed tag node.
    let index = NonNull::new(unsafe { field.__bindgen_anon_1.tagOpts.tagIndex })?;

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

    let quick_exit = ctx.in_not_sub_tree() || weight == 0.0;
    let children = (0..num_children)
        .map(|i| {
            let child = node.child_mut(i);
            into_union_child(eval_child(
                ctx,
                index,
                child,
                weight,
                field,
                min_term_prefix,
                max_prefix_expansions,
                config,
            ))
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

#[expect(clippy::too_many_arguments)]
fn eval_child<'index>(
    ctx: &'index mut QueryEvalContext,
    index: NonNull<ffi::TagIndex>,
    mut child: QueryNodeMut<'_>,
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
    let effective_weight = if hybrid { 0.0 } else { weight };

    match child.node_type() {
        QueryNodeType::Token => {
            // SAFETY: tag token children come directly from query syntax, so
            // their writable NUL-terminated strings use the module allocator.
            let mut tok = unsafe { child.token_mut_nul_terminated() }
                .expect("a tag token child must carry a token");
            // SAFETY: the parser allocated this token with the module allocator.
            unsafe { tok.normalize_tag(case_sensitive) };
            let value = tok.as_ref().as_bytes().unwrap_or_default();
            open_reader(ctx, index, value, effective_weight, field.index).map(Evaluated::C)
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
            max_prefix_expansions,
            config,
        ),
        QueryNodeType::Phrase => {
            let mut value = Vec::new();
            for i in 0..child.num_children() {
                let mut term = child.child_mut(i);
                assert_eq!(
                    term.node_type(),
                    QueryNodeType::Token,
                    "tag phrase children must be tokens"
                );
                // SAFETY: tag phrase children come directly from query syntax.
                let mut tok = unsafe { term.token_mut_nul_terminated() }
                    .expect("a tag phrase term must carry a token");
                // SAFETY: the parser allocated this token with the module allocator.
                unsafe { tok.normalize_tag(case_sensitive) };
                if i != 0 {
                    value.push(b' ');
                }
                value.extend_from_slice(
                    tok.as_ref()
                        .as_c_str()
                        .expect("a tag phrase term must carry a string")
                        .to_bytes(),
                );
            }
            open_reader(ctx, index, &value, effective_weight, field.index).map(Evaluated::C)
        }
        _ => unreachable!("tag child grammar admits only token, prefix, wildcard and phrase nodes"),
    }
}

#[expect(clippy::too_many_arguments)]
fn eval_prefix<'index>(
    ctx: &'index mut QueryEvalContext,
    index: NonNull<ffi::TagIndex>,
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
    // SAFETY: parser-owned prefix tokens use the module allocator.
    unsafe { tok.normalize_tag(case_sensitive) };
    let tok_ref = tok.as_ref();
    if tok_ref.len() < min_term_prefix {
        return None;
    }

    let value = tok_ref.as_bytes().unwrap_or_default();
    let with_suffix_trie = field.options() & ffi::FieldSpecOptions_FieldSpec_WithSuffixTrie != 0;
    let children = if mode == WildcardMode::Prefix || !with_suffix_trie {
        let iter_mode = match mode {
            WildcardMode::Prefix => ffi::tag_iter_mode_TAG_PREFIX_MODE,
            WildcardMode::Suffix => ffi::tag_iter_mode_TAG_SUFFIX_MODE,
            WildcardMode::Contains => ffi::tag_iter_mode_TAG_CONTAINS_MODE,
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

#[expect(clippy::too_many_arguments)]
fn eval_wildcard<'index>(
    ctx: &'index mut QueryEvalContext,
    index: NonNull<ffi::TagIndex>,
    mut child: QueryNodeMut<'_>,
    effective_weight: f64,
    field: &ffi::FieldSpec,
    case_sensitive: bool,
    max_prefix_expansions: usize,
    config: Config,
) -> Option<Evaluated<'index>> {
    let mut tok = child
        .token_mut()
        .expect("a tag wildcard child must carry a token");
    // SAFETY: parser-owned wildcard tokens use the module allocator.
    unsafe { tok.normalize_tag(case_sensitive) };
    tok.remove_wildcard_escapes();
    let tok_ref = tok.as_ref();
    let pattern = tok_ref.as_bytes().unwrap_or_default();
    // SAFETY: `index` points to the live tag index borrowed for evaluation.
    let has_suffix = unsafe { ffi::TagIndex_HasSuffix(index.as_ptr()) };

    let children = if pattern.is_empty() {
        open_reader(ctx, index, b"", 1.0, field.index)
            .map(wrap_c_iterator)
            .into_iter()
            .collect()
    } else if has_suffix {
        let deadline = clock_deadline(ctx);
        let skip_timeout_checks = deadline.is_none();
        let timeout = deadline.unwrap_or(ffi::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        });
        // SAFETY: the index and search context stay alive for evaluation, and
        // the helper returns either null, the sentinel, or an owned fat array.
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
        if matches.is_null() {
            return None;
        }
        if matches.addr() == BAD_POINTER_ADDR {
            collect_filtered_readers(
                ctx,
                index,
                pattern,
                ffi::tag_iter_mode_TAG_WILDCARD_MODE,
                field.index,
                max_prefix_expansions,
            )
        } else {
            collect_wildcard_suffix_readers(ctx, index, matches, field.index, max_prefix_expansions)
        }
    } else {
        collect_filtered_readers(
            ctx,
            index,
            pattern,
            ffi::tag_iter_mode_TAG_WILDCARD_MODE,
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

fn collect_filtered_readers(
    ctx: &mut QueryEvalContext,
    index: NonNull<ffi::TagIndex>,
    pattern: &[u8],
    mode: ffi::tag_iter_mode,
    field_index: ffi::t_fieldIndex,
    max_prefix_expansions: usize,
) -> Vec<CRQEIterator> {
    // SAFETY: `index` is live, and `pattern` remains readable for the call.
    let iter = unsafe {
        ffi::TagIndex_IterateValuesWithFilter(
            index.as_ptr(),
            pattern.as_ptr().cast(),
            pattern.len(),
            mode,
        )
    };
    let iter = NonNull::new(iter).expect("tag values iterator allocation failed");
    let iter = TrieIteratorGuard(iter);
    if let Some(timeout) = clock_deadline(ctx) {
        // SAFETY: the guard owns a live trie iterator.
        unsafe { ffi::TrieMapIterator_SetTimeout(iter.0.as_ptr(), timeout) };
    }

    let mut children = Vec::new();
    loop {
        let mut value = std::ptr::null_mut();
        let mut len = 0;
        let mut payload = std::ptr::null_mut();
        // SAFETY: the guard owns the iterator and all out-pointers are writable.
        let has_next = unsafe {
            ffi::TrieMapIterator_Next(iter.0.as_ptr(), &mut value, &mut len, &mut payload)
        } != 0;
        if !has_next {
            break;
        }
        if children.len() == max_prefix_expansions {
            ctx.status()
                .warnings_mut()
                .set_reached_max_prefix_expansions();
            break;
        }
        // SAFETY: `Next` returned a value readable for `len` bytes until the
        // next iterator call; `open_reader` consumes it synchronously.
        let value = unsafe { std::slice::from_raw_parts(value.cast(), len as usize) };
        if let Some(reader) = open_reader(ctx, index, value, 1.0, field_index) {
            children.push(wrap_c_iterator(reader));
        }
    }
    children
}

fn collect_suffix_readers(
    ctx: &mut QueryEvalContext,
    index: NonNull<ffi::TagIndex>,
    pattern: &[u8],
    prefix: bool,
    field_index: ffi::t_fieldIndex,
    max_prefix_expansions: usize,
) -> Option<Vec<CRQEIterator>> {
    let deadline = clock_deadline(ctx);
    let skip_timeout_checks = deadline.is_none();
    let timeout = deadline.unwrap_or(ffi::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    });
    // SAFETY: `index` and the search context stay live; `pattern` is readable
    // for the call. A non-null result is an owned fat array.
    let matches = unsafe {
        ffi::TagIndex_GetSuffixMatches(
            index.as_ptr(),
            pattern.as_ptr().cast(),
            pattern.len() as u32,
            prefix,
            timeout,
            skip_timeout_checks,
        )
    };
    let matches = NonNull::new(matches)?;
    let matches = CArrayGuard::new(matches);
    let mut children = Vec::new();
    let outer_len = matches.len();
    for i in 0..outer_len {
        if children.len() >= max_prefix_expansions {
            break;
        }
        // SAFETY: `i` is within the outer fat array.
        let inner_ptr = unsafe { matches.ptr.as_ptr().add(i) };
        // SAFETY: `inner_ptr` addresses the initialized entry at `i`.
        let inner = unsafe { *inner_ptr };
        let inner_len = c_array_len(inner);
        for j in 0..inner_len {
            if children.len() >= max_prefix_expansions {
                ctx.status()
                    .warnings_mut()
                    .set_reached_max_prefix_expansions();
                break;
            }
            // SAFETY: `j` is within the borrowed inner fat array and its entry
            // is a NUL-terminated tag-index string.
            let value_ptr = unsafe { inner.add(j) };
            // SAFETY: `value_ptr` addresses the initialized entry at `j`.
            let value_ptr = unsafe { *value_ptr };
            // SAFETY: the suffix index stores NUL-terminated tag strings.
            let value = unsafe { CStr::from_ptr(value_ptr) }.to_bytes();
            if let Some(reader) = open_reader(ctx, index, value, 1.0, field_index) {
                children.push(wrap_c_iterator(reader));
            }
        }
    }
    Some(children)
}

fn collect_wildcard_suffix_readers(
    ctx: &mut QueryEvalContext,
    index: NonNull<ffi::TagIndex>,
    matches: *mut *mut std::ffi::c_char,
    field_index: ffi::t_fieldIndex,
    max_prefix_expansions: usize,
) -> Vec<CRQEIterator> {
    let matches = CArrayGuard::new(NonNull::new(matches).expect("matches must be non-null"));
    let mut children = Vec::new();
    for i in 0..matches.len() {
        if children.len() >= max_prefix_expansions {
            ctx.status()
                .warnings_mut()
                .set_reached_max_prefix_expansions();
            break;
        }
        // SAFETY: `i` is within the owned fat array and its entry is a
        // NUL-terminated tag-index string.
        let value_ptr = unsafe { matches.ptr.as_ptr().add(i) };
        // SAFETY: `value_ptr` addresses the initialized entry at `i`.
        let value_ptr = unsafe { *value_ptr };
        // SAFETY: the suffix index stores NUL-terminated tag strings.
        let value = unsafe { CStr::from_ptr(value_ptr) }.to_bytes();
        if let Some(reader) = open_reader(ctx, index, value, 1.0, field_index) {
            children.push(wrap_c_iterator(reader));
        }
    }
    children
}

fn open_reader(
    ctx: &mut QueryEvalContext,
    index: NonNull<ffi::TagIndex>,
    value: &[u8],
    weight: f64,
    field_index: ffi::t_fieldIndex,
) -> Option<NonNull<ffi::QueryIterator>> {
    // SAFETY: all pointers are live for the call. `value` is length-delimited,
    // and the C function either returns an owning iterator or null.
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

fn into_union_child(evaluated: Option<Evaluated<'_>>) -> CRQEIterator {
    let ptr = evaluated
        .map(Evaluated::into_c_iterator)
        .unwrap_or_else(|| RQEIteratorWrapper::boxed_new(Empty));
    wrap_c_iterator(NonNull::new(ptr).expect("evaluated child iterator must not be null"))
}

fn wrap_c_iterator(iter: NonNull<ffi::QueryIterator>) -> CRQEIterator {
    // SAFETY: tag readers and lowered Rust iterators are valid owning query
    // iterators with the callbacks required by `CRQEIterator`.
    unsafe { CRQEIterator::new(iter) }
}

/// Owning guard for a C trie iterator.
struct TrieIteratorGuard(NonNull<ffi::TrieMapIterator>);

impl Drop for TrieIteratorGuard {
    fn drop(&mut self) {
        // SAFETY: the guard uniquely owns the iterator returned by the tag index.
        unsafe { ffi::TrieMapIterator_Free(self.0.as_ptr()) };
    }
}

/// Owning guard for the outer C fat arrays returned by suffix helpers.
struct CArrayGuard<T> {
    ptr: NonNull<T>,
    _owner: PhantomData<T>,
}

impl<T> CArrayGuard<T> {
    const fn new(ptr: NonNull<T>) -> Self {
        Self {
            ptr,
            _owner: PhantomData,
        }
    }

    fn len(&self) -> usize {
        c_array_len(self.ptr.as_ptr())
    }
}

impl<T> Drop for CArrayGuard<T> {
    fn drop(&mut self) {
        // SAFETY: the guard uniquely owns the fat-array head returned by C.
        unsafe { ffi::array_free(self.ptr.as_ptr().cast()) };
    }
}

fn c_array_len<T>(array: *mut T) -> usize {
    // SAFETY: callers pass either a null pointer or a C fat-array head, both
    // accepted by `array_len_func`.
    unsafe { ffi::array_len_func(array.cast()) as usize }
}
