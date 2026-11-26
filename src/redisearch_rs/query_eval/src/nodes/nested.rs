/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Evaluation of `QN_NESTED` query nodes: a `@field:( … )` group on a NESTED field.
//!
//! The group's inner expression is translated into a [`nested_index::query::Expr`], planned into
//! branches, and each branch walks the field's routing levels. Every terminal reached opens the
//! iterators that apply the branch's condition on the last attribute, and the group is the UNION
//! of all of them. A document matches when one of its elements matches the whole expression; see
//! `docs/design/nested-compound-index.md`, §5.

use std::ops::Bound;
use std::ptr::{self, NonNull};

use ::query::{QueryNode, WildcardMode};
use field::{FieldExpirationPredicate, FieldFilterContext, FieldMaskOrIndex};
use inverted_index::NumericFilter;
use nested_index::query::{Branch, Clause, Expr, PlanError, Predicate, TerminalFilter, plan};
use nested_index::{CollectError, Constraint, NestedIndex, Terminal};
use query_error::QueryErrorCode;
use query_types::QueryNodeType;
use rqe_core::FieldIndex;
use rqe_iterators::utils::{NoTimeoutChecker, TimeoutChecker, TimeoutContextRequest};
use rqe_iterators::{NumericIteratorVariant, c2rust::CRQEIterator, union_opaque::build_union};
use tag_index::{InMemoryMode, IterMode, Tag, TagIndex, TrieLookup};

use crate::{Config, Evaluated, NestedLimits, QueryEvalContext, QueryNodeMut};

/// Why a group could not be evaluated.
#[derive(Debug, thiserror::Error)]
enum GroupError {
    /// A clause the group does not answer, which the parser only lets through without a spec.
    #[error(
        "only TAG and NUMERIC conditions on the group's sub-fields, with AND, OR and NOT, are \
         supported inside a NESTED group"
    )]
    Unsupported,
    /// The inner expression could not be planned.
    #[error("NESTED group: {0}")]
    Plan(#[from] PlanError),
    /// The alternatives together reach more terminals than [`NestedLimits::max_terminals`].
    #[error("NESTED group: the query reaches more than {max} terminals")]
    TooManyTerminals {
        /// The group's limit.
        max: usize,
    },
    /// An alternative's walk failed: the request timed out, or a range cuts a bucket.
    #[error("NESTED group: {0}")]
    Walk(CollectError),
    /// The group opens more iterators than [`NestedLimits::max_readers`].
    #[error("NESTED group: the query opens more than {max} posting lists")]
    TooManyReaders {
        /// The group's limit.
        max: usize,
    },
}

impl GroupError {
    const fn code(&self) -> QueryErrorCode {
        match self {
            Self::Plan(PlanError::TooManyBranches { .. }) => QueryErrorCode::Limit,
            Self::Unsupported | Self::Plan(_) => QueryErrorCode::Syntax,
            // The group opens nothing rather than the terminals reached so far; the request then
            // answers as for any timeout before the first read (`AREQ_EmptyTreeOnBuildTimeout`).
            Self::Walk(CollectError::TimedOut) => QueryErrorCode::TimedOut,
            Self::TooManyTerminals { .. } | Self::TooManyReaders { .. } | Self::Walk(_) => {
                QueryErrorCode::Limit
            }
        }
    }
}

/// `QN_NESTED` — the UNION of the iterators of every terminal the group's alternatives reach.
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
    let field_ptr = NonNull::new(unsafe { spec.fields.add(field_index as usize) })
        .expect("a spec with fields has a field array");
    // SAFETY: `field_ptr` points into the live spec's field array, which the
    // query's spec read lock keeps alive and unmodified throughout evaluation.
    // Going through the raw pointer detaches the borrow from `ctx`.
    let field = unsafe { field_ptr.as_ref() };
    // The parser only builds a group on a NESTED field.
    assert!(
        field.types() & ffi::FieldType_INDEXFLD_T_NESTED != 0,
        "a nested node must reference a NESTED field"
    );
    // SAFETY: the field is NESTED, so `nestedOpts` is the active union member.
    let opts = unsafe { &field.__bindgen_anon_1.nestedOpts };
    let def = NonNull::new(opts.def).expect("a NESTED field has its definition");
    // SAFETY: the definition is built with the field and freed with it, so it lives as long as
    // the spec the query holds.
    let def = unsafe { def.as_ref() };
    // A field whose index could not be built matches nothing.
    let index = NonNull::new(opts.nestedIndex.cast::<NestedIndex>())?;
    // SAFETY: the index belongs to the field; the query's spec read lock keeps it
    // alive and excludes writers during evaluation.
    let index = unsafe { index.as_ref() };

    let sctx = NonNull::new(ctx.sctx_ptr().cast_mut()).expect("a query has a search context");
    let opened = match NonNull::new(ctx.sctx().limits) {
        Some(limits) => {
            // SAFETY: the request owns its limits at a stable address and outlives evaluation.
            // The checker is dropped when `open` returns, before the request can start another
            // execution cycle, so no source change or deadline write overlaps a probe.
            let mut timeout =
                unsafe { TimeoutContextRequest::new(limits, WALK_TIMEOUT_GRANULARITY) };
            open(
                index,
                def,
                &mut node,
                sctx,
                field_index,
                config,
                &mut timeout,
            )
        }
        None => open(
            index,
            def,
            &mut node,
            sctx,
            field_index,
            config,
            &mut NoTimeoutChecker,
        ),
    };
    match opened {
        Ok(readers) => {
            if readers.reached_max_prefix_expansions {
                ctx.status()
                    .warnings_mut()
                    .set_reached_max_prefix_expansions();
            }
            if readers.out.is_empty() {
                return None;
            }
            let weight = node.opts().weight;
            let iter = build_union(
                readers.out,
                true,
                config.min_union_iter_heap,
                QueryNodeType::Nested,
                weight,
            );
            Some(Evaluated::RustCompound(iter))
        }
        Err(err) => {
            ctx.status().set_error(err.code(), &err.to_string());
            None
        }
    }
}

/// How many nodes a routing walk enters between two reads of the clock, as the trie walks of
/// prefix expansion do.
const WALK_TIMEOUT_GRANULARITY: u32 = 100;

/// The iterators of every terminal the group `node` reaches. `timeout` is shared by all the
/// group's alternatives.
fn open(
    index: &NestedIndex,
    def: &ffi::NestedDef,
    node: &mut QueryNodeMut<'_>,
    sctx: NonNull<ffi::RedisSearchCtx>,
    field_index: FieldIndex,
    config: Config,
    timeout: &mut impl TimeoutChecker,
) -> Result<Readers, GroupError> {
    let limits = config.nested_limits;
    debug_assert_eq!(node.num_children(), 1, "a group has one inner expression");
    let inner = translate(node.child_mut(0), def, config.min_term_prefix as usize)?;
    let branches = plan(index, &inner.borrowed(), limits.max_branches)?;

    let node_ptr = node.as_non_null();
    let mut readers = Readers {
        out: Vec::new(),
        sctx,
        field_index,
        limits,
        max_prefix_expansions: config.max_prefix_expansions,
        reached_max_prefix_expansions: false,
    };
    // The terminal budget is shared by all alternatives.
    let mut terminals_left = limits.max_terminals;
    for branch in &branches {
        let terminals = index
            .routing()
            .collect_terminals(&branch.routing, terminals_left, timeout)
            .map_err(|err| match err {
                // Named by the group's own limit, not what this alternative had left of it.
                CollectError::TooManyTerminals { .. } => GroupError::TooManyTerminals {
                    max: limits.max_terminals,
                },
                err => GroupError::Walk(err),
            })?;
        terminals_left -= terminals.len();
        // SAFETY: `node_ptr` is the live QN_NESTED node under evaluation, part of the AST that
        // outlives every iterator of the query.
        unsafe { readers.open_branch(branch, &terminals, node_ptr) }?;
    }
    Ok(readers)
}

/// The iterators a group opens, one per numeric range or tag value of each terminal reached.
struct Readers {
    out: Vec<CRQEIterator>,
    sctx: NonNull<ffi::RedisSearchCtx>,
    field_index: FieldIndex,
    limits: NestedLimits,
    /// The readers one prefix opens in a branch, across all its terminals, as
    /// [`Config::max_prefix_expansions`] caps a top-level tag prefix.
    max_prefix_expansions: usize,
    /// Set once a prefix stopped at [`Readers::max_prefix_expansions`].
    reached_max_prefix_expansions: bool,
}

impl Readers {
    /// Opens the iterators of `branch` on each of its `terminals`.
    ///
    /// # Safety
    ///
    /// 1. `node` is the live `QN_NESTED` node under evaluation, part of an AST that outlives
    ///    every iterator opened here.
    unsafe fn open_branch(
        &mut self,
        branch: &Branch<'_>,
        terminals: &[&Terminal],
        node: NonNull<ffi::RSQueryNode>,
    ) -> Result<(), GroupError> {
        match &branch.terminal {
            TerminalFilter::Numeric { start, end } => {
                // SAFETY: guaranteed by 1.
                let filter = unsafe { self.numeric_filter(*start, *end, node) };
                for terminal in terminals {
                    let tree = terminal
                        .numeric_ptr()
                        .expect("the index's terminals are all numeric");
                    let field_ctx = self.field_ctx();
                    // SAFETY: 1. the search context and its spec outlive the query's
                    // iterators. 2. `field_ctx` names the field by index. 3. `tree` is the
                    // owner's pointer to the terminal's tree (`Terminal::numeric_ptr`), live as
                    // long as the field; writers take the spec write lock, which the query
                    // releases only while its iterators are parked, revalidating them after.
                    let variants = unsafe {
                        NumericIteratorVariant::from_tree(tree, self.sctx, filter, &field_ctx)
                    };
                    for variant in variants {
                        self.push(CRQEIterator::from_rust_leaf(variant))?;
                    }
                }
            }
            TerminalFilter::Tag(constraint) => {
                let mut expansions_left = self.max_prefix_expansions;
                for terminal in terminals {
                    let tags = terminal.tag().expect("the index's terminals are all tag");
                    let tags_ptr = terminal
                        .tag_ptr()
                        .expect("the index's terminals are all tag");
                    match constraint {
                        Constraint::TagIn(values) => {
                            // A value with a NUL byte is never indexed.
                            for tag in values.values().iter().filter_map(|value| Tag::new(value)) {
                                // SAFETY: `tags_ptr` is the owner pointer of `tags`' terminal.
                                unsafe { self.open_tag(tags, tags_ptr, tag) }?;
                            }
                        }
                        Constraint::Any | Constraint::TagNotIn(_) => {
                            let excluded = match constraint {
                                Constraint::TagNotIn(excluded) => Some(excluded),
                                _ => None,
                            };
                            let mut it = tags.value_iter();
                            while let Some((tag, _)) = it.advance() {
                                if !excluded
                                    .is_some_and(|excluded| excluded.contains(tag.as_bytes()))
                                {
                                    // SAFETY: `tags_ptr` is the owner pointer of `tags`' terminal.
                                    unsafe { self.open_tag(tags, tags_ptr, tag) }?;
                                }
                            }
                        }
                        Constraint::TagPrefix(prefix) => {
                            let Some(prefix) = Tag::new(prefix) else {
                                continue;
                            };
                            let mut it = tags.value_iter_filtered(prefix, IterMode::Prefix);
                            while let Some((tag, _)) = it.advance() {
                                if expansions_left == 0 {
                                    self.reached_max_prefix_expansions = true;
                                    break;
                                }
                                expansions_left -= 1;
                                // SAFETY: `tags_ptr` is the owner pointer of `tags`' terminal.
                                unsafe { self.open_tag(tags, tags_ptr, tag) }?;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Opens the reader of one value of a tag terminal, if any document holds it.
    ///
    /// # Safety
    ///
    /// 1. `tags_ptr` is the owner's pointer to `tags`, from [`Terminal::tag_ptr`] of the
    ///    terminal `tags` was borrowed from.
    unsafe fn open_tag(
        &mut self,
        tags: &TagIndex<InMemoryMode>,
        tags_ptr: NonNull<TagIndex<InMemoryMode>>,
        tag: Tag<'_>,
    ) -> Result<(), GroupError> {
        // SAFETY: 1. `tags_ptr` is the owner's pointer to the terminal's index, by 1. 2. It
        // stays valid as long as the field, which outlives the query's iterators. 3. The index
        // is mutated only under the spec write lock, which the query releases only while its
        // iterators are parked, revalidating them after.
        let lookup = unsafe { TrieLookup::new(tags_ptr) };
        // SAFETY: 1. `tags` outlives the iterator, as for `tags_ptr` above, and is mutated only
        // under the revalidation protocol. 2. The search context and its spec outlive the
        // query's iterators. 3. `lookup` resolves `tags`, the index behind `tags_ptr`.
        let reader = unsafe { tags.open_reader(self.sctx, tag, 1.0, self.field_index, lookup) };
        match reader {
            Some(reader) => self.push(CRQEIterator::from_rust_leaf(reader)),
            None => Ok(()),
        }
    }

    fn push(&mut self, iterator: CRQEIterator) -> Result<(), GroupError> {
        if self.out.len() == self.limits.max_readers {
            return Err(GroupError::TooManyReaders {
                max: self.limits.max_readers,
            });
        }
        self.out.push(iterator);
        Ok(())
    }

    const fn field_ctx(&self) -> FieldFilterContext {
        FieldFilterContext {
            field: FieldMaskOrIndex::Index(self.field_index),
            predicate: FieldExpirationPredicate::Default,
        }
    }

    /// A filter for a numeric terminal's range, handed to the group's node, which frees it with
    /// the AST.
    ///
    /// # Safety
    ///
    /// 1. `node` is the live `QN_NESTED` node under evaluation.
    /// 2. The returned filter is not used once the AST holding `node` is freed.
    unsafe fn numeric_filter<'ast>(
        &self,
        start: Bound<f64>,
        end: Bound<f64>,
        node: NonNull<ffi::RSQueryNode>,
    ) -> &'ast NumericFilter {
        let (min, min_inclusive) = match start {
            Bound::Included(v) => (v, true),
            Bound::Excluded(v) => (v, false),
            Bound::Unbounded => (f64::NEG_INFINITY, true),
        };
        let (max, max_inclusive) = match end {
            Bound::Included(v) => (v, true),
            Bound::Excluded(v) => (v, false),
            Bound::Unbounded => (f64::INFINITY, true),
        };
        // SAFETY: plain value arguments; a null geo filter makes it a numeric filter.
        let filter = unsafe {
            ffi::NewNumericFilter(
                min,
                max,
                min_inclusive,
                max_inclusive,
                true,
                self.field_index,
                ptr::null(),
            )
        };
        let filter = NonNull::new(filter).expect("the module allocator does not return null");
        // SAFETY: guaranteed by 1; the node takes ownership of the filter.
        unsafe { ffi::QueryNestedNode_AddFilter(node.as_ptr(), filter.as_ptr()) };
        // SAFETY: `ffi::NumericFilter` and `NumericFilter` share their `#[repr(C)]` layout (the
        // former is generated from the latter). The filter lives as long as the AST, which 2.
        // bounds the reference by, and is never written again.
        unsafe { filter.cast::<NumericFilter>().as_ref() }
    }
}

/// A group's inner expression, with its tag values copied out of the query nodes.
///
/// The values cannot be borrowed instead: a payload read from a [`QueryNodeMut`] borrows that
/// node's view, which lives only while its parent is reborrowed for it, so no borrow outlives the
/// walk on to the next sibling.
enum Owned {
    Tags {
        level: usize,
        values: Vec<Vec<u8>>,
    },
    Prefix {
        level: usize,
        prefix: Vec<u8>,
    },
    Range {
        level: usize,
        start: Bound<f64>,
        end: Bound<f64>,
    },
    And(Vec<Owned>),
    Or(Vec<Owned>),
    Not(Box<Owned>),
}

impl Owned {
    fn borrowed(&self) -> Expr<'_> {
        let clause = |level: usize, predicate| Expr::Clause(Clause { level, predicate });
        match self {
            Self::Tags { level, values } => clause(
                *level,
                Predicate::TagIn(values.iter().map(Vec::as_slice).collect()),
            ),
            Self::Prefix { level, prefix } => clause(*level, Predicate::TagPrefix(prefix)),
            Self::Range { level, start, end } => clause(
                *level,
                Predicate::Range {
                    start: *start,
                    end: *end,
                },
            ),
            Self::And(children) => Expr::And(children.iter().map(Self::borrowed).collect()),
            Self::Or(children) => Expr::Or(children.iter().map(Self::borrowed).collect()),
            Self::Not(child) => Expr::Not(Box::new(child.borrowed())),
        }
    }
}

/// The level of `INDEX` that sub-field `sub_field` is.
fn level_of(def: &ffi::NestedDef, sub_field: u8) -> Result<usize, GroupError> {
    def.levels[..usize::from(def.numLevels)]
        .iter()
        .position(|&level| level == sub_field)
        .ok_or(GroupError::Unsupported)
}

/// The lower end of a numeric filter: unbounded only for an included `-inf`, which every value
/// is above. An excluded `-inf` still leaves out an element whose value is `-inf`.
fn start_of(nf: &NumericFilter) -> Bound<f64> {
    match (nf.min == f64::NEG_INFINITY, nf.min_inclusive) {
        (true, true) => Bound::Unbounded,
        (_, true) => Bound::Included(nf.min),
        (_, false) => Bound::Excluded(nf.min),
    }
}

/// The upper end of a numeric filter, as in [`start_of`].
fn end_of(nf: &NumericFilter) -> Bound<f64> {
    match (nf.max == f64::INFINITY, nf.max_inclusive) {
        (true, true) => Bound::Unbounded,
        (_, true) => Bound::Included(nf.max),
        (_, false) => Bound::Excluded(nf.max),
    }
}

/// Translates the inner expression of a group, normalising its tag values as the sub-fields
/// store them.
///
/// A prefix shorter than `min_term_prefix` matches nothing, as a top-level tag prefix does.
fn translate(
    mut node: QueryNodeMut<'_>,
    def: &ffi::NestedDef,
    min_term_prefix: usize,
) -> Result<Owned, GroupError> {
    enum Kind {
        And,
        Or,
        Not,
        Nothing,
        Tag(u8),
        Range(u8, Bound<f64>, Bound<f64>),
    }
    let kind = match node.as_enum() {
        QueryNode::Phrase { exact: false } => Kind::And,
        QueryNode::Union => Kind::Or,
        QueryNode::Not => Kind::Not,
        // A clause that is only stopwords.
        QueryNode::Null => Kind::Nothing,
        QueryNode::Tag {
            sub_field: Some(sub_field),
            ..
        } => Kind::Tag(sub_field),
        QueryNode::Numeric {
            nf,
            sub_field: Some(sub_field),
        } if nf.is_numeric_filter() => Kind::Range(sub_field, start_of(nf), end_of(nf)),
        _ => return Err(GroupError::Unsupported),
    };
    let children = |node: &mut QueryNodeMut<'_>| -> Result<Vec<Owned>, GroupError> {
        (0..node.num_children())
            .map(|i| translate(node.child_mut(i), def, min_term_prefix))
            .collect()
    };
    Ok(match kind {
        Kind::And => Owned::And(children(&mut node)?),
        Kind::Or => Owned::Or(children(&mut node)?),
        Kind::Not => Owned::Not(Box::new(
            children(&mut node)?
                .pop()
                .expect("a NOT node has one child"),
        )),
        Kind::Nothing => Owned::Or(Vec::new()),
        Kind::Range(sub_field, start, end) => Owned::Range {
            level: level_of(def, sub_field)?,
            start,
            end,
        },
        Kind::Tag(sub_field) => {
            let level = level_of(def, sub_field)?;
            let case_sensitive = def.subFields[usize::from(sub_field)].caseSensitive;
            let mut values = Vec::new();
            let mut alternatives = Vec::new();
            for i in 0..node.num_children() {
                let mut child = node.child_mut(i);
                match child.node_type() {
                    QueryNodeType::Token => {
                        // SAFETY: invariant (5) of `QueryNodeMut::new` extends the token string
                        // requirements to the token children of a tag node.
                        let mut tok = unsafe { child.token_node_mut() }
                            .expect("a tag token child must carry a token");
                        // SAFETY: invariant (5) of `QueryNodeMut::new` puts this token in the
                        // module allocator whenever the sub-field is case-insensitive.
                        unsafe { tok.normalize_tag(case_sensitive) };
                        values.push(token_bytes(&tok.as_ref())?);
                    }
                    QueryNodeType::Phrase => {
                        // `{multi word}`: one value, its words joined by a space.
                        let mut value = Vec::new();
                        for j in 0..child.num_children() {
                            let mut term = child.child_mut(j);
                            // SAFETY: as for a token child, invariant (5) of
                            // `QueryNodeMut::new` covers a tag phrase's terms.
                            let mut tok =
                                unsafe { term.token_node_mut() }.ok_or(GroupError::Unsupported)?;
                            // SAFETY: as above.
                            unsafe { tok.normalize_tag(case_sensitive) };
                            if j != 0 {
                                value.push(b' ');
                            }
                            value.extend(token_bytes(&tok.as_ref())?);
                        }
                        values.push(value);
                    }
                    QueryNodeType::Prefix => {
                        let mode = match child.as_enum() {
                            QueryNode::Prefix { mode, .. } => mode,
                            _ => unreachable!("the node type is Prefix"),
                        };
                        if !matches!(mode, WildcardMode::Prefix) {
                            return Err(GroupError::Unsupported);
                        }
                        let mut tok = child
                            .token_mut()
                            .expect("a tag prefix child must carry a token");
                        // SAFETY: invariant (5) of `QueryNodeMut::new`, as for a token child.
                        unsafe { tok.normalize_tag(case_sensitive) };
                        let tok = tok.as_ref();
                        if tok.len() >= min_term_prefix {
                            alternatives.push(Owned::Prefix {
                                level,
                                prefix: token_bytes(&tok)?,
                            });
                        }
                    }
                    _ => return Err(GroupError::Unsupported),
                }
            }
            if !values.is_empty() {
                alternatives.push(Owned::Tags { level, values });
            }
            // With every prefix too short and no value, the empty OR: nothing.
            if alternatives.len() == 1 {
                alternatives.pop().expect("one alternative")
            } else {
                Owned::Or(alternatives)
            }
        }
    })
}

fn token_bytes<const NUL_TERMINATED: bool>(
    tok: &rs_token::RSTokenRef<'_, NUL_TERMINATED>,
) -> Result<Vec<u8>, GroupError> {
    tok.as_bytes()
        .map(<[u8]>::to_vec)
        .ok_or(GroupError::Unsupported)
}
