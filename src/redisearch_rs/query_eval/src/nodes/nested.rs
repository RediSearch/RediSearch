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
use nested_index::query::{Branch, Clause, Expr, Predicate, TerminalFilter, plan};
use nested_index::{Constraint, NestedIndex, Terminal, WalkLimits};
use query_error::QueryErrorCode;
use query_types::QueryNodeType;
use rqe_core::FieldIndex;
use rqe_iterators::{NumericIteratorVariant, c2rust::CRQEIterator, union_opaque::build_union};
use tag_index::{IterMode, Tag, TrieLookup};

use crate::{Config, Evaluated, QueryEvalContext, QueryNodeMut};

/// The most alternatives a group's inner expression may expand to once its ORs are distributed.
pub const MAX_BRANCHES: usize = 16;

/// The most terminals one group may read, across all its alternatives.
pub const MAX_TERMINALS: usize = 10_000;

/// The most routing nodes one group may enter, across all its alternatives.
pub const MAX_NODES: usize = 100_000;

/// The most iterators one group may open: one per numeric range of a numeric terminal, one per
/// tag value of a tag terminal.
pub const MAX_READERS: usize = 100_000;

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
    let field_ptr = unsafe { spec.fields.add(field_index as usize) };
    // SAFETY: `field_ptr` points into the live spec's field array, which the
    // query's spec read lock keeps alive and unmodified throughout evaluation.
    // Going through the raw pointer detaches the borrow from `ctx`.
    let field = unsafe { &*field_ptr };
    // The parser only builds a group on a NESTED field.
    assert!(
        field.types() & ffi::FieldType_INDEXFLD_T_NESTED != 0,
        "a nested node must reference a NESTED field"
    );
    // SAFETY: the field is NESTED, so `nestedOpts` is the active union member.
    let opts = unsafe { &field.__bindgen_anon_1.nestedOpts };
    // SAFETY: a NESTED field always has its definition, built with the field.
    let def = unsafe { &*opts.def };
    // A field whose index could not be built matches nothing.
    let index = NonNull::new(opts.nestedIndex.cast::<NestedIndex>())?;
    // SAFETY: the index belongs to the field; the query's spec read lock keeps it
    // alive and excludes writers during evaluation.
    let index = unsafe { index.as_ref() };

    let num_children = node.num_children();
    debug_assert_eq!(num_children, 1, "a group has one inner expression");
    let inner = match translate(node.child_mut(0), def) {
        Ok(inner) => inner,
        Err(message) => {
            ctx.status().set_error(QueryErrorCode::Syntax, &message);
            return None;
        }
    };
    let expr = inner.borrowed();
    let branches = match plan(index, &expr, MAX_BRANCHES) {
        Ok(branches) => branches,
        Err(err) => {
            ctx.status()
                .set_error(QueryErrorCode::Syntax, &format!("NESTED group: {err}"));
            return None;
        }
    };

    let node_ptr = node.as_non_null();
    let sctx = NonNull::new(ctx.sctx_ptr().cast_mut()).expect("a query has a search context");
    let mut readers = Readers {
        out: Vec::new(),
        sctx,
        field_index,
    };
    let mut limits = WalkLimits {
        max_terminals: MAX_TERMINALS,
        max_nodes: MAX_NODES,
    };
    for branch in &branches {
        let terminals = match index.routing().collect_terminals(&branch.routing, limits) {
            Ok(terminals) => terminals,
            Err(err) => {
                ctx.status()
                    .set_error(QueryErrorCode::Limit, &format!("NESTED group: {err}"));
                return None;
            }
        };
        // Later alternatives share what is left of the budget. Nodes are not counted by the walk's
        // result, so each alternative may enter up to the whole node budget.
        limits.max_terminals -= terminals.len();
        if let Err(message) = readers.open_branch(branch, &terminals, node_ptr) {
            ctx.status().set_error(QueryErrorCode::Limit, &message);
            return None;
        }
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

/// The iterators a group opens, one per numeric range or tag value of each terminal reached.
struct Readers {
    out: Vec<CRQEIterator>,
    sctx: NonNull<ffi::RedisSearchCtx>,
    field_index: FieldIndex,
}

impl Readers {
    fn open_branch(
        &mut self,
        branch: &Branch<'_>,
        terminals: &[&Terminal],
        node: NonNull<ffi::RSQueryNode>,
    ) -> Result<(), String> {
        match &branch.terminal {
            TerminalFilter::Numeric { start, end } => {
                let filter = self.numeric_filter(*start, *end, node);
                for terminal in terminals {
                    let tree = terminal
                        .numeric_ptr()
                        .expect("the index's terminals are all numeric");
                    let field_ctx = self.field_ctx();
                    // SAFETY: 1. the search context and its spec outlive the query's
                    // iterators. 2. `field_ctx` names the field by index. 3. `tree` is the
                    // owner's pointer to the terminal's tree, which lives as long as the field;
                    // writers take the spec write lock, released only while iterators are
                    // parked and revalidated on re-acquire.
                    let variants = unsafe {
                        NumericIteratorVariant::from_tree(tree, self.sctx, filter, &field_ctx)
                    };
                    for variant in variants {
                        self.push(CRQEIterator::from_rust_leaf(variant))?;
                    }
                }
            }
            TerminalFilter::Tag(constraint) => {
                for terminal in terminals {
                    let tags_ptr = terminal
                        .tag_ptr()
                        .expect("the index's terminals are all tag");
                    let tags = terminal.tag().expect("the index's terminals are all tag");
                    let values: Vec<Vec<u8>> = match constraint {
                        Constraint::TagIn(values) => values.iter().map(|v| v.to_vec()).collect(),
                        Constraint::Any | Constraint::TagNotIn(_) => {
                            let mut all = Vec::new();
                            let mut it = tags.value_iter();
                            while let Some((tag, _)) = it.advance() {
                                all.push(tag.as_bytes().to_vec());
                            }
                            if let Constraint::TagNotIn(excluded) = constraint {
                                all.retain(|v| !excluded.contains(&v.as_slice()));
                            }
                            all
                        }
                        Constraint::TagPrefix(prefix) => {
                            let Some(prefix) = Tag::new(prefix) else {
                                continue;
                            };
                            let mut all = Vec::new();
                            let mut it = tags.value_iter_filtered(prefix, IterMode::Prefix);
                            while let Some((tag, _)) = it.advance() {
                                all.push(tag.as_bytes().to_vec());
                            }
                            all
                        }
                        Constraint::BucketRange(_) => {
                            unreachable!("a tag terminal's filter is a tag constraint")
                        }
                    };
                    for value in &values {
                        // A value with a NUL byte was never indexed.
                        let Some(tag) = Tag::new(value) else {
                            continue;
                        };
                        // SAFETY: `tags_ptr` is the owner's pointer to the terminal's index, valid
                        // as long as the field, and mutated only under the spec write lock, which
                        // the query releases only while its iterators are parked.
                        let lookup = unsafe { TrieLookup::new(tags_ptr) };
                        // SAFETY: 1. the terminal outlives the query's iterators, as above.
                        // 2. The search context and its spec outlive them too. 3. `lookup`
                        // resolves `tags`, the index behind `tags_ptr`.
                        let reader = unsafe {
                            tags.open_reader(self.sctx, tag, 1.0, self.field_index, lookup)
                        };
                        if let Some(reader) = reader {
                            self.push(CRQEIterator::from_rust_leaf(reader))?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn push(&mut self, iterator: CRQEIterator) -> Result<(), String> {
        if self.out.len() == MAX_READERS {
            return Err(format!(
                "NESTED group: the query opens more than {MAX_READERS} posting lists"
            ));
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

    /// A filter for a numeric terminal's range, owned by the group's node so it lives as long as
    /// the iterators that read it.
    fn numeric_filter(
        &self,
        start: Bound<f64>,
        end: Bound<f64>,
        node: NonNull<ffi::RSQueryNode>,
    ) -> &'static NumericFilter {
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
        // SAFETY: `node` is the live QN_NESTED node under evaluation; it takes ownership of the
        // filter and frees it with the AST.
        unsafe { ffi::QueryNestedNode_AddFilter(node.as_ptr(), filter) };
        // SAFETY: `ffi::NumericFilter` and `NumericFilter` share their `#[repr(C)]` layout (the
        // former is generated from the latter). The filter is freed with the AST, which outlives
        // every iterator of the query, and is never written again.
        unsafe { &*filter.cast::<NumericFilter>() }
    }
}

/// A group's inner expression, with its tag values copied out of the query nodes.
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
fn level_of(def: &ffi::NestedDef, sub_field: u8) -> usize {
    def.levels[..usize::from(def.numLevels)]
        .iter()
        .position(|&level| level == sub_field)
        .expect("INDEX lists every sub-field")
}

const UNSUPPORTED: &str = "only TAG and NUMERIC conditions on the group's sub-fields, with AND, OR and NOT, are supported inside a NESTED group";

/// Translates the inner expression of a group, normalising its tag values as the sub-fields
/// store them.
fn translate(mut node: QueryNodeMut<'_>, def: &ffi::NestedDef) -> Result<Owned, String> {
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
        QueryNode::Tag { sub_field, .. } => Kind::Tag(sub_field),
        QueryNode::Numeric { nf, sub_field } => {
            if !nf.is_numeric_filter() {
                return Err(UNSUPPORTED.to_owned());
            }
            let start = match (nf.min == f64::NEG_INFINITY, nf.min_inclusive) {
                (true, _) => Bound::Unbounded,
                (false, true) => Bound::Included(nf.min),
                (false, false) => Bound::Excluded(nf.min),
            };
            let end = match (nf.max == f64::INFINITY, nf.max_inclusive) {
                (true, _) => Bound::Unbounded,
                (false, true) => Bound::Included(nf.max),
                (false, false) => Bound::Excluded(nf.max),
            };
            Kind::Range(sub_field, start, end)
        }
        _ => return Err(UNSUPPORTED.to_owned()),
    };
    let children = |node: &mut QueryNodeMut<'_>| -> Result<Vec<Owned>, String> {
        (0..node.num_children())
            .map(|i| translate(node.child_mut(i), def))
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
            level: level_of(def, sub_field),
            start,
            end,
        },
        Kind::Tag(sub_field) => {
            let level = level_of(def, sub_field);
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
                            let mut tok = unsafe { term.token_node_mut() }
                                .ok_or_else(|| UNSUPPORTED.to_owned())?;
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
                            return Err(UNSUPPORTED.to_owned());
                        }
                        let mut tok = child
                            .token_mut()
                            .expect("a tag prefix child must carry a token");
                        // SAFETY: invariant (5) of `QueryNodeMut::new`, as for a token child.
                        unsafe { tok.normalize_tag(case_sensitive) };
                        alternatives.push(Owned::Prefix {
                            level,
                            prefix: token_bytes(&tok.as_ref())?,
                        });
                    }
                    _ => return Err(UNSUPPORTED.to_owned()),
                }
            }
            if !values.is_empty() {
                alternatives.push(Owned::Tags { level, values });
            }
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
) -> Result<Vec<u8>, String> {
    tok.as_bytes()
        .map(<[u8]>::to_vec)
        .ok_or_else(|| UNSUPPORTED.to_owned())
}
