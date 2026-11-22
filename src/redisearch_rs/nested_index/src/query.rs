/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Planning a query group, `@items:( … )`, into walks of the routing levels.
//!
//! A group's inner expression is an [`Expr`] over [`Clause`]s, each on one level. Planning turns
//! it into disjunctive normal form: one [`Branch`] per conjunction, each with one [`Constraint`]
//! per routing level and a [`TerminalFilter`] on the terminal. A document matches the group when
//! one of its elements matches some branch, so the caller UNIONs the branches' terminals.

use std::ops::Bound;

use compound_index::{BucketRange, Constraint, Level};

use crate::{NestedIndex, TerminalKind};

/// A condition on one level of a group, in `INDEX` order: the routing levels first, the terminal
/// last.
#[derive(Debug, Clone, PartialEq)]
pub struct Clause<'a> {
    /// The level the condition is on.
    pub level: usize,
    /// What the element's value on that level must satisfy.
    pub predicate: Predicate<'a>,
}

/// What an element's value on one level must satisfy.
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate<'a> {
    /// The tag is one of the values: `@color:{red|blue}`.
    TagIn(Vec<&'a [u8]>),
    /// The tag starts with the prefix: `@color:{re*}`.
    TagPrefix(&'a [u8]),
    /// The number is in the range: `@price:[lo hi]`, each end inclusive, exclusive or unbounded.
    Range {
        /// The lower end.
        start: Bound<f64>,
        /// The upper end.
        end: Bound<f64>,
    },
}

/// The inner expression of a group.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr<'a> {
    /// One condition.
    Clause(Clause<'a>),
    /// Every child holds for the same element.
    And(Vec<Expr<'a>>),
    /// Some child holds for the element.
    Or(Vec<Expr<'a>>),
    /// The child does not hold for the element.
    Not(Box<Expr<'a>>),
}

/// What a terminal's iterator must apply: the condition on the last `INDEX` attribute.
#[derive(Debug, Clone, PartialEq)]
pub enum TerminalFilter<'a> {
    /// Every value of a tag terminal, through [`Constraint::Any`], or the given tag values.
    Tag(Constraint<'a>),
    /// The values of a numeric terminal within the range.
    Numeric {
        /// The lower end.
        start: Bound<f64>,
        /// The upper end.
        end: Bound<f64>,
    },
}

/// One conjunction of a group, ready to walk.
///
/// Every tag set in it, in a [`Constraint::TagIn`] or [`Constraint::TagNotIn`], is sorted and
/// holds each value once, so a caller can binary-search it.
#[derive(Debug, Clone, PartialEq)]
pub struct Branch<'a> {
    /// One constraint per routing level, for
    /// [`CompoundIndex::collect_terminals`](compound_index::CompoundIndex::collect_terminals).
    pub routing: Vec<Constraint<'a>>,
    /// The condition each returned terminal's iterator applies.
    pub terminal: TerminalFilter<'a>,
}

/// Why a group could not be planned.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlanError {
    /// A clause names a level the index does not have.
    #[error("the group has no level {level}")]
    NoSuchLevel {
        /// The level named.
        level: usize,
    },
    /// A tag condition on a numeric level, or a range on a tag level.
    #[error("the condition on level {level} does not match the level's type")]
    PredicateMismatch {
        /// The level.
        level: usize,
    },
    /// A negated prefix, or a prefix and a negation on the same level, which no constraint
    /// expresses.
    #[error("a prefix cannot be negated, or combined with a negation on the same attribute")]
    NegatedPrefix,
    /// The disjunctive normal form has more branches than allowed.
    #[error("the group expands to more than {max} alternatives")]
    TooManyBranches {
        /// The limit.
        max: usize,
    },
}

/// Plans `expr` against `index`: the branches to walk, at most `max_branches` of them.
///
/// Branches that can match nothing, such as `@color:{red} @color:{blue}`, are dropped, so an
/// empty result means the group matches no document.
pub fn plan<'a>(
    index: &NestedIndex,
    expr: &Expr<'a>,
    max_branches: usize,
) -> Result<Vec<Branch<'a>>, PlanError> {
    let levels = index.routing().levels();
    let num_levels = levels.len() + 1;
    let conjunctions = dnf(expr, false, max_branches)?;
    let mut branches = Vec::with_capacity(conjunctions.len());
    for mut conjunction in conjunctions {
        let mut filters: Vec<LevelFilter<'a>> = (0..num_levels)
            .map(|level| {
                let numeric = if level < levels.len() {
                    matches!(levels[level], Level::Buckets(_))
                } else {
                    matches!(index.terminal_kind(), TerminalKind::Numeric(_))
                };
                if numeric {
                    LevelFilter::Range(Bound::Unbounded, Bound::Unbounded)
                } else {
                    LevelFilter::Tag(TagFilter::Any)
                }
            })
            .collect();
        // Value lists first, then prefixes, then negations: a negation under a prefix can only
        // be planned once a value list has turned the prefix into a set, so taking literals in
        // this order makes the outcome, plan or error, independent of how the query orders them.
        conjunction.sort_by_key(|literal| match literal.clause.predicate {
            _ if literal.negated => 2,
            Predicate::TagPrefix(_) => 1,
            Predicate::TagIn(_) | Predicate::Range { .. } => 0,
        });
        // Every literal is checked, even past one that empties the branch, so an unsupported
        // literal is an error wherever it stands.
        let mut possible = true;
        for literal in &conjunction {
            let level = literal.clause.level;
            let filter = filters
                .get_mut(level)
                .ok_or(PlanError::NoSuchLevel { level })?;
            possible &= filter.and(literal)?;
        }
        if !possible {
            continue;
        }
        let terminal = match filters.pop().expect("at least one level") {
            LevelFilter::Tag(tag) => TerminalFilter::Tag(tag.into_constraint()),
            LevelFilter::Range(start, end) => TerminalFilter::Numeric { start, end },
        };
        let routing = filters
            .into_iter()
            .map(|filter| match filter {
                LevelFilter::Tag(tag) => tag.into_constraint(),
                LevelFilter::Range(Bound::Unbounded, Bound::Unbounded) => Constraint::Any,
                LevelFilter::Range(start, end) => {
                    Constraint::BucketRange(BucketRange { start, end })
                }
            })
            .collect();
        branches.push(Branch { routing, terminal });
    }
    Ok(branches)
}

/// A clause, possibly negated: one term of a conjunction.
#[derive(Debug, Clone)]
struct Literal<'a> {
    clause: Clause<'a>,
    negated: bool,
}

/// The disjunctive normal form of `expr`, negated when `negate` is set: NOTs are pushed down to
/// the clauses (De Morgan), and ANDs are distributed over ORs.
fn dnf<'a>(expr: &Expr<'a>, negate: bool, max: usize) -> Result<Vec<Vec<Literal<'a>>>, PlanError> {
    match (expr, negate) {
        (
            Expr::Clause(Clause {
                level,
                predicate: Predicate::Range { start, end },
            }),
            true,
        ) => {
            // An element outside a range is below it or above it: one branch each.
            let below = match start {
                Bound::Included(lo) => Some(Bound::Excluded(*lo)),
                Bound::Excluded(lo) => Some(Bound::Included(*lo)),
                Bound::Unbounded => None,
            };
            let above = match end {
                Bound::Included(hi) => Some(Bound::Excluded(*hi)),
                Bound::Excluded(hi) => Some(Bound::Included(*hi)),
                Bound::Unbounded => None,
            };
            let pieces = [
                below.map(|end| (Bound::Unbounded, end)),
                above.map(|start| (start, Bound::Unbounded)),
            ];
            Ok(pieces
                .into_iter()
                .flatten()
                .map(|(start, end)| {
                    vec![Literal {
                        clause: Clause {
                            level: *level,
                            predicate: Predicate::Range { start, end },
                        },
                        negated: false,
                    }]
                })
                .collect())
        }
        (Expr::Clause(clause), negated) => Ok(vec![vec![Literal {
            clause: clause.clone(),
            negated,
        }]]),
        (Expr::Not(inner), negate) => dnf(inner, !negate, max),
        (Expr::Or(children), false) | (Expr::And(children), true) => {
            let mut out = Vec::new();
            for child in children {
                out.extend(dnf(child, negate, max)?);
                if out.len() > max {
                    return Err(PlanError::TooManyBranches { max });
                }
            }
            Ok(out)
        }
        (Expr::And(children), false) | (Expr::Or(children), true) => {
            // The empty conjunction: no condition, so every element.
            let mut out = vec![Vec::new()];
            for child in children {
                let child = dnf(child, negate, max)?;
                if out.len().saturating_mul(child.len()) > max {
                    return Err(PlanError::TooManyBranches { max });
                }
                // A one-branch child, the usual case of a plain clause, extends each conjunction
                // in place. Copying them instead would make a long AND quadratic; only a child
                // with several branches copies, and the limit allows few of those.
                if let [single] = child.as_slice() {
                    for conjunction in &mut out {
                        conjunction.extend(single.iter().cloned());
                    }
                    continue;
                }
                out = out
                    .iter()
                    .flat_map(|left| {
                        child.iter().map(move |right| {
                            let mut both = left.clone();
                            both.extend(right.iter().cloned());
                            both
                        })
                    })
                    .collect();
            }
            Ok(out)
        }
    }
}

/// The conditions a conjunction puts on one level so far.
enum LevelFilter<'a> {
    Tag(TagFilter<'a>),
    Range(Bound<f64>, Bound<f64>),
}

/// The tag values a conjunction allows on one level. Value sets are kept sorted and deduplicated,
/// so combining two takes time linear in their sizes, never their product.
enum TagFilter<'a> {
    Any,
    Among(Vec<&'a [u8]>),
    Excluding(Vec<&'a [u8]>),
    Prefix(&'a [u8]),
}

/// `values` as a sorted set.
fn sorted_set<'a>(values: &[&'a [u8]]) -> Vec<&'a [u8]> {
    let mut set = values.to_vec();
    set.sort_unstable();
    set.dedup();
    set
}

fn contains(set: &[&[u8]], value: &[u8]) -> bool {
    set.binary_search(&value).is_ok()
}

impl<'a> TagFilter<'a> {
    fn into_constraint(self) -> Constraint<'a> {
        match self {
            Self::Any => Constraint::Any,
            Self::Among(values) => Constraint::TagIn(values),
            Self::Excluding(values) => Constraint::TagNotIn(values),
            Self::Prefix(prefix) => Constraint::TagPrefix(prefix),
        }
    }
}

impl<'a> LevelFilter<'a> {
    /// Adds `literal` to the level's conditions. Returns `false` when they can no longer match.
    fn and(&mut self, literal: &Literal<'a>) -> Result<bool, PlanError> {
        let level = literal.clause.level;
        match (self, &literal.clause.predicate) {
            (Self::Tag(filter), Predicate::TagIn(values)) => filter.and_in(values, literal.negated),
            (Self::Tag(filter), Predicate::TagPrefix(prefix)) => {
                if literal.negated {
                    return Err(PlanError::NegatedPrefix);
                }
                filter.and_prefix(prefix)
            }
            (Self::Range(start, end), Predicate::Range { start: lo, end: hi }) => {
                debug_assert!(!literal.negated, "`dnf` complements negated ranges");
                *start = max_start(*start, *lo);
                *end = min_end(*end, *hi);
                Ok(!is_empty(*start, *end))
            }
            _ => Err(PlanError::PredicateMismatch { level }),
        }
    }
}

impl<'a> TagFilter<'a> {
    fn and_in(&mut self, values: &[&'a [u8]], negated: bool) -> Result<bool, PlanError> {
        let values = sorted_set(values);
        let next = match (std::mem::replace(self, Self::Any), negated) {
            (Self::Any, false) => Self::Among(values),
            (Self::Any, true) => Self::Excluding(values),
            (Self::Among(mut held), false) => {
                held.retain(|v| contains(&values, v));
                Self::Among(held)
            }
            (Self::Among(mut held), true) => {
                held.retain(|v| !contains(&values, v));
                Self::Among(held)
            }
            (Self::Excluding(held), false) => {
                Self::Among(values.into_iter().filter(|v| !contains(&held, v)).collect())
            }
            (Self::Excluding(held), true) => Self::Excluding(sorted_set(&[held, values].concat())),
            (Self::Prefix(prefix), false) => Self::Among(
                values
                    .into_iter()
                    .filter(|v| v.starts_with(prefix))
                    .collect(),
            ),
            // NOT in a set, under a prefix: no constraint expresses it.
            (Self::Prefix(_), true) => return Err(PlanError::NegatedPrefix),
        };
        let possible = !matches!(&next, Self::Among(values) if values.is_empty());
        *self = next;
        Ok(possible)
    }

    fn and_prefix(&mut self, prefix: &'a [u8]) -> Result<bool, PlanError> {
        let next = match std::mem::replace(self, Self::Any) {
            Self::Any => Self::Prefix(prefix),
            Self::Among(mut held) => {
                held.retain(|v| v.starts_with(prefix));
                Self::Among(held)
            }
            Self::Excluding(_) => return Err(PlanError::NegatedPrefix),
            Self::Prefix(held) if held.starts_with(prefix) => Self::Prefix(held),
            Self::Prefix(held) if prefix.starts_with(held) => Self::Prefix(prefix),
            Self::Prefix(_) => Self::Among(Vec::new()),
        };
        let possible = !matches!(&next, Self::Among(values) if values.is_empty());
        *self = next;
        Ok(possible)
    }
}

fn max_start(a: Bound<f64>, b: Bound<f64>) -> Bound<f64> {
    match (a, b) {
        (Bound::Unbounded, x) | (x, Bound::Unbounded) => x,
        (Bound::Included(x), Bound::Included(y)) => Bound::Included(x.max(y)),
        (Bound::Excluded(x), Bound::Excluded(y)) => Bound::Excluded(x.max(y)),
        (Bound::Included(i), Bound::Excluded(e)) | (Bound::Excluded(e), Bound::Included(i)) => {
            if i > e {
                Bound::Included(i)
            } else {
                Bound::Excluded(e)
            }
        }
    }
}

fn min_end(a: Bound<f64>, b: Bound<f64>) -> Bound<f64> {
    match (a, b) {
        (Bound::Unbounded, x) | (x, Bound::Unbounded) => x,
        (Bound::Included(x), Bound::Included(y)) => Bound::Included(x.min(y)),
        (Bound::Excluded(x), Bound::Excluded(y)) => Bound::Excluded(x.min(y)),
        (Bound::Included(i), Bound::Excluded(e)) | (Bound::Excluded(e), Bound::Included(i)) => {
            if i < e {
                Bound::Included(i)
            } else {
                Bound::Excluded(e)
            }
        }
    }
}

/// Whether no number lies between `start` and `end`.
fn is_empty(start: Bound<f64>, end: Bound<f64>) -> bool {
    match (start, end) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => false,
        (Bound::Included(lo), Bound::Included(hi)) => lo > hi,
        (Bound::Included(lo) | Bound::Excluded(lo), Bound::Included(hi) | Bound::Excluded(hi)) => {
            lo >= hi
        }
    }
}
