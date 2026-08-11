/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! One module per query-AST node type, each exposing an `eval` entry point that
//! [`eval_node`](crate::eval_node) dispatches to.
//!
//! The machinery they share — the evaluator [`Config`](crate::Config), the
//! [`Evaluated`](crate::Evaluated) outcome type, and the helpers for evaluating
//! a child node — lives at the crate root rather than here.

pub(crate) mod geo;
pub(crate) mod geometry;
pub(crate) mod ids;
pub(crate) mod missing;
pub(crate) mod not;
pub(crate) mod null;
pub(crate) mod numeric;
pub(crate) mod optional;
pub(crate) mod phrase;
pub(crate) mod prefix;
pub(crate) mod token;
pub(crate) mod union;
pub(crate) mod wildcard;
pub(crate) mod wildcard_query;
