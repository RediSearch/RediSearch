/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Evaluation of `QN_NULL` query nodes.

use rqe_iterators::Empty;

use crate::Evaluated;

/// `QN_NULL` — stopword queries produce an empty iterator.
pub(crate) fn eval<'index>() -> Evaluated<'index> {
    Evaluated::RustLeaf(Box::new(Empty))
}
