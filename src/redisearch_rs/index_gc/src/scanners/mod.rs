/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! One module per index kind the GC collects from. Each knows how to walk its
//! part of a spec and how to apply the resulting deltas to a live spec; how the
//! two halves are connected (a pipe to a forked child, or the same thread) is
//! the policy's business.
//!
//! Across these modules, `scan` finds an index's delta and `apply` lands one on
//! the live spec; `collect_*` and `handle_*` are the forking policy's halves of
//! the same, scanning and sending in the child and receiving and applying in the
//! parent.

pub mod existing_docs;
pub mod missing_docs;
pub mod numeric;
pub mod tags;
pub mod terms;
