/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The routing layer of a compound index over the elements of a NESTED JSON array.
//!
//! A compound index orders the attributes of an array element, such as `(color, size, price)`.
//! Every attribute but the last is a routing [`Level`]: an exact tag value ([`TagLevel`]) or a
//! fixed numeric bucket ([`BucketLevel`]). The last attribute lives in a *terminal*, the value
//! of type `T` at the end of each path, which this crate stores but does not interpret. A caller
//! uses a numeric range tree or a doc-id posting list as `T`.
//!
//! Each element follows exactly one path from the root to a terminal, so a doc id posted in a
//! terminal means "this document has an element with these values on every level above". That is
//! what keeps several conditions on one element paired, whichever levels a query fixes: a
//! [`CompoundIndex::collect_terminals`] walk that skips a level only visits more terminals, and
//! never returns a terminal whose path breaks a constraint.
//!
//! # Route, then post
//!
//! The index never sees a doc id. Indexing an element takes two steps, and the index owns only
//! the first:
//!
//! 1. **Route**: [`CompoundIndex::get_or_insert_with`] follows the element's path, creates
//!    the missing nodes and the terminal if the path is new, and returns `&mut T`.
//! 2. **Post**: the caller adds the doc id, with the element's terminal value, to that `T`.
//!
//! Elements of one document that share a path get the same terminal, so the terminal decides
//! whether a doc id posted twice is stored twice. To index a document whole or not at all, run
//! [`CompoundIndex::check_paths`] on all its paths first: once it passes, no route fails.
//!
//! The design is described in `docs/design/nested-compound-index.md`.

mod constraint;
mod error;
mod index;
mod key;
mod level;

pub use constraint::{BucketRange, Constraint};
pub use error::{BucketsError, CollectError, InsertError, PathError, SchemaError};
pub use index::{CompoundIndex, MAX_LEVELS, WalkLimits};
pub use key::Key;
pub use level::{BucketLevel, Level, MAX_BUCKETS, TagLevel};
