/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

mod bindings;
mod lookup;
mod row;

// Link both Rust-provided and C-provided symbols
#[cfg(all(test, feature = "unittest"))]
extern crate redisearch_rs;
// Mock or stub the ones that aren't provided by the line above
#[cfg(all(test, feature = "unittest"))]
redis_mock::mock_or_stub_missing_redis_c_symbols!();

pub use bindings::{IndexSpec, IndexSpecCache, SchemaRule};
pub use lookup::{
    Cursor, CursorMut, RLookup, RLookupKey, RLookupKeyFlag, RLookupKeyFlags, RLookupOption,
    RLookupOptions, opaque::OpaqueRLookup,
};
pub use row::RLookupRow;
pub use row::opaque::OpaqueRLookupRow;
