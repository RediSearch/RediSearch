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
#[cfg(test)]
mod mock;
mod row;

pub use bindings::IndexSpecCache;
pub use lookup::{
    RLookup, RLookupKey, RLookupKeyFlag, RLookupKeyFlags, RLookupOption, RLookupOptions,
};
pub use row::RLookupRow;
use value::RSValueTrait;

/// Retrieves an item from the given `RLookupRow` based on the provided `RLookupKey`.
/// The function first checks for dynamic values, and if not found, it checks the sorting vector
/// if the `SvSrc` flag is set in the key.
/// If the item is not found in either location, it returns `None`.
///
/// # Lifetime
/// The returned reference is tied to the lifetime of the input `RLookupRow`.
pub fn rlookup_get_item<'a>(
    key: &RLookupKey,
    row: &'a RLookupRow<impl RSValueTrait>,
) -> Option<&'a impl RSValueTrait> {
    // 1. Check dynamic values first
    if row.len() > key.dstidx as usize {
        return row.dyn_values()[key.dstidx as usize].as_ref();
    }

    // 2. If not found in dynamic values, check the sorting vector if the SvSrc flag is set
    if key.flags.contains(RLookupKeyFlag::SvSrc) {
        let sv = row.sorting_vector()?;
        sv.get(key.svidx as usize)
    } else {
        None
    }
}
