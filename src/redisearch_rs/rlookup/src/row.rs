/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#[derive(Debug, Clone)]
pub struct RSValueOpqaue {
    // Placeholder for the actual type definition
    // This should be defined based on the actual use case
}

pub struct RSSortingVectorOpaque {
    // Placeholder for the actual type definition
    // This should be defined based on the actual use case
}

/// Row data for a lookup key. This abstracts the question of if the data comes from a SortinVector
/// or from a dynamic value.
#[expect(unused, reason = "used by later stacked PRs")]
pub struct RLookupRow {
    /// Sorting vector attached to document
    sorting_vector: RSSortingVectorOpaque,

    /// Dynamic values obtained from prior processing
    values: Vec<Option<RSValueOpqaue>>,

    /// How many values actually exist in dyn. Note that this is not the length of the array!
    num: u32,
}

impl RLookupRow {}
