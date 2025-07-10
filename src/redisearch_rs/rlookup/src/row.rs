/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use sorting_vector::RSSortingVector;
use value::RSValueTrait;

/// Row data for a lookup key. This abstracts the question of if the data comes from a SortinVector
/// or from a dynamic value.
pub struct RLookupRow<T: RSValueTrait> {
    ///
    /// The cleanup of the RLookupRow can be triggered by Drop.
    /// Sorting vector attached to document
    #[expect(unused, reason = "used by later stacked PRs")]
    sorting_vector: RSSortingVector<T>,

    /// Dynamic values obtained from prior processing
    values: Vec<Option<T>>,

    /// How many values actually exist in dyn. Note that this is not the length of the array!
    num: u32,
}

impl<T: RSValueTrait> Drop for RLookupRow<T> {
    fn drop(&mut self) {
        // Wipe the row, decrementing any RSValues
        self.wipe();
    }
}

impl<T: RSValueTrait> RLookupRow<T> {
    /// Wipes the row, retaining its memory but decrementing any included values.
    /// This does not free all the memory consumed by the row, but simply resets
    /// the row data (preserving any caches) so that it may be refilled.    
    pub fn wipe(&mut self) {
        for value in &mut self.values.iter_mut().filter(|v| v.is_some()) {
            if let Some(v) = value {
                v.decrement();
                *value = None;
                self.num -= 1;
            }
        }
    }
}
