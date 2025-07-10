/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::RLookupKey;

#[derive(Debug, Clone)]
pub struct RSValueOpqaue {
    // Placeholder for the actual type definition
    // This should be defined based on the actual use case
}

pub struct RSSortingVectorOpaque {
    // Placeholder for the actual type definition
    // This should be defined based on the actual use case
}

impl RSValueOpqaue {
    pub fn increment(&mut self) {}

    pub fn decrement(&mut self) {}
}

/// Row data for a lookup key. This abstracts the question of if the data comes from a SortinVector
/// or from a dynamic value.
///
/// The cleanup of the RLookupRow can be triggered by Drop.
pub struct RLookupRow {
    /// Sorting vector attached to document
    #[expect(unused, reason = "used by later stacked PRs")]
    sorting_vector: RSSortingVectorOpaque,

    /// Dynamic values obtained from prior processing
    values: Vec<Option<RSValueOpqaue>>,

    /// How many values actually exist in dyn. Note that this is not the length of the array!
    num: u32,
}

impl Drop for RLookupRow {
    fn drop(&mut self) {
        // Wipe the row, decrementing any RSValues
        self.wipe();
    }
}

impl RLookupRow {
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

    /// Write a value to a lookup table. Key must already be registered, and not
    /// refer to a read-only (SVSRC) key.
    ///
    /// The value written won't have its refcount incremented
    pub fn write_own_key(&mut self, key: &RLookupKey, val: RSValueOpqaue) {
        let idx = key.dstidx;
        if self.values.len() <= idx as usize {
            self.values.resize((idx + 1) as usize, None);
        }

        let in_place = &mut self.values[idx as usize];
        if let Some(existing_value) = in_place {
            existing_value.decrement();
            self.num -= 1;
        }

        *in_place = Some(val);
        self.num += 1;
    }

    /// Write a value to a lookup table. Key must already be registered, and not
    /// refer to a read-only (SVSRC) key.
    ///
    /// The value written will have its refcount incremented
    pub fn write_key(&mut self, key: &RLookupKey, mut val: RSValueOpqaue) {
        val.increment();
        self.write_own_key(key, val);
    }
}
