pub type DocId = u64;

use ffi::RSSortingVector;
use ffi::RSValue;

/// Row data for a lookup key. This abstracts the question of "where" the data comes from.
///
/// This stores the values received by an iteration over a RLookup.
///
/// cbindgen:field-names=[sv, dyn, ndyn]
#[repr(C)]
pub struct RLookupRow {
    /// contains sortable values for the row, is depending on the filed sorting
    pub sorting_vector: *const RSSortingVector,

    /// contains the dynamic values of the row
    pub dyn_values: *mut *mut RSValue,

    /// the number of dynamic values in the row
    pub num_values: libc::size_t,
}
