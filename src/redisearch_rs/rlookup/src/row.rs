/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#[cfg(debug_assertions)]
use crate::rlookup_id::RLookupId;
use crate::{RLookup, RLookupKey, RLookupKeyFlag, RLookupKeyFlags, SchemaRuleWrapper};
use sorting_vector::RSSortingVector;
use std::{borrow::Cow, ffi::CStr};
use value::RSValueTrait;

/// Row data for a lookup key. This abstracts the question of if the data comes from a borrowed [RSSortingVector]
/// or from dynamic values stored in the row during processing.
///
/// The type itself exposes the dynamic values, [`RLookupRow::dyn_values`], as a vector of `Option<T>`, where `T` is the type
/// of the value and it also provides methods to get the length of the dynamic values and check if they are empty.
///
/// The type `T` is the type of the value stored in the row, which must implement the [`RSValueTrait`].
/// [`RSValueTrait`] is a temporary trait that will be replaced by a type implementing `RSValue` in Rust, see MOD-10347.
///
/// The C-side allocations of values in [`RLookupRow::dyn_values`] and [`RLookupRow::sorting_vector`] are released on drop.
#[derive(Debug)]
pub struct RLookupRow<'a, T: RSValueTrait> {
    /// Sorting vector attached to document
    sorting_vector: Option<&'a RSSortingVector<T>>,

    /// Dynamic values obtained from prior processing
    dyn_values: Vec<Option<T>>,

    /// The number of values in [`RLookupRow::dyn_values`] that are `is_some()`. Note that this
    /// is not the length of [`RLookupRow::dyn_values`]
    num_dyn_values: u32,

    #[cfg(debug_assertions)]
    rlookup_id: RLookupId,
}

impl<'a, T: RSValueTrait> RLookupRow<'a, T> {
    /// Creates a new `RLookupRow` with an empty [`RLookupRow::dyn_values`] vector and
    /// a [`RLookupRow::sorting_vector`] of the given length.
    #[cfg_attr(not(debug_assertions), allow(unused_variables))]
    pub const fn new(rlookup: &RLookup<'_>) -> Self {
        Self {
            sorting_vector: None,
            dyn_values: vec![],
            num_dyn_values: 0,
            #[cfg(debug_assertions)]
            rlookup_id: rlookup.id(),
        }
    }

    /// Returns the length of [`RLookupRow::dyn_values`].
    pub const fn len(&self) -> usize {
        self.dyn_values.len()
    }

    /// Returns the number of visible fields in this [`RLookupRow`].
    ///
    /// # Arguments
    ///
    /// * `lookup` - The RLookup instance containing the keys and their flags.
    /// * `required_flags` - Flags that must be present on a key for it to be counted.
    /// * `excluded_flags` - Flags that must not be present on a key for it to be counted.
    /// * `rule` - An optional [`SchemaRuleWrapper`] to exclude key names used for special purposes, e.g. score, lang or payload.
    ///   If set to `None`, no such exclusions are applied (this is the case on the coordinator).
    ///
    /// The returned `Vec<bool>` indicates which fields were counted (true) or skipped (false).
    pub fn get_length(
        &self,
        lookup: &RLookup,
        required_flags: RLookupKeyFlags,
        excluded_flags: RLookupKeyFlags,
        rule: Option<SchemaRuleWrapper>,
    ) -> (usize, Vec<bool>) {
        let mut skip_field_indices = vec![false; self.len()];
        let num_fields = self.get_length_no_alloc(
            lookup,
            required_flags,
            excluded_flags,
            rule,
            skip_field_indices.as_mut_slice(),
        );
        (num_fields, skip_field_indices)
    }

    /// Returns the number of visible fields in this [`RLookupRow`].
    ///
    /// It acts as [`RLookupRow::get_length`] but instead of allocating a Vec internally, it takes a mutable slice, thus
    /// avoiding allocations and allowing the caller to reuse memory.
    ///
    /// See [`RLookupRow::get_length`] for argument details.
    pub fn get_length_no_alloc(
        &self,
        lookup: &RLookup,
        required_flags: RLookupKeyFlags,
        excluded_flags: RLookupKeyFlags,
        rule: Option<SchemaRuleWrapper>,
        out_flags: &mut [bool],
    ) -> usize {
        let mut num_fields = 0;
        for (idx, key) in lookup.cursor().enumerate() {
            // check flags and if key exists in row:
            if !key.flags.contains(required_flags)
                || key.flags.intersects(excluded_flags)
                || self.get(key).is_none()
            {
                continue;
            }

            // ensure a value is part of the RLookupRow:
            // is there a rule, if not we're on coordinator...
            if let Some(rule) = &rule
                && (rule.lang_field().is_some_and(|lf| lf == key.name_as_cstr())
                    || rule
                        .score_field()
                        .is_some_and(|sf| sf == key.name_as_cstr())
                    || rule
                        .payload_field()
                        .is_some_and(|p| p == key.name_as_cstr()))
            {
                // we trust the shards to not send those fields.
                // on coordinator, we reach this code
                continue;
            };

            out_flags[idx] = true;
            num_fields += 1;
        }

        num_fields
    }

    /// Returns true if the [`RLookupRow::dyn_values`] vector is empty.
    pub const fn is_empty(&self) -> bool {
        self.dyn_values.is_empty()
    }

    /// Readonly access to the [`RLookupRow::dyn_values`] vector that has been generated by prior processing.
    ///
    /// The function [`RLookupRow::write_key`] can be used to write values to this vector.
    pub fn dyn_values(&self) -> &[Option<T>] {
        &self.dyn_values
    }

    /// Sets the capacity of the [`RLookupRow::dyn_values`] vector to the given capacity.
    /// It fills up the vector with None values to the given capacity.
    /// This is useful to preallocate memory for the row if you know the number of values that will be written to it.
    pub fn set_dyn_capacity(&mut self, capacity: usize) {
        self.dyn_values.resize(capacity, None);
    }

    /// Readonly access to [`RLookupRow::sorting_vector`], it may be `None` if no sorting vector was set.
    pub const fn sorting_vector(&self) -> Option<&RSSortingVector<T>> {
        self.sorting_vector
    }

    /// Borrow a sorting vector for the row.
    pub const fn set_sorting_vector(&mut self, sv: &'a RSSortingVector<T>) {
        self.sorting_vector = Some(sv);
    }

    /// The number of values in [`RLookupRow::dyn_values`] that are `is_some()`. Note that this
    /// is not the length of [`RLookupRow::dyn_values`]
    pub const fn num_dyn_values(&self) -> u32 {
        self.num_dyn_values
    }

    /// Retrieves an item from the given `RLookupRow` based on the provided `RLookupKey`.
    /// The function first checks for dynamic values, and if not found, it checks the sorting vector
    /// if the `SvSrc` flag is set in the key.
    /// If the item is not found in either location, it returns `None`.
    pub fn get(&self, key: &RLookupKey) -> Option<&T> {
        // Check dynamic values first
        if self.len() > key.dstidx as usize
            && let Some(val) = self
                .dyn_values()
                .get(key.dstidx as usize)
                .expect("value is not in dynamic values even though dstidx is in bounds")
                .as_ref()
        {
            return Some(val);
        }

        // If not found in dynamic values, check the sorting vector if the SvSrc flag is set
        if key.flags.contains(RLookupKeyFlag::SvSrc) {
            self.sorting_vector()?.get(key.svidx as usize)
        } else {
            None
        }
    }

    /// Write a value to the lookup table in [`RLookupRow::dyn_values`]. Key must already be registered, and not
    /// refer to a read-only (SVSRC) key.
    pub fn write_key(&mut self, key: &RLookupKey, val: T) -> Option<T> {
        #[cfg(debug_assertions)]
        assert_eq!(key.rlookup_id(), self.rlookup_id);

        let idx = key.dstidx;
        if self.dyn_values.len() <= idx as usize {
            self.set_dyn_capacity((idx + 1) as usize);
        }

        let prev = self.dyn_values[idx as usize].replace(val);

        if prev.is_none() {
            self.num_dyn_values += 1;
        }

        prev
    }

    /// Write a value to the lookup table *by-name*. This is useful for 'dynamic' keys
    /// for which it is not necessary to use the boilerplate of getting an explicit
    /// key.
    pub fn write_key_by_name(
        &mut self,
        rlookup: &mut RLookup<'a>,
        name: impl Into<Cow<'a, CStr>>,
        val: T,
    ) {
        let name = name.into();
        let key = if let Some(cursor) = rlookup.find_key_by_name(&name) {
            cursor.into_current().expect("the cursor returned by `Keys::find_by_name` must have a current key. This is a bug!")
        } else {
            rlookup
                .get_key_write(name, RLookupKeyFlags::empty())
                .expect("`RLookup::get_key_write` must never return None for non-existent keys. This is a bug!")
        };
        self.write_key(key, val);
    }

    /// Wipes the row, retaining its memory but decrementing the ref count of any included instance of `T`.
    /// This does not free all the memory consumed by the row, but simply resets
    /// the row data (preserving any caches) so that it may be refilled.
    pub fn wipe(&mut self) {
        for value in self.dyn_values.iter_mut().filter(|v| v.is_some()) {
            *value = None;
            self.num_dyn_values -= 1;
        }
    }

    /// Resets the row, clearing the dynamic values. This effectively wipes the row and deallocates the memory used for dynamic values.
    ///
    /// It does not affect the sorting vector.
    pub fn reset_dyn_values(&mut self) {
        self.num_dyn_values = 0;
        self.dyn_values = vec![];
    }

    /// Write fields from a source row into this row, the fields must exist in both lookups (schemas).
    ///
    /// Iterate through the source lookup keys, if it finds a corresponding key in the destination
    /// lookup by name, then it's value is written to this row as a destination.
    ///
    /// If a source key is not found in the destination lookup the function will panic (same as C behavior).
    ///
    /// If a source key has no value in the source row, it is skipped.
    ///
    /// # Arguments
    ///
    /// - `dst_lookup`: The destination lookup containing the schema of this row, must be the associated lookup of `self`.
    /// - `src_row`: The source row from which to copy values.
    /// - `src_lookup`: The source lookup containing the schema of the source row, must be the associated lookup of `src_row`.
    pub fn copy_fields_from(&mut self, dst_lookup: &RLookup, src_row: &Self, src_lookup: &RLookup) {
        let dst_row = self;

        // NB: the `Iterator` impl for `Cursor` will automatically skip overridden keys
        for src_key in src_lookup.cursor() {
            // Get value from source row
            if let Some(value) = src_row.get(src_key) {
                // Find corresponding key in destination lookup
                let dst_key = dst_lookup
                    .find_key_by_name(src_key.name())
                    .expect("we expect all source keys to exist in destination")
                    .into_current()
                    .unwrap();

                // Write fields to destination
                dst_row.write_key(dst_key, value.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use enumflags2::make_bitflags;
    use value::RSValueMock;

    use super::*;

    fn create_schema_rule_wrapper(
        lang_field: Option<&std::ffi::CStr>,
        score_field: Option<&std::ffi::CStr>,
        payload_field: Option<&std::ffi::CStr>,
    ) -> SchemaRuleWrapper {
        use std::ptr::NonNull;

        let lang_ptr = lang_field.map_or(std::ptr::null_mut(), |cstr| cstr.as_ptr().cast_mut());
        let score_ptr = score_field.map_or(std::ptr::null_mut(), |cstr| cstr.as_ptr().cast_mut());
        let payload_ptr =
            payload_field.map_or(std::ptr::null_mut(), |cstr| cstr.as_ptr().cast_mut());

        let schema_rule = ffi::SchemaRule {
            lang_field: lang_ptr,
            score_field: score_ptr,
            payload_field: payload_ptr,
            type_: 0,
            prefixes: std::ptr::null_mut(),
            filter_exp_str: std::ptr::null_mut(),
            filter_exp: std::ptr::null_mut(),
            filter_fields: std::ptr::null_mut(),
            filter_fields_index: std::ptr::null_mut(),
            score_default: 0.0,
            lang_default: 0,
            index_all: false,
        };

        let boxed_rule = Box::new(schema_rule);
        let non_null_ptr = NonNull::new(Box::into_raw(boxed_rule)).unwrap();

        SchemaRuleWrapper::from_raw(non_null_ptr.as_ptr()).unwrap()
    }

    #[test]
    fn get_length_without_flags() {
        let mut rlookup = RLookup::new();
        let mut row = RLookupRow::<RSValueMock>::new(&rlookup);
        row.write_key_by_name(&mut rlookup, c"a", RSValueMock::create_num(42.));
        row.write_key_by_name(&mut rlookup, c"b", RSValueMock::create_num(12.));
        row.write_key_by_name(&mut rlookup, c"c", RSValueMock::create_num(36.));

        let (len, flags) = row.get_length(
            &rlookup,
            RLookupKeyFlags::empty(),
            RLookupKeyFlags::empty(),
            Some(create_schema_rule_wrapper(None, None, None)),
        );
        assert_eq!(len, 3);
        assert_eq!(flags, vec![true, true, true]);
    }

    #[test]
    fn get_length_on_empty() {
        let rlookup = RLookup::new();
        let row = RLookupRow::<RSValueMock>::new(&rlookup);

        let (len, flags) = row.get_length(
            &rlookup,
            RLookupKeyFlags::empty(),
            RLookupKeyFlags::empty(),
            Some(create_schema_rule_wrapper(None, None, None)),
        );
        assert_eq!(len, 0);
        assert_eq!(flags, vec![]);
    }

    #[test]
    fn get_length_required_flags() {
        let mut rlookup = RLookup::new();
        let mut row = RLookupRow::<RSValueMock>::new(&rlookup);
        let rlk = rlookup
            .get_key_write(c"a", make_bitflags!(RLookupKeyFlag::ExplicitReturn))
            .expect("key must be created");
        row.write_key(rlk, RSValueMock::create_num(42.));
        row.write_key_by_name(&mut rlookup, c"b", RSValueMock::create_num(12.));
        row.write_key_by_name(&mut rlookup, c"c", RSValueMock::create_num(36.));

        let (len, flags) = row.get_length(
            &rlookup,
            make_bitflags!(RLookupKeyFlag::ExplicitReturn),
            RLookupKeyFlags::empty(),
            Some(create_schema_rule_wrapper(None, None, None)),
        );
        assert_eq!(len, 1);
        assert_eq!(flags, vec![true, false, false]);
    }

    #[test]
    fn get_length_excluded_flags() {
        let mut rlookup = RLookup::new();
        let mut row = RLookupRow::<RSValueMock>::new(&rlookup);
        let rlk = rlookup
            .get_key_load(c"a", c"a", make_bitflags!(RLookupKeyFlag::ExplicitReturn))
            .expect("key must be created");
        row.write_key(rlk, RSValueMock::create_num(42.));
        row.write_key_by_name(&mut rlookup, c"b", RSValueMock::create_num(12.));
        row.write_key_by_name(&mut rlookup, c"c", RSValueMock::create_num(36.));

        let (len, flags) = row.get_length(
            &rlookup,
            RLookupKeyFlags::empty(),
            make_bitflags!(RLookupKeyFlag::ExplicitReturn),
            Some(create_schema_rule_wrapper(None, None, None)),
        );
        assert_eq!(len, 2);
        assert_eq!(flags, vec![false, true, true]);
    }

    // historically this mix caused no items to be counted
    #[test]
    fn get_length_required_and_excluded_flags_same() {
        let mut rlookup = RLookup::new();
        let mut row = RLookupRow::<RSValueMock>::new(&rlookup);
        let rlk = rlookup
            .get_key_load(c"a", c"a", make_bitflags!(RLookupKeyFlag::ExplicitReturn))
            .expect("key must be created");
        row.write_key(rlk, RSValueMock::create_num(42.));
        row.write_key_by_name(&mut rlookup, c"b", RSValueMock::create_num(12.));
        row.write_key_by_name(&mut rlookup, c"c", RSValueMock::create_num(36.));

        let (len, flags) = row.get_length(
            &rlookup,
            make_bitflags!(RLookupKeyFlag::ExplicitReturn),
            make_bitflags!(RLookupKeyFlag::ExplicitReturn),
            Some(create_schema_rule_wrapper(None, None, None)),
        );
        assert_eq!(len, 0);
        assert_eq!(flags, vec![false, false, false]);
    }

    // Without a rule we expect no filtering for special purpose keys like score, lang or payload
    #[test]
    fn get_length_without_rule() {
        let mut rlookup = RLookup::new();
        let mut row = RLookupRow::<RSValueMock>::new(&rlookup);
        row.write_key_by_name(&mut rlookup, c"a", RSValueMock::create_num(42.));
        row.write_key_by_name(&mut rlookup, c"b", RSValueMock::create_num(12.));
        row.write_key_by_name(&mut rlookup, c"c", RSValueMock::create_num(36.));

        let (len, flags) = row.get_length(
            &rlookup,
            RLookupKeyFlags::empty(),
            RLookupKeyFlags::empty(),
            None,
        );

        assert_eq!(len, 3);
        assert_eq!(flags, vec![true, true, true]);
    }

    // The rule is used to filter special purpose keys like score, lang or payload
    #[test]
    fn get_length_with_rule() {
        let mut rlookup = RLookup::new();
        let mut row = RLookupRow::<RSValueMock>::new(&rlookup);
        row.write_key_by_name(&mut rlookup, c"a", RSValueMock::create_num(42.));
        row.write_key_by_name(&mut rlookup, c"b", RSValueMock::create_num(12.));
        row.write_key_by_name(&mut rlookup, c"score", RSValueMock::create_num(100.));

        let (len, flags) = row.get_length(
            &rlookup,
            RLookupKeyFlags::empty(),
            RLookupKeyFlags::empty(),
            Some(create_schema_rule_wrapper(None, Some(c"score"), None)),
        );

        assert_eq!(len, 2);
        assert_eq!(flags, vec![true, true, false]);

        // add some more keys with special keys:
        row.write_key_by_name(
            &mut rlookup,
            c"lang",
            RSValueMock::create_string("en".to_string()),
        );
        row.write_key_by_name(&mut rlookup, c"c", RSValueMock::create_num(42.));
        row.write_key_by_name(&mut rlookup, c"payload", RSValueMock::create_num(815.0));

        let (len, flags) = row.get_length(
            &rlookup,
            RLookupKeyFlags::empty(),
            RLookupKeyFlags::empty(),
            Some(create_schema_rule_wrapper(
                Some(c"lang"),
                Some(c"score"),
                Some(c"payload"),
            )),
        );

        assert_eq!(len, 3);
        assert_eq!(flags, vec![true, true, false, false, true, false]);
    }
}
