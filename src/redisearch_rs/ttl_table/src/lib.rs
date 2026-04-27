/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A per-document time-to-live table.
//!
//! This is a parallel pure-Rust implementation of the C module under
//! `src/ttl_table/`, kept available alongside the C version so the two can be
//! benchmarked against each other before any decision is made to retire the C
//! implementation.
//!
//! Each entry in the table associates a [`u64`] document id with:
//!
//! - A document-level expiration time (a [`libc::timespec`]).
//! - A list of per-field expiration times, sorted by field index.
//!
//! The two key queries are:
//!
//! - [`TimeToLiveTable::has_doc_expired`] — tells whether the document as a
//!   whole has expired at a given point in time.
//! - [`TimeToLiveTable::verify_doc_and_field`] /
//!   [`TimeToLiveTable::verify_doc_and_field_mask`] — checks expiration of an
//!   individual field or a set of fields described by a bit mask, under one of
//!   the [`FieldExpirationPredicate`] variants.

use std::collections::HashMap;
use std::collections::hash_map::Entry as MapEntry;

pub use field::FieldExpirationPredicate;
use libc::timespec;

/// The expiration time recorded for a single field of a document.
///
/// Callers of [`TimeToLiveTable::add`] are responsible for sorting these by
/// `index` in ascending order. The verification routines rely on the sort
/// order to scan the list in linear time.
#[derive(Debug, Clone, Copy)]
pub struct FieldExpiration {
    /// The field index this expiration applies to.
    pub index: u16,
    /// The point in time at which the field expires.
    pub point: timespec,
}

/// Bit-mask abstraction used by [`TimeToLiveTable::verify_doc_and_field_mask`].
///
/// The C module exposes two flavours of the mask check, one over a 32-bit
/// mask and one over the wider 128/64-bit `t_fieldMask`. The trait lets the
/// Rust port collapse both into a single generic implementation.
pub trait BitMask: Copy {
    /// Returns the number of set bits.
    fn count_ones(self) -> u32;

    /// Clears and returns the index of the lowest set bit, or `None` if the
    /// mask is zero.
    fn pop_lowest_set_bit(&mut self) -> Option<u32>;
}

macro_rules! impl_bit_mask {
    ($t:ty) => {
        impl BitMask for $t {
            #[inline]
            fn count_ones(self) -> u32 {
                <$t>::count_ones(self)
            }

            #[inline]
            fn pop_lowest_set_bit(&mut self) -> Option<u32> {
                if *self == 0 {
                    return None;
                }
                let bit = self.trailing_zeros();
                *self &= !((1 as $t) << bit);
                Some(bit)
            }
        }
    };
}

impl_bit_mask!(u32);
impl_bit_mask!(u64);
impl_bit_mask!(u128);

#[derive(Debug)]
struct Entry {
    doc_expiration: timespec,
    fields: Vec<FieldExpiration>,
}

/// A table mapping document ids to their document-level and per-field
/// expiration metadata.
#[derive(Debug, Default)]
pub struct TimeToLiveTable {
    map: HashMap<u64, Entry>,
}

impl TimeToLiveTable {
    /// Creates an empty table.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts the expiration metadata for a document.
    ///
    /// `fields` must be sorted in ascending order by `index`. The verify
    /// routines rely on this invariant.
    ///
    /// # Panics
    ///
    /// In debug builds, panics if `doc_id` is already present in the table —
    /// the C version asserts the same, since callers are expected to call
    /// [`Self::remove`] first if they want to replace an entry.
    pub fn add(&mut self, doc_id: u64, doc_expiration: timespec, fields: Vec<FieldExpiration>) {
        debug_assert!(fields.is_sorted_by_key(|f| f.index));

        match self.map.entry(doc_id) {
            MapEntry::Vacant(v) => {
                v.insert(Entry {
                    doc_expiration,
                    fields,
                });
            }
            MapEntry::Occupied(_) => {
                debug_assert!(false, "Failed to add document to ttl table");
            }
        }
    }

    /// Removes the metadata for a document. No-op if the id is absent.
    pub fn remove(&mut self, doc_id: u64) {
        self.map.remove(&doc_id);
    }

    /// Returns `true` if the table holds no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Returns `true` if the document has a document-level expiration that
    /// has elapsed by `now`. Returns `false` if the document is not tracked.
    #[must_use]
    pub fn has_doc_expired(&self, doc_id: u64, now: &timespec) -> bool {
        let Some(entry) = self.map.get(&doc_id) else {
            return false;
        };
        did_expire(&entry.doc_expiration, now)
    }

    /// Checks the expiration state of a single field of a document under
    /// `predicate`.
    ///
    /// Mirrors the semantics of `TimeToLiveTable_VerifyDocAndField` in the C
    /// module:
    /// - If the document has no entry in the table, returns `true`
    ///   (no expiration is recorded for it, so the field is trivially valid
    ///   for `Default` and trivially absent for `Missing`).
    /// - If the document has an entry but no per-field expirations, returns
    ///   `true` for the same reason.
    /// - Otherwise, scans the per-field list for a matching index and returns
    ///   the result that respects `predicate`.
    #[must_use]
    pub fn verify_doc_and_field(
        &self,
        doc_id: u64,
        field: u16,
        predicate: FieldExpirationPredicate,
        now: &timespec,
    ) -> bool {
        let Some(entry) = self.map.get(&doc_id) else {
            // the document did not have a ttl for itself or its fields
            // if predicate is default then we know at least one field is valid
            // if predicate is missing then we know the field is indeed missing since the document has no expiration for it
            return true;
        };
        if entry.fields.is_empty() {
            // the document has no fields with expiration times, there exists at least one valid field
            return true;
        }

        for fe in &entry.fields {
            if fe.index == field {
                // the field has an expiration time
                let expired = did_expire(&fe.point, now);
                return if expired {
                    // the document is invalid (should return `false`), unless we look for missing fields
                    predicate == FieldExpirationPredicate::Missing
                } else {
                    // the document is valid (should return `true`), unless we look for missing fields
                    predicate != FieldExpirationPredicate::Missing
                };
            }
        }

        // the field was not found in the document's field expirations,
        // which means it is valid unless the predicate is FIELD_EXPIRATION_MISSING
        predicate != FieldExpirationPredicate::Missing
    }

    /// Checks the expiration state of a set of fields described by `mask`.
    ///
    /// `ft_id_to_field_index` translates a bit position in `mask` into the
    /// `t_fieldIndex` recorded in the table. It must contain at least as many
    /// entries as the highest set bit of `mask`.
    ///
    /// Mirrors the combined behaviour of `TimeToLiveTable_VerifyDocAndFieldMask`
    /// and `TimeToLiveTable_VerifyDocAndWideFieldMask` in the C module: the
    /// generic parameter `M` covers both the 32-bit and the wider mask paths
    /// and resolves the `TODO: Rust - unify` left in the C source.
    #[must_use]
    pub fn verify_doc_and_field_mask<M: BitMask>(
        &self,
        doc_id: u64,
        mut mask: M,
        predicate: FieldExpirationPredicate,
        now: &timespec,
        ft_id_to_field_index: &[u16],
    ) -> bool {
        let Some(entry) = self.map.get(&doc_id) else {
            // the document did not have a ttl for itself or its fields
            // if predicate is default then we know at least one field is valid
            // if predicate is missing then we know the field is indeed missing since the document has no expiration for it
            return true;
        };

        let field_with_expiration_count = entry.fields.len();
        let field_count = mask.count_ones() as usize;
        if field_with_expiration_count == 0 {
            // the document has no fields with expiration times, there exists at least one valid field
            return true;
        }
        if field_with_expiration_count < field_count
            && predicate == FieldExpirationPredicate::Default
        {
            // the document has less fields with expiration times than the fields we are checking
            // at least one field is valid
            return true;
        }

        let mut predicate_misses: usize = 0;
        let mut current_field_index: usize = 0;
        while let Some(bit_index) = mask.pop_lowest_set_bit() {
            let field_index_to_check = ft_id_to_field_index[bit_index as usize];
            
            // Attempt to find the next field expiration that matches the current field index
            while current_field_index < field_with_expiration_count
                && field_index_to_check > entry.fields[current_field_index].index
            {
                current_field_index += 1;
            }
            if current_field_index >= field_with_expiration_count {
                // No more fields with expiration times to check
                break;
            }
            if field_index_to_check < entry.fields[current_field_index].index {
                // The field we are checking is not present in the current field expiration
                continue;
            }
            debug_assert_eq!(
                field_index_to_check,
                entry.fields[current_field_index].index
            );
            
            // Match found - we need to check if it has an expiration time
            let expired = did_expire(&entry.fields[current_field_index].point, now);
            if !expired && predicate == FieldExpirationPredicate::Default {
                return true;
            }
            if expired && predicate == FieldExpirationPredicate::Missing {
                return true;
            }
            predicate_misses += 1;
        }

        match predicate {
            FieldExpirationPredicate::Default => {
                // If we are checking for the default predicate, we need at least one valid field
                predicate_misses < field_count
            },
            FieldExpirationPredicate::Missing => {
                // If we are checking for the missing predicate, we need at least one expired field
                // If we reached here, it means we did not find any expired fields
                false
            }
        }
    }
}

/// Returns `true` if `field` has elapsed by `now`.
///
/// A `field` whose `tv_sec` and `tv_nsec` are both zero is treated as
/// "no expiration set" and never expires, matching the C `DidExpire` helper.
#[inline]
const fn did_expire(field: &timespec, now: &timespec) -> bool {
    if field.tv_sec == 0 && field.tv_nsec == 0 {
        return false;
    }
    !((field.tv_sec > now.tv_sec) || (field.tv_sec == now.tv_sec && field.tv_nsec > now.tv_nsec))
}
