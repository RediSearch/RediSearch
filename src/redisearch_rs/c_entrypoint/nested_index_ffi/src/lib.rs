/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C bindings for the index of a NESTED field.
//!
//! The schema crosses the boundary once, as an array of [`NestedLevelDef`] given to
//! [`NestedIndex_New`]. Indexing is split in two, as for sparse vectors, so that reading the JSON
//! document runs before the spec write lock is taken: the loader fills a [`NestedRows`] element by
//! element, and [`NestedIndex_IndexDocument`] checks and writes it under the lock.

use std::{ffi::c_char, num::NonZeroUsize};

use ffi::t_docId;
use nested_index::{
    BucketLevel, CompoundIndex, Constraint, Element, FloatCompression, IndexParams, Key, Level,
    NestedIndex, TagLevel, Terminal, TerminalKind, TerminalValue, WalkLimits,
};
use tag_index::Tag;

/// The kind of one routing level, as C declares it.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cheadergen::config(export, prefix_with_name)]
pub enum NestedLevelKind {
    /// Children keyed by an exact tag value.
    Tag = 0,
    /// Children keyed by fixed numeric buckets.
    Buckets = 1,
}

/// One routing level of a NESTED field's `INDEX`, as C describes it.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
#[cheadergen::config(export)]
pub struct NestedLevelDef {
    /// Which kind of level this is.
    pub kind: NestedLevelKind,
    /// For [`NestedLevelKind::Tag`]: the `MAXCARD` limit per node, `0` for none.
    pub max_cardinality: usize,
    /// For [`NestedLevelKind::Buckets`]: the boundaries, `num_boundaries` of them.
    pub boundaries: *const f64,
    /// For [`NestedLevelKind::Buckets`]: the number of boundaries.
    pub num_boundaries: usize,
}

/// The kind of the last attribute of `INDEX`, which every terminal stores.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cheadergen::config(export, prefix_with_name)]
pub enum NestedTerminalKind {
    /// A NUMERIC attribute, in a numeric range tree.
    Numeric = 0,
    /// A TAG attribute, in a tag index.
    Tag = 1,
}

/// What indexing one document changed, for the spec's statistics.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cheadergen::config(export)]
pub struct NestedIndexOutcome {
    /// Terminals created.
    pub new_terminals: usize,
    /// The change in the number of records, for `numRecords`.
    pub records: i64,
    /// The change in postings memory, in bytes, for `invertedSize`.
    pub size_delta: i64,
    /// The change in the number of posting-list blocks.
    pub block_delta: i64,
}

/// One element's key on one level, owned until the document is indexed.
enum OwnedKey {
    Tag(Vec<u8>),
    Numeric(f64),
}

/// One element's terminal value, owned until the document is indexed.
enum OwnedValue {
    Tag(Vec<u8>),
    Numeric(f64),
}

/// The elements of one document, read from JSON before the spec lock is taken.
///
/// Each element is its keys, pushed in level order, then its terminal value, which ends it.
pub struct NestedRows {
    /// Every element's keys, back to back.
    keys: Vec<OwnedKey>,
    /// One `(number of keys, value)` per finished element.
    elements: Vec<(usize, OwnedValue)>,
    /// Keys pushed since the last finished element.
    pending: usize,
}

/// Writes `message` into `err` as a NUL-terminated string, truncated to `err_len` bytes.
///
/// # Safety
///
/// `err` is [valid] for writes of `err_len` bytes, or `err_len` is zero.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
unsafe fn write_error(err: *mut c_char, err_len: usize, message: &str) {
    if err_len == 0 {
        return;
    }
    // SAFETY: guaranteed by the caller.
    let buf = unsafe { std::slice::from_raw_parts_mut(err.cast::<u8>(), err_len) };
    let n = message.len().min(err_len - 1);
    buf[..n].copy_from_slice(&message.as_bytes()[..n]);
    buf[n] = 0;
}

/// Build the index of a NESTED field from its routing levels and terminal kind.
///
/// `compress_floats` applies to numeric terminals, and is read from the module configuration
/// once, when the index is built. Returns null when the levels are rejected (too few or too many,
/// invalid bucket boundaries), after writing a NUL-terminated reason into `err`. Free a non-null
/// result with [`NestedIndex_Free`].
///
/// # Safety
///
/// 1. `levels` is [valid] for reads of `num_levels` elements, or `num_levels` is zero.
/// 2. Every [`NestedLevelKind::Buckets`] level's `boundaries` is [valid] for reads of
///    `num_boundaries` elements, or `num_boundaries` is zero.
/// 3. `err` is [valid] for writes of `err_len` bytes, or `err_len` is zero.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NestedIndex_New(
    levels: *const NestedLevelDef,
    num_levels: usize,
    terminal: NestedTerminalKind,
    compress_floats: bool,
    err: *mut c_char,
    err_len: usize,
) -> *mut NestedIndex {
    let defs = if num_levels == 0 {
        &[][..]
    } else {
        // SAFETY: guaranteed by 1.
        unsafe { std::slice::from_raw_parts(levels, num_levels) }
    };
    let mut built = Vec::with_capacity(defs.len());
    for (position, def) in defs.iter().enumerate() {
        let level = match def.kind {
            NestedLevelKind::Tag => Level::Tag(match NonZeroUsize::new(def.max_cardinality) {
                Some(max) => TagLevel::with_max_cardinality(max),
                None => TagLevel::new(),
            }),
            NestedLevelKind::Buckets => {
                let boundaries = if def.num_boundaries == 0 {
                    Vec::new()
                } else {
                    // SAFETY: guaranteed by 2.
                    unsafe { std::slice::from_raw_parts(def.boundaries, def.num_boundaries) }
                        .to_vec()
                };
                match BucketLevel::new(boundaries) {
                    Ok(buckets) => Level::Buckets(buckets),
                    Err(e) => {
                        // SAFETY: guaranteed by 3.
                        unsafe { write_error(err, err_len, &format!("level {position}: {e}")) };
                        return std::ptr::null_mut();
                    }
                }
            }
        };
        built.push(level);
    }
    let kind = match terminal {
        NestedTerminalKind::Numeric => TerminalKind::Numeric(if compress_floats {
            FloatCompression::On
        } else {
            FloatCompression::Off
        }),
        NestedTerminalKind::Tag => TerminalKind::Tag,
    };
    match NestedIndex::new(built, kind) {
        Ok(index) => Box::into_raw(Box::new(index)),
        Err(e) => {
            // SAFETY: guaranteed by 3.
            unsafe { write_error(err, err_len, &e.to_string()) };
            std::ptr::null_mut()
        }
    }
}

/// Free an index built by [`NestedIndex_New`]. A null `index` is a no-op.
///
/// # Safety
///
/// `index` is null, or was returned by [`NestedIndex_New`] and not freed yet.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NestedIndex_Free(index: *mut NestedIndex) {
    if !index.is_null() {
        // SAFETY: guaranteed by the caller.
        drop(unsafe { Box::from_raw(index) });
    }
}

/// An empty set of elements for a field with `num_levels` routing levels. Free it with
/// [`NestedRows_Free`].
#[unsafe(no_mangle)]
pub extern "C" fn NestedRows_New(num_levels: usize) -> *mut NestedRows {
    Box::into_raw(Box::new(NestedRows {
        keys: Vec::with_capacity(num_levels),
        elements: Vec::new(),
        pending: 0,
    }))
}

/// Free a set of elements. A null `rows` is a no-op.
///
/// # Safety
///
/// `rows` is null, or was returned by [`NestedRows_New`] and not freed yet.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NestedRows_Free(rows: *mut NestedRows) {
    if !rows.is_null() {
        // SAFETY: guaranteed by the caller.
        drop(unsafe { Box::from_raw(rows) });
    }
}

/// Append a tag key to the current element.
///
/// # Safety
///
/// 1. `rows` is a valid pointer returned by [`NestedRows_New`].
/// 2. `value` is [valid] for reads of `len` bytes, or `len` is zero.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NestedRows_PushTagKey(
    rows: *mut NestedRows,
    value: *const c_char,
    len: usize,
) {
    // SAFETY: guaranteed by 1.
    let rows = unsafe { &mut *rows };
    // SAFETY: guaranteed by 2.
    let bytes = unsafe { bytes(value, len) };
    rows.keys.push(OwnedKey::Tag(bytes.to_vec()));
    rows.pending += 1;
}

/// Append a numeric key to the current element.
///
/// # Safety
///
/// `rows` is a valid pointer returned by [`NestedRows_New`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NestedRows_PushNumericKey(rows: *mut NestedRows, value: f64) {
    // SAFETY: guaranteed by the caller.
    let rows = unsafe { &mut *rows };
    rows.keys.push(OwnedKey::Numeric(value));
    rows.pending += 1;
}

/// End the current element with a tag terminal value.
///
/// # Safety
///
/// As [`NestedRows_PushTagKey`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NestedRows_FinishTag(
    rows: *mut NestedRows,
    value: *const c_char,
    len: usize,
) {
    // SAFETY: guaranteed by 1 of [`NestedRows_PushTagKey`].
    let rows = unsafe { &mut *rows };
    // SAFETY: guaranteed by 2 of [`NestedRows_PushTagKey`].
    let bytes = unsafe { bytes(value, len) };
    rows.elements
        .push((rows.pending, OwnedValue::Tag(bytes.to_vec())));
    rows.pending = 0;
}

/// End the current element with a numeric terminal value.
///
/// # Safety
///
/// As [`NestedRows_PushNumericKey`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NestedRows_FinishNumeric(rows: *mut NestedRows, value: f64) {
    // SAFETY: guaranteed by the caller.
    let rows = unsafe { &mut *rows };
    rows.elements
        .push((rows.pending, OwnedValue::Numeric(value)));
    rows.pending = 0;
}

/// Drop the keys pushed since the last finished element, for an element the loader skips.
///
/// # Safety
///
/// As [`NestedRows_PushNumericKey`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NestedRows_DiscardElement(rows: *mut NestedRows) {
    // SAFETY: guaranteed by the caller.
    let rows = unsafe { &mut *rows };
    let keep = rows.keys.len() - rows.pending;
    rows.keys.truncate(keep);
    rows.pending = 0;
}

/// The number of finished elements.
///
/// # Safety
///
/// `rows` is a valid pointer returned by [`NestedRows_New`].
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn NestedRows_Len(rows: *const NestedRows) -> usize {
    // SAFETY: guaranteed by the caller.
    unsafe { &*rows }.elements.len()
}

/// Index every element of `rows` for the document `doc_id`.
///
/// On success, writes what changed into `outcome` and returns `true`. On failure the index is
/// unchanged; returns `false` after writing a NUL-terminated reason into `err`.
///
/// # Safety
///
/// 1. `index` is a valid pointer returned by [`NestedIndex_New`], held exclusively (under the
///    spec write lock).
/// 2. `rows` is a valid pointer returned by [`NestedRows_New`], with no unfinished element.
/// 3. `outcome` is [valid] for writes.
/// 4. `err` is [valid] for writes of `err_len` bytes, or `err_len` is zero.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NestedIndex_IndexDocument(
    index: *mut NestedIndex,
    doc_id: t_docId,
    rows: *const NestedRows,
    max_depth_range: usize,
    outcome: *mut NestedIndexOutcome,
    err: *mut c_char,
    err_len: usize,
) -> bool {
    // SAFETY: guaranteed by 1.
    let index = unsafe { &mut *index };
    // SAFETY: guaranteed by 2.
    let rows = unsafe { &*rows };
    debug_assert_eq!(rows.pending, 0, "every element is finished before indexing");

    let keys: Vec<Key<'_>> = rows
        .keys
        .iter()
        .map(|key| match key {
            OwnedKey::Tag(bytes) => Key::Tag(bytes),
            OwnedKey::Numeric(value) => Key::Numeric(*value),
        })
        .collect();
    let mut elements = Vec::with_capacity(rows.elements.len());
    let mut start = 0;
    for (position, (num_keys, value)) in rows.elements.iter().enumerate() {
        let value = match value {
            OwnedValue::Numeric(value) => TerminalValue::Numeric(*value),
            OwnedValue::Tag(bytes) => match Tag::new(bytes) {
                Some(tag) => TerminalValue::Tag(tag),
                None => {
                    // SAFETY: guaranteed by 4.
                    unsafe {
                        write_error(
                            err,
                            err_len,
                            &format!("element {position}: tag value contains a NUL byte"),
                        )
                    };
                    return false;
                }
            },
        };
        elements.push(Element {
            path: &keys[start..start + num_keys],
            value,
        });
        start += num_keys;
    }

    match index.index_document(doc_id, &elements, IndexParams { max_depth_range }) {
        Ok(delta) => {
            // SAFETY: guaranteed by 3.
            unsafe {
                outcome.write(NestedIndexOutcome {
                    new_terminals: delta.new_terminals,
                    records: delta.records,
                    size_delta: delta.size_delta,
                    block_delta: delta.block_delta,
                })
            };
            true
        }
        Err(e) => {
            // SAFETY: guaranteed by 4.
            unsafe { write_error(err, err_len, &e.to_string()) };
            false
        }
    }
}

/// The number of terminals, one per distinct path indexed.
///
/// # Safety
///
/// `index` is a valid pointer returned by [`NestedIndex_New`], not written concurrently.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn NestedIndex_NumTerminals(index: *const NestedIndex) -> usize {
    // SAFETY: guaranteed by the caller.
    unsafe { &*index }.routing().num_terminals()
}

/// The records written and not yet collected.
///
/// # Safety
///
/// As [`NestedIndex_NumTerminals`].
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn NestedIndex_NumRecords(index: *const NestedIndex) -> usize {
    // SAFETY: guaranteed by the caller.
    unsafe { &*index }.num_records()
}

/// The terminals' overhead in bytes, excluding postings, which the spec already counts in
/// `invertedSize`. Walks every terminal.
///
/// # Safety
///
/// As [`NestedIndex_NumTerminals`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NestedIndex_Overhead(index: *const NestedIndex) -> usize {
    // SAFETY: guaranteed by the caller.
    let routing: &CompoundIndex<Terminal> = unsafe { &*index }.routing();
    let any = vec![Constraint::Any; routing.levels().len()];
    routing
        .collect_terminals(&any, WalkLimits::UNLIMITED)
        .expect("Any on every level is a valid, unbounded walk")
        .iter()
        .map(|terminal| terminal.mem_usage())
        .sum()
}

/// The bytes at `ptr`, or an empty slice when `len` is zero.
///
/// # Safety
///
/// `ptr` is [valid] for reads of `len` bytes, or `len` is zero.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
const unsafe fn bytes<'a>(ptr: *const c_char, len: usize) -> &'a [u8] {
    if len == 0 {
        &[]
    } else {
        // SAFETY: guaranteed by the caller.
        unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), len) }
    }
}
