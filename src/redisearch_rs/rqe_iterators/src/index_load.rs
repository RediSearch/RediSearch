/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Index loading: a decorator that hands the value a hit was matched on — the
//! number of a NUMERIC clause, the tag of a TAG clause — to the result
//! pipeline, as a yielded value, so the pipeline reads it as a field instead of
//! loading it from the document.
//!
//! This is the iterator half of the `$as` query attribute, an unstable
//! feature. The value reaches the row the same way a vector range query's
//! distance does: the decorator pushes it into the hit's
//! [`yielded_values`](RSIndexResult::yielded_values_mut) under the [`RLookupKey`] the
//! pipeline resolved for the alias, and the yielded values loader writes it into the
//! row.

use std::{cell::Cell, fmt, mem::ManuallyDrop, ptr::NonNull, rc::Rc};

use ffi::QueryIterator;
use index_result::{RSIndexResult, SharedString, SharedStringVTable, YieldedData};
use index_spec::IndexSpecReadGuard;
use ref_mode::{Active, Ref, Suspended};
use rlookup::{KeyHandleSlot, RLookupKey, RLookupKeyHandle};
use rqe_core::DocId;
use value::{SharedValue, Value};

use crate::{
    IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    RQEValidateStatus, ResumeOutcome, SkipToOutcome,
    boxed::{resume_single_child, suspend_single_child},
    c2rust::CRQEIterator,
    interop::{ProfileChildren, RQEIteratorWrapper, rust_profile_children},
    profile_print::{ProfilePrint, ProfilePrintCtx},
    score_bound::{BlockBound, MinCompetitiveScore},
};

/// The cell pipeline construction writes the alias's [`RLookupKey`] into,
/// through an [`RLookupKeyHandle`] that points at it, shared by every
/// decorator of one clause.
///
/// A TAG clause decorates each of its tag readers, and they all load under
/// the clause's one alias. The cell is an allocation of its own because the
/// decorators move after the handle is bound: profiling re-boxes every
/// compound iterator, and it runs before pipeline construction writes the key.
///
/// The last clone to drop invalidates the handle, so pipeline construction
/// never writes into a freed cell.
#[derive(Clone)]
pub struct KeyCell<'query>(Rc<KeyCellInner<'query>>);

struct KeyCellInner<'query> {
    key: Cell<*mut RLookupKey<'query>>,
    handle: KeyHandleSlot<'query>,
}

impl<'query> KeyCell<'query> {
    /// A cell with no key and no handle.
    pub fn new() -> Self {
        Self(Rc::new(KeyCellInner {
            key: Cell::new(std::ptr::null_mut()),
            handle: KeyHandleSlot::new(),
        }))
    }

    /// The slot pipeline construction writes the bound key into, valid until
    /// the last clone of this cell is dropped.
    pub fn slot(&self) -> NonNull<*mut RLookupKey<'query>> {
        NonNull::new(self.0.key.as_ptr()).expect("`Rc` contents are non-null")
    }

    /// Set the [`RLookupKeyHandle`] that tracks [`slot`](Self::slot).
    ///
    /// # Safety
    ///
    /// 1. The conditions of [`KeyHandleSlot::set`], with this cell as the
    ///    slot's owner.
    ///
    /// # Panics
    ///
    /// If the cell is already shared: the handle is set once, right after
    /// binding, before any decorator holds the cell.
    pub unsafe fn set_handle(&mut self, handle: NonNull<RLookupKeyHandle<'query>>) {
        let inner = Rc::get_mut(&mut self.0).expect("the handle is set before the cell is shared");
        // SAFETY: (1).
        unsafe { inner.handle.set(Some(handle)) };
    }

    /// The key pipeline construction bound, or null while it has bound none.
    fn key(&self) -> *mut RLookupKey<'query> {
        self.0.key.get()
    }
}

impl Default for KeyCell<'_> {
    fn default() -> Self {
        Self::new()
    }
}

/// Where a decorator finds the value it loads for a hit.
///
/// `'static`: a source borrows nothing from the index, so a decorator holds it
/// unchanged across suspend and resume.
pub trait ValueSource: 'static {
    /// The value to load for `hit`, or `None` when the hit holds none.
    fn value(&self, hit: &RSIndexResult<'_>) -> Option<YieldedData>;
}

/// A NUMERIC clause's value: the hit's own number when the hit is a numeric
/// leaf, and otherwise the first numeric record among its children, depth
/// first. A numeric range over several range-tree leaves is a union, which
/// keeps the value in the child that produced the document.
///
/// The value is the one the index stores: a range tree built with
/// `search-_numeric-compress` may store a float slightly off the document's.
#[derive(Clone, Copy, Debug, Default)]
pub struct HitNumber;

impl ValueSource for HitNumber {
    fn value(&self, hit: &RSIndexResult<'_>) -> Option<YieldedData> {
        hit_number(hit).map(YieldedData::Number)
    }
}

fn hit_number(result: &RSIndexResult<'_>) -> Option<f64> {
    if let Some(value) = result.as_numeric() {
        return Some(value);
    }
    let aggregate = result.as_aggregate()?;
    (0..aggregate.len()).find_map(|i| aggregate.get(i).and_then(hit_number))
}

/// A TAG clause's value: the one tag its decorated reader reads, built once
/// with the decorator. Every hit carries a clone, an atomic reference-count
/// increment rather than a copy of the string.
///
/// The reader knows its tag before reading any hit, on disk as in memory, so
/// the string is built once per reader rather than from each hit.
impl ValueSource for SharedString {
    fn value(&self, _hit: &RSIndexResult<'_>) -> Option<YieldedData> {
        Some(YieldedData::String(self.clone()))
    }
}

/// The child a decorator refused, because it may not report the child's type.
pub struct Refused<I>(I);

impl<'index, I: RQEIterator<'index>> Refused<I> {
    /// The refused child's type.
    pub fn type_(&self) -> IteratorType {
        self.0.type_()
    }

    /// The refused child, undecorated.
    pub fn into_child(self) -> I {
        self.0
    }
}

impl<'index, I: RQEIterator<'index>> fmt::Debug for Refused<I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Refused").field(&self.type_()).finish()
    }
}

/// A decorator that attaches the value its [`ValueSource`] finds for each hit
/// of its child to the hit, as a yielded value under the alias's [`RLookupKey`].
///
/// Parameterised over a [`Ref`] mode — see [`IndexLoad`] for the [`Active`]
/// instantiation that implements [`RQEIterator`].
///
/// It reports its child's [`type_`](RQEIterator::type_), so it is invisible to
/// everything that dispatches on the type: loading is an option of the clause,
/// not a kind of iterator.
///
/// When several values of one document match, one of them is loaded; which
/// one is unspecified. Parents move a child's yielded values into their own
/// result, so the value is pushed afresh on every positioning, replacing whatever the
/// hit carried.
///
/// Until pipeline construction binds a key, the decorator pushes nothing: the
/// yielded values loader writes every value through its key, and a null one cannot
/// be written.
///
/// # Invariants
///
/// 1. **Layout compatibility across modes.** `RawIndexLoad` is `#[repr(C)]`
///    and carries no `Rf`-dependent field other than the zero-sized
///    `_marker`, so its `Active` and `Suspended` instantiations are
///    layout-identical given that the child `I` and its `I::Suspended` are,
///    which [`suspend_child_slot_in_place`](crate::boxed::suspend_child_slot_in_place)
///    enforces. The `const` block below proves the wrapper's own part.
/// 2. **The child is transparently wrappable.** The child's type satisfies
///    [`IteratorType::is_transparently_wrappable`], so the decorator may
///    report it as its own. [`IndexLoad::with_source`] checks it.
#[repr(C)]
pub struct RawIndexLoad<'query, Rf: Ref, I, V = HitNumber> {
    child: I,
    key: KeyCell<'query>,
    source: V,
    _marker: std::marker::PhantomData<Rf>,
}

impl<'query, Rf: Ref, I, V> RawIndexLoad<'query, Rf, I, V> {
    /// The child field of the decorator at `raw`, formed without a reference.
    ///
    /// # Safety
    ///
    /// 1. `raw` points to a live allocation of `Self`.
    unsafe fn child_ptr(raw: *mut Self) -> *mut I {
        // SAFETY: 1.
        unsafe { &raw mut (*raw).child }
    }

    /// Drop every field of the decorator at `raw` but the child, in place.
    ///
    /// # Safety
    ///
    /// 1. `raw` points to a live, exclusively owned allocation of `Self` whose
    ///    fields other than the child are valid. The child slot is not read.
    /// 2. Those fields are not used again.
    unsafe fn drop_rest(raw: *mut Self) {
        // SAFETY: 1.
        let key = unsafe { &raw mut (*raw).key };
        // SAFETY: 1 and 2: the field is valid, owned and never used again.
        unsafe { std::ptr::drop_in_place(key) };
        // SAFETY: 1.
        let source = unsafe { &raw mut (*raw).source };
        // SAFETY: 1 and 2: the field is valid, owned and never used again.
        unsafe { std::ptr::drop_in_place(source) };
    }
}

/// Alias for an [`Active`] [`RawIndexLoad`] — the only instantiation with an
/// [`RQEIterator`] impl.
pub type IndexLoad<'index, I, V = HitNumber> = RawIndexLoad<'index, Active<'index>, I, V>;

// Compile-time proof of invariant 1 on `RawIndexLoad`, for a representative
// child whose `Active`/`Suspended` forms differ but stay layout-compatible.
const _: () = {
    use crate::Wildcard;
    use std::mem::{align_of, offset_of, size_of};
    type AChild = Wildcard<'static>;
    type SChild = <Wildcard<'static> as RQEIteratorBoxed<'static>>::Suspended;
    type A = RawIndexLoad<'static, Active<'static>, AChild, SharedString>;
    type S = RawIndexLoad<'static, Suspended, SChild, SharedString>;
    assert!(offset_of!(A, child) == offset_of!(S, child));
    assert!(offset_of!(A, key) == offset_of!(S, key));
    assert!(offset_of!(A, source) == offset_of!(S, source));
    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

/// Replace `result`'s yielded values with `(key, the value source finds)`, or leave
/// them untouched when `key` is null or the source finds no value.
fn attach<'index>(
    result: &mut RSIndexResult<'index>,
    key: *mut RLookupKey<'index>,
    source: &impl ValueSource,
) {
    // `YieldedValues` types the key the way C sees it: the header
    // `RLookupKey` is exported under, which is this key's first field.
    //
    // SAFETY: a non-null `key` is the key pipeline construction bound, owned by
    // the pipeline's lookup, which outlives every result the iterator tree
    // yields.
    let Some(key) = (unsafe { key.cast::<ffi::RLookupKey>().as_ref() }) else {
        return;
    };
    let Some(value) = source.value(result) else {
        return;
    };
    let values = result.yielded_values_mut();
    values.reset();
    values.push_with_key(key, value);
}

impl<'index, I: RQEIterator<'index>> IndexLoad<'index, I> {
    /// Decorate `child` to load the numbers of its hits, in a cell of its own
    /// with no key bound yet.
    ///
    /// # Errors
    ///
    /// As [`with_source`](Self::with_source).
    pub fn new(child: I) -> Result<Self, Refused<I>> {
        Self::with_source(child, KeyCell::new(), HitNumber)
    }
}

impl<'index, I: RQEIterator<'index>, V: ValueSource> IndexLoad<'index, I, V> {
    /// Decorate `child` to load the values `source` finds, under the key
    /// bound in `key`.
    ///
    /// # Errors
    ///
    /// Hands `child` back, undecorated, when the decorator may not report its
    /// type (invariant 2).
    pub fn with_source(child: I, key: KeyCell<'index>, source: V) -> Result<Self, Refused<I>> {
        if !child.type_().is_transparently_wrappable() {
            return Err(Refused(child));
        }
        Ok(Self {
            child,
            key,
            source,
            _marker: std::marker::PhantomData,
        })
    }

    /// The decorated iterator.
    pub const fn child(&self) -> &I {
        &self.child
    }

    /// The slot pipeline construction writes the bound key into, as
    /// [`KeyCell::slot`].
    pub fn key_slot(&self) -> NonNull<*mut RLookupKey<'index>> {
        self.key.slot()
    }
}

impl<'index, I: RQEIterator<'index>, V: ValueSource> RQEIterator<'index>
    for IndexLoad<'index, I, V>
{
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.child.current()
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        let key = self.key.key();
        let mut result = self.child.read()?;
        if let Some(result) = result.as_deref_mut() {
            attach(result, key, &self.source);
        }
        Ok(result)
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        let key = self.key.key();
        let source = &self.source;
        // A `NotFound` hit is attached too: parents keep it as the next
        // candidate and consume it later without reading the child again.
        Ok(self.child.skip_to(doc_id)?.map(|outcome| match outcome {
            SkipToOutcome::Found(result) => {
                attach(result, key, source);
                SkipToOutcome::Found(result)
            }
            SkipToOutcome::NotFound(result) => {
                attach(result, key, source);
                SkipToOutcome::NotFound(result)
            }
        }))
    }

    fn rewind(&mut self) {
        self.child.rewind();
    }

    fn num_estimated(&self) -> usize {
        self.child.num_estimated()
    }

    fn last_doc_id(&self) -> DocId {
        self.child.last_doc_id()
    }

    fn at_eof(&self) -> bool {
        self.child.at_eof()
    }

    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        let key = self.key.key();
        Ok(match self.child.revalidate(spec)? {
            RQEValidateStatus::Moved {
                current: Some(result),
            } => {
                attach(result, key, &self.source);
                RQEValidateStatus::Moved {
                    current: Some(result),
                }
            }
            status => status,
        })
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        self.child.type_()
    }

    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64 {
        self.child
            .intersection_sort_weight(prioritize_union_children)
    }

    fn reports_score_bounds(&self) -> bool {
        self.child.reports_score_bounds()
    }

    fn current_max_score(&self) -> f64 {
        self.child.current_max_score()
    }

    fn current_block_bound(&self) -> Option<BlockBound> {
        self.child.current_block_bound()
    }

    fn read_with_threshold(
        &mut self,
        min_competitive_score: MinCompetitiveScore,
    ) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        let key = self.key.key();
        let mut result = self.child.read_with_threshold(min_competitive_score)?;
        if let Some(result) = result.as_deref_mut() {
            attach(result, key, &self.source);
        }
        Ok(result)
    }
}

/// Transparent: the profile shows the decorated iterator as it would without
/// index loading.
///
/// Implemented for the [`CRQEIterator`] child alone, the instantiation
/// [`boxed_new`] lowers.
impl<'index, V: ValueSource> ProfilePrint for IndexLoad<'index, CRQEIterator, V> {
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        self.child.print_profile(map, ctx);
    }
}

impl<'index, V: ValueSource> ProfileChildren<'index> for IndexLoad<'index, CRQEIterator, V> {
    fn profile_children(self) -> Self {
        let Self {
            child,
            key,
            source,
            _marker,
        } = self;
        Self {
            child: child.into_profiled(),
            key,
            source,
            _marker,
        }
    }
}

impl<'index, I, V> RQEIteratorBoxed<'index> for IndexLoad<'index, I, V>
where
    I: RQEIteratorBoxed<'index>,
    V: ValueSource,
{
    type Suspended = RawIndexLoad<'index, Suspended, I::Suspended, V>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        // SAFETY: (1) `child_ptr` projects to the child slot without forming a
        // reference. (2) The `Active` and `Suspended` forms are layout-identical
        // apart from that slot (invariant 1).
        unsafe { suspend_single_child(self, Self::child_ptr) }
    }
}

impl<'query, S, V> RQESuspendedIterator<'query> for RawIndexLoad<'query, Suspended, S, V>
where
    S: RQESuspendedIterator<'query>,
    V: ValueSource,
{
    type Resumed<'a>
        = IndexLoad<'a, S::Resumed<'a>, V>
    where
        'query: 'a;

    fn resume<'a>(
        self: Box<Self>,
        guard: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        // SAFETY: (1) `child_ptr` projects to the child slot without forming a
        // reference. (2) The forms are layout-identical apart from that slot
        // (invariant 1). (3) `drop_rest` drops every other field in place,
        // without reading the child slot.
        let outcome = unsafe {
            resume_single_child::<_, _, Self::Resumed<'a>>(
                self,
                guard,
                Self::child_ptr,
                Self::drop_rest,
            )
        }?;
        // A move repositions the child onto a hit whose value was never
        // attached, or was taken by a parent.
        Ok(match outcome {
            ResumeOutcome::Moved(mut active) => {
                let IndexLoad {
                    child, key, source, ..
                } = &mut *active;
                if let Some(result) = child.current() {
                    attach(result, key.key(), source);
                }
                ResumeOutcome::Moved(active)
            }
            outcome => outcome,
        })
    }

    fn last_doc_id(&self) -> DocId {
        S::last_doc_id(&self.child)
    }

    fn num_estimated(&self) -> usize {
        S::num_estimated(&self.child)
    }
}

/// Lower `decorator` to the C ABI.
pub fn boxed_new<V: ValueSource>(
    decorator: IndexLoad<'_, CRQEIterator, V>,
) -> NonNull<QueryIterator> {
    // Not `boxed_new_compound`: the decorator reports its child's type, which
    // may be a leaf's, yet it has a child to profile.
    RQEIteratorWrapper::boxed_new_inner(
        decorator,
        Some(rust_profile_children::<IndexLoad<'_, CRQEIterator, V>>),
    )
}

/// [`SharedString`] operations over a string [`SharedValue`], whose raw
/// pointer is the `RSValue *` C sees.
static SHARED_VALUE_STRING: SharedStringVTable = SharedStringVTable {
    retain: |value| {
        // SAFETY: a `SharedString` built by `shared_string` holds a pointer
        // `SharedValue::into_raw` returned, a reference to which is live; the
        // borrowed handle is not dropped, and the clone's reference is kept.
        let borrowed = ManuallyDrop::new(unsafe { SharedValue::from_raw(value.as_ptr().cast()) });
        std::mem::forget(SharedValue::clone(&borrowed));
    },
    release: |value| {
        // SAFETY: as above, and the caller hands this reference over.
        drop(unsafe { SharedValue::from_raw(value.as_ptr().cast()) });
    },
    bytes: |value| {
        // SAFETY: as above; the caller keeps the reference for as long as it
        // reads the bytes.
        let value = unsafe { value.cast::<Value>().as_ref() };
        NonNull::from(
            value
                .as_str_bytes()
                .expect("a `SharedString` holds a string value"),
        )
    },
};

/// A [`SharedString`] holding `bytes`, in a newly allocated [`SharedValue`].
pub fn shared_string(bytes: Vec<u8>) -> SharedString {
    let raw = SharedValue::new_string(bytes).into_raw();
    let raw = NonNull::new(raw.cast_mut().cast()).expect("`SharedValue` pointers are non-null");
    // SAFETY: `raw` is a string value whose one reference is handed over, and
    // `SHARED_VALUE_STRING` holds `SharedValue`'s operations, which are
    // thread-safe.
    unsafe { SharedString::from_raw(raw, &SHARED_VALUE_STRING) }
}

/// How many references the [`SharedValue`] behind `string` has, if
/// [`shared_string`] built it.
pub fn shared_value_refcount(string: &SharedString) -> Option<usize> {
    if !std::ptr::eq(string.vtable(), &SHARED_VALUE_STRING) {
        return None;
    }
    // SAFETY: `shared_string` built it, so the pointer is one
    // `SharedValue::into_raw` returned and `string` holds a reference to it;
    // the borrowed handle is not dropped.
    let value =
        ManuallyDrop::new(unsafe { SharedValue::from_raw(string.as_ptr().as_ptr().cast()) });
    Some(SharedValue::refcount(&value))
}
