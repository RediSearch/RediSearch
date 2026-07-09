/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{cell::RefCell, rc::Rc, time::Duration};

use index_result::{RSIndexResult, RSOffsetSlice};
use rqe_core::{DocId, RS_FIELDMASK_ALL};
use rqe_iterators::{IteratorType, TypeErasedRQEIterator, WildcardIterator};

/// Test iterator used in unit tests that expect an [`RQEIterator`]
/// child which produces a fixed sequence of document identifiers.
///
/// `Mock` simulates a very small posting list:
///
/// * It owns a fixed array of document ids that must be sorted in
///   increasing order.
/// * Calls to [`RQEIterator::read`] walk that array from left to right
///   and copy the current id into a reusable [`RSIndexResult`] that is
///   stored inside the iterator.
/// * Calls to [`RQEIterator::skip_to`] advance `next_index` until it
///   reaches the requested id or the first id that is greater than it.
///
/// The iterator is intentionally simple and deterministic so that
/// higher level iterators can be tested without depending on the
/// actual inverted index implementation.  For example it is used as
/// the child iterator in the `Optional` iterator tests to verify
/// interaction between real and virtual results, end of input handling
/// and validation.
///
/// The iterator also owns a [`MockData`] value that is stored inside a
/// reference counted cell.  Test code can obtain a handle to this
/// state through [`Mock::data`] in order to:
///
/// * Inspect how many times `RQEIterator::revalidate` (removed) was called.
/// * Inspect how many times [`RQEIterator::read`] was called.
/// * Configure what `RQEIterator::revalidate` (removed) will return through
///   [`MockData::set_revalidate_result`].
/// * Configure an error that will be returned once the iterator
///   reaches the end of the document ids through
///   [`MockData::set_error_at_done`].
///
/// Taken from the C++tests in
/// `tests/cpptests/iterator_util.h`.
///
/// `#[repr(C)]` so that `Mock<'index, N>` and [`MockSuspended<N>`] are
/// layout-compatible: `suspend`/`resume` relabel the same heap allocation
/// between the two via [`Box::from_raw`], which is only sound if both have a
/// fixed, matching field layout (default repr may order distinct types
/// differently even when their fields match).
#[repr(C)]
pub struct Mock<'index, const N: usize> {
    result: RSIndexResult<'index>,
    doc_ids: [DocId; N],
    /// One term position per document, or `None` for a virtual (non-term) result.
    ///
    /// Each value must be in the range `1..=127`: values in that range are their own
    /// single-byte LEB128 varint encoding, so the byte can be passed directly to
    /// [`RSOffsetSlice::from_slice`] without a separate encoding step.
    positions: Option<[u8; N]>,
    next_index: usize,
    data: MockData,
}

/// Error that can be injected into a [`Mock`] from tests.
///
/// This type is intentionally small and is translated into the
/// public [`rqe_iterators::RQEIteratorError`] type through
/// [`MockIteratorError::as_rqe_iterator_error`].
///
/// This allows tests to
/// express expectations in terms of the real error type while still
/// using a simple local enum to control behaviour.
#[derive(Debug, Clone, Copy)]
pub(crate) enum MockIteratorError {
    /// Simulate a timeout in the child iterator.
    ///
    /// Optionally introduce an actual delay of the specified [`Duration`].
    /// Note that this blocks the current thread. This delay is happening
    /// right before the error is returned for the `skip_to` / `read` call.
    TimeoutError(Option<Duration>),
}

impl MockIteratorError {
    /// Convert this mock error into the public [`rqe_iterators::RQEIteratorError`] type.
    ///
    /// This helper keeps the mock specific error type private to the
    /// test utilities while still surfacing the correct error value to
    /// production code and test assertions.
    fn into_rqe_iterator_error(self) -> rqe_iterators::RQEIteratorError {
        match self {
            Self::TimeoutError(opt_delay) => {
                if let Some(delay) = opt_delay {
                    std::thread::sleep(delay);
                }
                rqe_iterators::RQEIteratorError::TimedOut
            }
        }
    }
}

/// Shared mutable test state that belongs to a [`Mock`].
///
/// The value is reference counted so that:
///
/// * The iterator can own it.
/// * Tests can keep their own handle obtained through
///   [`Mock::data`] and observe or mutate the state while the
///   iterator is in use.
///
/// Most tests treat `MockData` as a light weight handle that can be
/// cloned cheaply and passed around by value.
pub struct MockData(Rc<RefCell<MockDataInternal>>);

impl MockData {
    /// Create a new [`MockData`] instance with default behaviour.
    ///
    /// The initial state is:
    ///
    /// * `revalidate_result` set to [`MockRevalidateResult::Ok`].
    /// * `validation_count` equal to zero.
    /// * `read_count` equal to zero.
    /// * `error_at_done` set to `None` which means no error will be
    ///   raised at end of input.
    fn new() -> Self {
        Self(Rc::new(RefCell::new(MockDataInternal {
            revalidate_result: MockRevalidateResult::default(),
            validation_count: 0,
            read_count: 0,
            error_at_done: None,
            delays: Vec::new(),
            force_read_none: false,
        })))
    }

    /// Configure the result that [`Mock::revalidate`] will report.
    ///
    /// This value is read on every call to `revalidate` of the owning
    /// iterator.  Tests can update it at any time to change how the
    /// iterator reacts to validation requests.
    pub fn set_revalidate_result(&mut self, result: MockRevalidateResult) -> &mut Self {
        self.0.borrow_mut().revalidate_result = result;
        self
    }

    /// Configure the error that is returned once the iterator reaches
    /// end of input.
    ///
    /// If `maybe_err` is `Some`, the next call to [`RQEIterator::read`]
    /// or [`RQEIterator::skip_to`] that reaches end of the document id
    /// array will immediately return that error instead of `Ok(None)`.
    ///
    /// If `maybe_err` is `None`, end of input is reported as `Ok(None)`.
    pub fn set_error_at_done(&mut self, maybe_err: Option<MockIteratorError>) -> &mut Self {
        self.0.borrow_mut().error_at_done = maybe_err;
        self
    }

    /// Configure a delay that should be introduced since the given index,
    /// either in a [`RQEIterator::read`] or [`RQEIterator::skip_to`] call
    /// for the [`Mock`] iterator.
    pub fn add_delay_since_index(&mut self, index: DocId, delay: Duration) -> &mut Self {
        {
            let mut data = self.0.borrow_mut();
            data.delays.push((index, delay));
            data.delays.sort_by_cached_key(|(idx, _delay)| *idx);
        }

        self
    }

    /// Number of times [`Mock::revalidate`] was called.
    ///
    /// This counter is incremented whenever the owning iterator calls
    /// `revalidate` on its child.  Tests use this to assert that a
    /// particular code path triggers the expected number of validation
    /// attempts.
    pub fn revalidate_count(&self) -> usize {
        self.0.borrow().validation_count
    }

    /// Force the next call to `read()` to return `None` even if not at EOF.
    ///
    /// This simulates an iterator that doesn't know it's at EOF until `read()` is called.
    /// The Inverted Index iterator exhibits this behavior: `at_eof()` returns `false`
    /// before a `read()` call, but `read()` discovers there are no more records and
    /// returns `None` (then sets `at_eos = true`).
    ///
    /// The flag is automatically cleared after one use.
    pub fn set_force_read_none(&mut self, force: bool) -> &mut Self {
        self.0.borrow_mut().force_read_none = force;
        self
    }

    /// Number of times [`Mock::read`] was called.
    ///
    /// This counter is incremented whenever the owning iterator calls
    /// `read` or performs a `skip_to` that internally delegates to
    /// `read`.  It is useful when tests need to verify how often a
    /// child iterator was advanced.
    pub fn read_count(&self) -> usize {
        self.0.borrow().read_count
    }
}

struct MockDataInternal {
    revalidate_result: MockRevalidateResult,
    validation_count: usize,
    read_count: usize,
    error_at_done: Option<MockIteratorError>,
    delays: Vec<(DocId, Duration)>,
    /// If true, the next call to `read()` will return `None` even if not at EOF.
    /// Simulates Inverted Index iterators that discover EOF only when `read()` is called.
    force_read_none: bool,
}

impl MockDataInternal {
    fn delay_if_index_limit_reached(&mut self, idx: DocId) {
        // assumes that these delays are sorted in ascending order,
        // as guaranteed by the [`MockData::add_delay_since_index`] method.

        if let Some((limit, delay)) = self.delays.get(0).copied()
            && idx >= limit
        {
            self.delays.remove(0);
            std::thread::sleep(delay);
        }
    }
}

/// Result configured through [`MockData`] that controls what
/// [`Mock::revalidate`] reports.
///
/// The enum mirrors the conceptual outcomes of validation that are
/// relevant for the higher level iterators under test.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MockRevalidateResult {
    #[default]
    Ok,
    Abort,
    Move,
}

impl<'index, const N: usize> Mock<'index, N> {
    /// Create a new [`Mock`] over a fixed array of document ids.
    ///
    /// The ids in `doc_ids` must be sorted in increasing order because
    /// the iterator assumes monotonic forward progress when serving
    /// `read` and `skip_to` calls.
    ///
    /// The internal [`RSIndexResult`] is created as a virtual result
    /// with weight equal to `1.0` and field mask set to
    /// `RS_FIELDMASK_ALL`.  Each call to `read` or `skip_to` overwrites
    /// `doc_id` in that single result instance.
    pub fn new(doc_ids: [DocId; N]) -> Self {
        debug_assert!(doc_ids.is_sorted(), "Mock Iterator API assumes sorted list");
        Self {
            result: RSIndexResult::build_virt()
                .weight(1.)
                .field_mask(RS_FIELDMASK_ALL)
                .build(),
            doc_ids,
            positions: None,
            next_index: 0,
            data: MockData::new(),
        }
    }

    /// Like [`Mock::new`], but each document carries a term position (valid range `1..=127`).
    /// The result produced for each document will be a `Term` record instead of a virtual one,
    pub fn new_with_positions(doc_ids: [DocId; N], positions: [u8; N]) -> Self {
        debug_assert!(
            positions.iter().all(|&p| (1..=127).contains(&p)),
            "positions must be in 1..=127 (single-byte varint range)"
        );
        Self {
            positions: Some(positions),
            ..Self::new(doc_ids)
        }
    }

    /// Return a handle to the shared [`MockData`] of this iterator.
    ///
    /// The returned value clones the underlying `Rc` so it is cheap to
    /// copy and can outlive any particular borrow of the iterator.
    /// Mutations performed through this handle are immediately visible
    /// to the iterator and to other handles that were cloned from it.
    pub fn data(&self) -> MockData {
        MockData(self.data.0.clone())
    }

    /// Replace `self.result` with the appropriate result for the document at `index`.
    ///
    /// If positions are configured, builds a term result with owned offsets for
    /// that document. Otherwise builds a virtual result.
    fn set_result(&mut self, index: usize) {
        let doc_id = self.doc_ids[index];
        if let Some(positions) = self.positions {
            let pos_byte = [positions[index]];
            let offsets = RSOffsetSlice::from_slice(&pos_byte).to_owned();
            self.result = RSIndexResult::build_term()
                .doc_id(doc_id)
                .weight(1.)
                .field_mask(RS_FIELDMASK_ALL)
                .owned_record(None, offsets)
                .build();
        } else {
            self.result.doc_id = doc_id;
        }
    }
}

impl<'index, const N: usize> rqe_iterators::RQEIterator<'index> for Mock<'index, N> {
    type Suspended = MockSuspended<N>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: `Mock<'index, N>` and `MockSuspended<N>` have identical layout
        // (lifetime parameter is phantom in this test helper). Box::from_raw reuses
        // the same heap allocation.
        unsafe { Box::from_raw(raw as *mut MockSuspended<N>) }
    }

    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        Some(&mut self.result)
    }

    fn read(
        &mut self,
    ) -> Result<Option<&mut RSIndexResult<'index>>, rqe_iterators::RQEIteratorError> {
        {
            let mut data = self.data.0.borrow_mut();
            data.read_count += 1;

            // Check if we should force return None (simulating misbehaving iterator)
            if data.force_read_none {
                data.force_read_none = false; // Clear after one use
                return Ok(None);
            }

            if self.at_eof() {
                return if let Some(err) = data.error_at_done {
                    Err(err.into_rqe_iterator_error())
                } else {
                    Ok(None)
                };
            }
        }

        self.set_result(self.next_index);
        self.next_index += 1;

        self.data
            .0
            .borrow_mut()
            .delay_if_index_limit_reached(self.result.doc_id);

        Ok(Some(&mut self.result))
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<rqe_iterators::SkipToOutcome<'_, 'index>>, rqe_iterators::RQEIteratorError>
    {
        let mut data = self.data.0.borrow_mut();

        data.read_count += 1;

        // `skip_to` must be called with a target strictly past the current
        // position; re-skipping to (or below) `last_doc_id()` is a contract
        // violation, mirroring the real iterator's
        // `debug_assert!(self.last_doc_id() < doc_id)` (see
        // `rqe_iterators::inverted_index::core`). Resume-driven re-skips
        // `rewind()` first — exactly like the real `revalidate` path — so they
        // never target the current position.
        debug_assert!(
            self.last_doc_id() < doc_id,
            "skip_to called with a target at or below the current position"
        );

        if self.at_eof() {
            return if let Some(err) = data.error_at_done {
                Err(err.into_rqe_iterator_error())
            } else {
                Ok(None)
            };
        }

        while self.next_index < N && self.doc_ids[self.next_index] < doc_id {
            data.delay_if_index_limit_reached(self.doc_ids[self.next_index]);
            self.next_index += 1;
        }

        data.read_count -= 1; // Decrement the read count before calling Read
        drop(data);

        Ok(self.read()?.map(|result| {
            if result.doc_id == doc_id {
                rqe_iterators::SkipToOutcome::Found(result)
            } else {
                rqe_iterators::SkipToOutcome::NotFound(result)
            }
        }))
    }

    fn rewind(&mut self) {
        self.next_index = 0;
        self.result.doc_id = 0;

        let mut data = self.data.0.borrow_mut();
        data.read_count = 0;
    }

    fn num_estimated(&self) -> usize {
        N
    }

    fn last_doc_id(&self) -> DocId {
        self.result.doc_id
    }

    fn at_eof(&self) -> bool {
        self.next_index >= N
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::Mock
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

impl<'index, const N: usize> WildcardIterator<'index> for Mock<'index, N> {}

impl<const N: usize> rqe_iterators::profile_print::ProfilePrint for Mock<'_, N> {
    fn print_profile(
        &self,
        map: &mut redis_reply::MapBuilder<'_>,
        _ctx: &mut rqe_iterators::profile_print::ProfilePrintCtx<'_>,
    ) {
        map.kv_simple_string(c"Type", c"MOCK");
    }
}

impl<const N: usize> rqe_iterators::profile_print::ProfilePrint for MockSuspended<N> {
    fn print_profile(
        &self,
        map: &mut redis_reply::MapBuilder<'_>,
        _ctx: &mut rqe_iterators::profile_print::ProfilePrintCtx<'_>,
    ) {
        map.kv_simple_string(c"Type", c"MOCK");
    }
}

/// Suspended counterpart of [`Mock`].
///
/// Tests that drive the suspend/resume cycle through the FFI wrapper need the inner
/// iterator type to implement [`RQEIterator`]. The Mock holds no real index
/// borrows (its `result` is a freshly-built [`RSIndexResult`] over owned data), so
/// the suspended counterpart is byte-identical to the active form at any lifetime.
///
/// `#[repr(C)]` so the layout matches [`Mock`] for the `suspend`/`resume`
/// pointer relabel (see [`Mock`]).
#[repr(C)]
pub struct MockSuspended<const N: usize> {
    result: RSIndexResult<'static>,
    doc_ids: [DocId; N],
    positions: Option<[u8; N]>,
    next_index: usize,
    data: MockData,
}

// Compile-time proof that the `Mock` and `MockSuspended` are layout-identical.
const _: () = {
    use std::mem::offset_of;

    const SIZE: usize = 5;
    type A<'a> = Mock<'a, SIZE>;
    type S = MockSuspended<SIZE>;

    // Every field starts at the same offset.
    assert!(offset_of!(A, result) == offset_of!(S, result));
    assert!(offset_of!(A, doc_ids) == offset_of!(S, doc_ids));
    assert!(offset_of!(A, positions) == offset_of!(S, positions));
    assert!(offset_of!(A, next_index) == offset_of!(S, next_index));
    assert!(offset_of!(A, data) == offset_of!(S, data));

    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

impl<'query, const N: usize> rqe_iterators::RQESuspendedIterator<'query> for MockSuspended<N> {
    type Resumed<'a>
        = Mock<'a, N>
    where
        'query: 'a;

    fn resume<'a>(
        mut self: Box<Self>,
        _guard: &index_spec::IndexSpecReadGuard<'a>,
    ) -> Result<rqe_iterators::ResumeOutcome<Box<Mock<'a, N>>>, rqe_iterators::RQEIteratorError>
    where
        'query: 'a,
    {
        // Honour the [`MockRevalidateResult`] configured on the mock's
        // [`MockData`] — mirrors what `Mock::revalidate` did on the legacy
        // path, so tests driving suspend/resume see the same per-mock
        // outcomes as before. Without this, every resume would report OK
        // and tests that pre-configure Abort/Move behaviour would silently
        // see the Ok path.
        let revalidate_result = {
            let mut data = self.data.0.borrow_mut();
            data.validation_count += 1;
            data.revalidate_result
        };
        let moved = match revalidate_result {
            MockRevalidateResult::Ok => false,
            MockRevalidateResult::Move => {
                // Mirror `Mock::revalidate`'s Move case: set the result to
                // the doc id at `next_index`, then advance past it. This
                // matches the legacy path's side effects so suspend/resume
                // and revalidate produce indistinguishable end states.
                if self.next_index < N {
                    // Rebuild the full result (doc_id + offsets for positioned
                    // mocks), mirroring `Mock::set_result` on the legacy
                    // `revalidate` path. Without this, positioned mocks would
                    // carry stale offsets after a resume-driven move.
                    let index = self.next_index;
                    let doc_id = self.doc_ids[index];
                    if let Some(positions) = self.positions {
                        let pos_byte = [positions[index]];
                        let offsets = RSOffsetSlice::from_slice(&pos_byte).to_owned();
                        self.result = RSIndexResult::build_term()
                            .doc_id(doc_id)
                            .weight(1.)
                            .field_mask(RS_FIELDMASK_ALL)
                            .owned_record(None, offsets)
                            .build();
                    } else {
                        self.result.doc_id = doc_id;
                    }
                    self.next_index += 1;
                }
                true
            }
            // Unrecoverable: drop the suspended mock, materialize no active iterator.
            MockRevalidateResult::Abort => return Ok(rqe_iterators::ResumeOutcome::Aborted),
        };
        let raw = Box::into_raw(self);
        // SAFETY: layout-identical (see [`Mock::suspend`]).
        let active = unsafe { Box::from_raw(raw as *mut Mock<'a, N>) };
        Ok(if moved {
            rqe_iterators::ResumeOutcome::Moved(active)
        } else {
            rqe_iterators::ResumeOutcome::Ok(active)
        })
    }

    fn last_doc_id(&self) -> DocId {
        // Mirror the active `Mock::last_doc_id`, which returns `result.doc_id`.
        // Suspend copies `result` into `_result`, so this matches exactly;
        // composite resume compares children via this value.
        self.result.doc_id
    }

    fn num_estimated(&self) -> usize {
        self.doc_ids.len()
    }
}

/// Dynamic-size variant of [`Mock`] that uses a [`Vec`] instead of a fixed array.
///
/// This is useful when the document IDs are determined at runtime.
///
/// `#[repr(C)]` so that `MockVec<'index>` and [`MockVecSuspended`] are
/// layout-compatible for the `suspend`/`resume` pointer relabel (see [`Mock`]).
#[repr(C)]
pub struct MockVec<'index> {
    result: RSIndexResult<'index>,
    doc_ids: Vec<DocId>,
    next_index: usize,
    data: MockData,
}

impl<'index> MockVec<'index> {
    /// Create a new [`MockVec`] from a vector of document ids.
    ///
    /// The ids must be sorted in increasing order.
    pub fn new(doc_ids: Vec<DocId>) -> Self {
        debug_assert!(doc_ids.is_sorted(), "MockVec API assumes sorted list");
        Self {
            result: RSIndexResult::build_virt()
                .weight(1.)
                .field_mask(RS_FIELDMASK_ALL)
                .build(),
            doc_ids,
            next_index: 0,
            data: MockData::new(),
        }
    }

    /// Create a [`MockVec`] wrapped as a [`TypeErasedRQEIterator`].
    pub fn new_boxed(doc_ids: Vec<DocId>) -> TypeErasedRQEIterator<'index> {
        TypeErasedRQEIterator::new(Box::new(Self::new(doc_ids)))
    }
}

impl<'index> rqe_iterators::RQEIterator<'index> for MockVec<'index> {
    type Suspended = MockVecSuspended;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: `MockVec<'index>` and `MockVecSuspended` have identical layout
        // (lifetime parameter is phantom in this test helper).
        unsafe { Box::from_raw(raw as *mut MockVecSuspended) }
    }

    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        Some(&mut self.result)
    }

    fn read(
        &mut self,
    ) -> Result<Option<&mut RSIndexResult<'index>>, rqe_iterators::RQEIteratorError> {
        {
            let mut data = self.data.0.borrow_mut();
            data.read_count += 1;

            if data.force_read_none {
                data.force_read_none = false;
                return Ok(None);
            }

            if self.at_eof() {
                return if let Some(err) = data.error_at_done {
                    Err(err.into_rqe_iterator_error())
                } else {
                    Ok(None)
                };
            }
        }

        self.result.doc_id = self.doc_ids[self.next_index];
        self.next_index += 1;

        self.data
            .0
            .borrow_mut()
            .delay_if_index_limit_reached(self.result.doc_id);

        Ok(Some(&mut self.result))
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<rqe_iterators::SkipToOutcome<'_, 'index>>, rqe_iterators::RQEIteratorError>
    {
        let mut data = self.data.0.borrow_mut();

        data.read_count += 1;

        // See `Mock::skip_to` — the target must be strictly past the current
        // position, mirroring the real iterator's
        // `debug_assert!(self.last_doc_id() < doc_id)`.
        debug_assert!(
            self.last_doc_id() < doc_id,
            "skip_to called with a target at or below the current position"
        );

        let n = self.doc_ids.len();
        if self.at_eof() {
            return if let Some(err) = data.error_at_done {
                Err(err.into_rqe_iterator_error())
            } else {
                Ok(None)
            };
        }

        while self.next_index < n && self.doc_ids[self.next_index] < doc_id {
            data.delay_if_index_limit_reached(self.doc_ids[self.next_index]);
            self.next_index += 1;
        }

        data.read_count -= 1;
        drop(data);

        Ok(self.read()?.map(|result| {
            if result.doc_id == doc_id {
                rqe_iterators::SkipToOutcome::Found(result)
            } else {
                rqe_iterators::SkipToOutcome::NotFound(result)
            }
        }))
    }

    fn rewind(&mut self) {
        self.next_index = 0;
        self.result.doc_id = 0;

        let mut data = self.data.0.borrow_mut();
        data.read_count = 0;
    }

    fn num_estimated(&self) -> usize {
        self.doc_ids.len()
    }

    fn last_doc_id(&self) -> DocId {
        self.result.doc_id
    }

    fn at_eof(&self) -> bool {
        self.next_index >= self.doc_ids.len()
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::Mock
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

/// Suspended counterpart of [`MockVec`].
///
/// Layout-identical to [`MockVec`] (the lifetime parameter is purely phantom
/// in this test helper: `result` is a freshly-built [`RSIndexResult`] over
/// owned data).
///
/// `#[repr(C)]` so the layout matches [`MockVec`] for the `suspend`/`resume`
/// pointer relabel (see [`Mock`]).
#[repr(C)]
pub struct MockVecSuspended {
    result: RSIndexResult<'static>,
    doc_ids: Vec<DocId>,
    next_index: usize,
    data: MockData,
}

// Compile-time proof that the `MockVec` and `MockVecSuspended` are layout-identical.
const _: () = {
    use std::mem::offset_of;

    type A<'a> = MockVec<'a>;
    type S = MockVecSuspended;

    // Every field starts at the same offset.
    assert!(offset_of!(A, result) == offset_of!(S, result));
    assert!(offset_of!(A, doc_ids) == offset_of!(S, doc_ids));
    assert!(offset_of!(A, next_index) == offset_of!(S, next_index));
    assert!(offset_of!(A, data) == offset_of!(S, data));

    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

impl<'query> rqe_iterators::RQESuspendedIterator<'query> for MockVecSuspended {
    type Resumed<'a>
        = MockVec<'a>
    where
        'query: 'a;

    fn resume<'a>(
        mut self: Box<Self>,
        _guard: &index_spec::IndexSpecReadGuard<'a>,
    ) -> Result<rqe_iterators::ResumeOutcome<Box<MockVec<'a>>>, rqe_iterators::RQEIteratorError>
    where
        'query: 'a,
    {
        // Honour the [`MockRevalidateResult`] configured on the mock's
        // [`MockData`] — mirrors what `MockVec::revalidate` does on the legacy
        // path, so tests driving suspend/resume see the same per-mock
        // outcomes as before.
        let revalidate_result = {
            let mut data = self.data.0.borrow_mut();
            data.validation_count += 1;
            data.revalidate_result
        };
        let moved = match revalidate_result {
            MockRevalidateResult::Ok => false,
            MockRevalidateResult::Move => {
                if self.next_index < self.doc_ids.len() {
                    // Mirror `MockVec::revalidate`'s Move path: advance to the
                    // next doc id. `MockVec` results are always virtual (no
                    // offsets), so setting `doc_id` alone is sufficient.
                    self.result.doc_id = self.doc_ids[self.next_index];
                    self.next_index += 1;
                }
                true
            }
            // Unrecoverable: drop the suspended mock, materialize no active iterator.
            MockRevalidateResult::Abort => return Ok(rqe_iterators::ResumeOutcome::Aborted),
        };
        let raw = Box::into_raw(self);
        // SAFETY: layout-identical (see [`MockVec::suspend`]).
        let active = unsafe { Box::from_raw(raw as *mut MockVec<'a>) };
        Ok(if moved {
            rqe_iterators::ResumeOutcome::Moved(active)
        } else {
            rqe_iterators::ResumeOutcome::Ok(active)
        })
    }

    fn last_doc_id(&self) -> DocId {
        // Mirror the active `MockVec::last_doc_id`, which returns `result.doc_id`.
        self.result.doc_id
    }

    fn num_estimated(&self) -> usize {
        self.doc_ids.len()
    }
}

impl rqe_iterators::profile_print::ProfilePrint for MockVecSuspended {
    fn print_profile(
        &self,
        map: &mut redis_reply::MapBuilder<'_>,
        _ctx: &mut rqe_iterators::profile_print::ProfilePrintCtx<'_>,
    ) {
        map.kv_simple_string(c"Type", c"MOCK VEC");
    }
}

impl rqe_iterators::profile_print::ProfilePrint for MockVec<'_> {
    fn print_profile(
        &self,
        map: &mut redis_reply::MapBuilder<'_>,
        _ctx: &mut rqe_iterators::profile_print::ProfilePrintCtx<'_>,
    ) {
        map.kv_simple_string(c"Type", c"MOCK VEC");
    }
}
