/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for the two disk-mode paths that cross into the storage backend:
//! `TagIndex::index`, which stages postings through `SearchDisk_IndexTags`, and
//! `TagIndex::open_reader`, which builds a reader through the registered
//! enterprise iterators.
//!
//! Both backends are closed source, so each is replaced here by a recording
//! stand-in — the C `disk` API table for the write path, a
//! `SearchEnterpriseIterators` implementation for the read path. What is under
//! test is the marshalling either side of that boundary: which tags, document id
//! and field index reach the backend, and what the Rust API makes of the answer
//! it gives back.

use std::{
    ffi::{CStr, CString, c_char, c_void},
    ptr::NonNull,
    sync::{
        Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
};

use ffi::{
    RSToken, RedisSearchDiskAPI, RedisSearchDiskIndexSpec, RedisSearchDiskSnapshot,
    SearchDiskWriteBatchHandle, t_docId, t_fieldIndex,
};
use redis_module::RedisModuleCtx;
use rqe_iterators::{
    QueryError, RQEIterator, RQEIteratorPrintable, SEARCH_ENTERPRISE_ITERATORS,
    SearchEnterpriseIterators, wildcard::Wildcard,
};
use rqe_iterators_test_utils::MockContext;
use tag_index::{OnDiskMode, TagIndex};

use crate::util::as_tag;

/// Field index the indexes under test are created with. Deliberately not `0`, so
/// a path that dropped the field id on the floor would show up as a mismatch
/// rather than as an accidental pass.
const FIELD_INDEX: t_fieldIndex = 7;

/// A disk index spec the fake backends never dereference, but which is a real
/// allocation: the read path takes a reference to it on the way to the backend,
/// and a reference has to be in bounds even when nothing reads through it.
struct FakeDiskSpec {
    /// Keeps the spec allocation alive. Every use goes through
    /// [`ptr`](Self::ptr), the one pointer derived from it.
    _spec: Box<RedisSearchDiskIndexSpec>,
    ptr: NonNull<RedisSearchDiskIndexSpec>,
}

impl FakeDiskSpec {
    fn new() -> Self {
        let mut spec = Box::new(std::ptr::null::<c_void>());
        let ptr = NonNull::from(&mut *spec);
        Self { _spec: spec, ptr }
    }

    /// A disk-mode index over this spec, with the suffix trie disabled.
    fn index(&self) -> TagIndex<OnDiskMode> {
        // SAFETY: `self.ptr` points at the boxed spec, which outlives the
        // returned index — every test drops the index first.
        unsafe { TagIndex::<OnDiskMode>::new(self.ptr, FIELD_INDEX, false) }
    }

    /// The address the backend should see as the index's spec.
    fn addr(&self) -> usize {
        self.ptr.as_ptr() as usize
    }
}

// -------------------------------------------------------------------------
// Write path: a fake `disk` API table standing in for the storage backend.
// -------------------------------------------------------------------------

// The disk backend's global API table, defined in `src/search_disk.c`. It is left
// NULL in the open-source build and every `SearchDisk_*` entry point dispatches
// through it, so installing a table here is what makes the write path callable
// from a test.
unsafe extern "C" {
    // Lowercase to match the C global's name.
    static mut disk: *mut RedisSearchDiskAPI;
}

/// One `indexTags` call, as the fake backend saw it. Pointers are recorded as
/// addresses so a call record is [`Send`].
#[derive(Debug, PartialEq, Eq)]
struct IndexTagsCall {
    ctx: usize,
    spec: usize,
    batch: usize,
    values: Vec<Vec<u8>>,
    doc_id: t_docId,
    field_index: t_fieldIndex,
}

/// Serialises the tests that install a fake `disk` table, which is process-global.
static DISK_API_LOCK: Mutex<()> = Mutex::new(());

/// What the fake `indexTags` has recorded so far.
static INDEX_TAGS_CALLS: Mutex<Vec<IndexTagsCall>> = Mutex::new(Vec::new());

/// What the fake `indexTags` answers.
static INDEX_TAGS_ANSWER: AtomicBool = AtomicBool::new(true);

unsafe extern "C" fn fake_index_tags(
    ctx: *mut RedisModuleCtx,
    spec: *mut RedisSearchDiskIndexSpec,
    batch: *mut SearchDiskWriteBatchHandle,
    values: *mut *const c_char,
    num_values: usize,
    doc_id: t_docId,
    field_index: t_fieldIndex,
) -> bool {
    // SAFETY: `TagIndex::index` passes an array of `num_values` pointers, each
    // one taken from a live `&CStr`, so the array and every string in it are
    // readable for the duration of this call.
    let values = unsafe { std::slice::from_raw_parts(values, num_values) }
        .iter()
        // SAFETY: as above — every entry is a live, NUL-terminated `&CStr`.
        .map(|value| unsafe { CStr::from_ptr(*value) }.to_bytes().to_vec())
        .collect();

    INDEX_TAGS_CALLS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push(IndexTagsCall {
            ctx: ctx as usize,
            spec: spec as usize,
            batch: batch as usize,
            values,
            doc_id,
            field_index,
        });

    INDEX_TAGS_ANSWER.load(Ordering::Relaxed)
}

/// Installs [`fake_index_tags`] as the disk backend for as long as it is held,
/// then restores the previous global.
struct FakeIndexTagsBackend {
    _lock: MutexGuard<'static, ()>,
    previous: *mut RedisSearchDiskAPI,
    /// Keeps the table `disk` points at alive for the guard's lifetime.
    _api: Box<RedisSearchDiskAPI>,
}

impl FakeIndexTagsBackend {
    /// `answer` is what the fake backend returns from every call it records.
    fn install(answer: bool) -> Self {
        let lock = DISK_API_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        INDEX_TAGS_CALLS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
        INDEX_TAGS_ANSWER.store(answer, Ordering::Relaxed);

        // SAFETY: every field of the disk API table is a nullable function
        // pointer, whose all-zero bit pattern is the valid `None`.
        let mut api: Box<RedisSearchDiskAPI> = Box::new(unsafe { std::mem::zeroed() });
        api.index.indexTags = Some(fake_index_tags);

        // SAFETY: `lock` is held, so no other test is touching the global.
        let previous = unsafe { disk };
        // SAFETY: as above, plus `api` outlives the pointer stored here because
        // the guard below owns it.
        unsafe { disk = std::ptr::from_mut(&mut *api) };

        Self {
            _lock: lock,
            previous,
            _api: api,
        }
    }

    /// Everything the fake backend recorded since it was installed, in order.
    fn calls(&self) -> Vec<IndexTagsCall> {
        std::mem::take(
            &mut *INDEX_TAGS_CALLS
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }
}

impl Drop for FakeIndexTagsBackend {
    fn drop(&mut self) {
        // SAFETY: `self._lock` is still held, so no other test is reading or
        // writing the global.
        unsafe { disk = self.previous };
    }
}

// -------------------------------------------------------------------------
// Read path: a fake enterprise iterator backend.
// -------------------------------------------------------------------------

/// The `top_id` of the [`Wildcard`] the fake enterprise backend hands back. It
/// serves as a fingerprint: an iterator reporting this estimate came from the
/// backend and not from anywhere else.
const MOCK_TAG_TOP_ID: t_docId = 4242;

/// The one tag the fake enterprise backend refuses to build an iterator for.
const FAILING_TAG: &[u8] = b"backend-error";

/// The message that refusal carries.
const BACKEND_ERROR: &str = "the disk backend could not open this tag";

/// One `new_tag_on_disk` call, as the fake backend saw it.
#[derive(Debug, PartialEq)]
struct TagIteratorCall {
    spec: usize,
    token: Vec<u8>,
    field_index: t_fieldIndex,
    weight: f64,
    snapshot: usize,
}

/// What the fake enterprise backend has recorded so far.
static TAG_ITERATOR_CALLS: Mutex<Vec<TagIteratorCall>> = Mutex::new(Vec::new());

/// Stands in for the closed-source disk iterators, recording what the tag reader
/// forwards and answering with a recognisable iterator — or, for [`FAILING_TAG`],
/// with a failure.
struct FakeEnterpriseIterators;

impl SearchEnterpriseIterators for FakeEnterpriseIterators {
    fn new_wildcard_on_disk<'index>(
        &self,
        _index: &'index mut RedisSearchDiskIndexSpec,
        _weight: f64,
        _snapshot: NonNull<RedisSearchDiskSnapshot>,
        _status: Option<&mut QueryError>,
    ) -> Result<Box<dyn RQEIteratorPrintable<'index> + 'index>, Box<dyn std::error::Error>> {
        unimplemented!("the tag index never opens a disk wildcard iterator")
    }

    fn new_term_on_disk_with_offsets<'index>(
        &self,
        _index: &'index mut RedisSearchDiskIndexSpec,
        _query_term: Box<query_term::RSQueryTerm>,
        _field_mask: inverted_index::FieldMask,
        _weight: f64,
        _snapshot: NonNull<RedisSearchDiskSnapshot>,
    ) -> Result<Box<dyn RQEIteratorPrintable<'index> + 'index>, Box<dyn std::error::Error>> {
        unimplemented!("the tag index never opens a disk term iterator")
    }

    fn new_term_on_disk_without_offsets<'index>(
        &self,
        _index: &'index mut RedisSearchDiskIndexSpec,
        _query_term: Box<query_term::RSQueryTerm>,
        _field_mask: inverted_index::FieldMask,
        _weight: f64,
        _snapshot: NonNull<RedisSearchDiskSnapshot>,
    ) -> Result<Box<dyn RQEIteratorPrintable<'index> + 'index>, Box<dyn std::error::Error>> {
        unimplemented!("the tag index never opens a disk term iterator")
    }

    fn new_tag_on_disk<'index>(
        &self,
        index: &'index mut RedisSearchDiskIndexSpec,
        token: &RSToken,
        field_index: t_fieldIndex,
        weight: f64,
        snapshot: NonNull<RedisSearchDiskSnapshot>,
    ) -> Result<Box<dyn RQEIteratorPrintable<'index> + 'index>, Box<dyn std::error::Error>> {
        // SAFETY: `TagIndex::open_reader` points `token` at the tag it was given,
        // which outlives this call.
        let token = unsafe { std::slice::from_raw_parts(token.str_.cast::<u8>(), token.len) };

        TAG_ITERATOR_CALLS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(TagIteratorCall {
                spec: std::ptr::from_mut(index) as usize,
                token: token.to_vec(),
                field_index,
                weight,
                snapshot: snapshot.as_ptr() as usize,
            });

        if token == FAILING_TAG {
            return Err(BACKEND_ERROR.into());
        }
        Ok(Box::new(Wildcard::new(MOCK_TAG_TOP_ID, weight)))
    }

    fn new_numeric_on_disk<'index>(
        &self,
        _index: &'index mut RedisSearchDiskIndexSpec,
        _filter: &inverted_index::NumericFilter,
        _field_index: t_fieldIndex,
        _snapshot: NonNull<RedisSearchDiskSnapshot>,
    ) -> Result<Box<dyn RQEIteratorPrintable<'index> + 'index>, Box<dyn std::error::Error>> {
        unimplemented!("the tag index never opens a disk numeric iterator")
    }

    fn new_geo_on_disk<'index>(
        &self,
        _index: &'index mut RedisSearchDiskIndexSpec,
        _gf: &'index mut ffi::GeoFilter,
        _field_index: t_fieldIndex,
        _snapshot: NonNull<RedisSearchDiskSnapshot>,
    ) -> Result<Box<dyn RQEIteratorPrintable<'index> + 'index>, Box<dyn std::error::Error>> {
        unimplemented!("the tag index never opens a disk geo iterator")
    }
}

/// Serialises the tests reading [`TAG_ITERATOR_CALLS`], which — like the
/// registration itself — is process-global.
static ENTERPRISE_LOCK: Mutex<()> = Mutex::new(());

/// Claims [`FakeEnterpriseIterators`] for as long as it is held.
struct FakeEnterpriseBackend {
    _lock: MutexGuard<'static, ()>,
}

impl FakeEnterpriseBackend {
    /// Register the fake backend and drop whatever an earlier test recorded. The
    /// registration itself happens once per process: the `OnceLock` keeps the
    /// first value, and there is only ever this one.
    fn install() -> Self {
        let lock = ENTERPRISE_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        SEARCH_ENTERPRISE_ITERATORS.get_or_init(|| Box::new(FakeEnterpriseIterators));
        TAG_ITERATOR_CALLS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
        Self { _lock: lock }
    }

    /// Everything the fake backend recorded since it was installed, in order.
    fn calls(&self) -> Vec<TagIteratorCall> {
        std::mem::take(
            &mut *TAG_ITERATOR_CALLS
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }
}

// -------------------------------------------------------------------------
// Write path tests.
// -------------------------------------------------------------------------

/// A non-null write context and batch. `TagIndex::index` only forwards them, and
/// the fake backend only records their addresses, so nothing ever dereferences
/// either.
const fn write_handles() -> (*mut RedisModuleCtx, *mut SearchDiskWriteBatchHandle) {
    (
        NonNull::<RedisModuleCtx>::dangling().as_ptr(),
        NonNull::<SearchDiskWriteBatchHandle>::dangling().as_ptr(),
    )
}

/// `index` hands the backend one call carrying every tag as a C string, in the
/// order given, together with the document id and the index's own field id.
#[test]
fn index_forwards_the_tags_document_and_field_to_the_backend() {
    const DOC_ID: t_docId = 11;

    let backend = FakeIndexTagsBackend::install(true);
    let spec = FakeDiskSpec::new();
    let mut idx = spec.index();
    let (ctx, batch) = write_handles();

    let tags: Vec<CString> = [&b"foo"[..], b"bar", b"foo"]
        .iter()
        .map(|tag| CString::new(*tag).expect("test literal is NUL-free"))
        .collect();
    let tags: Vec<&CStr> = tags.iter().map(CString::as_c_str).collect();

    // SAFETY: `ctx` and `batch` are the non-null handles the fake backend
    // expects, and it never dereferences them.
    let ok = unsafe { idx.index(ctx, batch, &tags, DOC_ID) };
    assert!(ok, "the fake backend accepted the write");

    assert_eq!(
        backend.calls(),
        vec![IndexTagsCall {
            ctx: ctx as usize,
            spec: spec.addr(),
            batch: batch as usize,
            // The repeated `foo` is *not* deduplicated: `index` stages exactly
            // what the document holds and leaves collapsing to the backend.
            values: vec![b"foo".to_vec(), b"bar".to_vec(), b"foo".to_vec()],
            doc_id: DOC_ID,
            field_index: FIELD_INDEX,
        }]
    );
}

/// The empty tag (INDEXEMPTY) reaches the backend as an empty C string rather
/// than being skipped on the way.
#[test]
fn index_forwards_the_empty_tag() {
    let backend = FakeIndexTagsBackend::install(true);
    let spec = FakeDiskSpec::new();
    let mut idx = spec.index();
    let (ctx, batch) = write_handles();

    // SAFETY: as in `index_forwards_the_tags_document_and_field_to_the_backend`.
    let ok = unsafe { idx.index(ctx, batch, &[c""], 1) };
    assert!(ok);

    let calls = backend.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].values, vec![Vec::<u8>::new()]);
}

/// A document with no tags for this field is not a backend write at all.
#[test]
fn index_without_tags_does_not_reach_the_backend() {
    let backend = FakeIndexTagsBackend::install(false);
    let spec = FakeDiskSpec::new();
    let mut idx = spec.index();
    let (ctx, batch) = write_handles();

    // SAFETY: as in `index_forwards_the_tags_document_and_field_to_the_backend`.
    let ok = unsafe { idx.index(ctx, batch, &[], 1) };

    assert!(
        ok,
        "an empty write succeeds without consulting the backend, which here \
         would have refused"
    );
    assert_eq!(backend.calls(), vec![]);
}

/// A backend refusal is reported to the caller, which aborts the write batch on
/// the strength of it.
#[test]
fn index_propagates_a_backend_refusal() {
    let _backend = FakeIndexTagsBackend::install(false);
    let spec = FakeDiskSpec::new();
    let mut idx = spec.index();
    let (ctx, batch) = write_handles();

    // SAFETY: as in `index_forwards_the_tags_document_and_field_to_the_backend`.
    let ok = unsafe { idx.index(ctx, batch, &[c"foo"], 1) };
    assert!(!ok, "the refusal is passed on rather than swallowed");
}

// -------------------------------------------------------------------------
// Read path tests.
// -------------------------------------------------------------------------

/// `open_reader` builds its reader through the backend, forwarding the tag, the
/// index's field id, the caller's weight and the search context's snapshot.
#[test]
fn open_reader_forwards_the_tag_field_and_snapshot_to_the_backend() {
    let backend = FakeEnterpriseBackend::install();

    let spec = FakeDiskSpec::new();
    let idx = spec.index();
    let mock = MockContext::new(0, 0);
    let mut snapshot: RedisSearchDiskSnapshot = std::ptr::null();
    let snapshot = std::ptr::from_mut(&mut snapshot);
    // SAFETY: `snapshot` outlives the iterator, which is dropped at the end of
    // this test.
    unsafe { mock.set_disk_snapshot(snapshot) };

    // SAFETY: `idx` and `mock` outlive the iterator; the snapshot is the one just
    // installed; and nothing else references the spec while the iterator lives.
    let it = unsafe { idx.open_reader(mock.sctx(), as_tag(b"hello"), 2.5) }
        .expect("the fake backend builds an iterator for every tag but the failing one");

    assert_eq!(
        it.num_estimated(),
        MOCK_TAG_TOP_ID as usize,
        "the reader is the one the backend returned"
    );
    assert_eq!(
        backend.calls(),
        vec![TagIteratorCall {
            spec: spec.addr(),
            token: b"hello".to_vec(),
            field_index: FIELD_INDEX,
            weight: 2.5,
            snapshot: snapshot as usize,
        }]
    );
}

/// A tag whose bytes carry no NUL terminator still reaches the backend intact:
/// the token is passed as pointer plus length, so the reader does not need one.
#[test]
fn open_reader_forwards_the_empty_tag() {
    let backend = FakeEnterpriseBackend::install();

    let spec = FakeDiskSpec::new();
    let idx = spec.index();
    let mock = MockContext::new(0, 0);
    mock.set_dummy_disk_snapshot();

    // SAFETY: as in `open_reader_forwards_the_tag_field_and_snapshot_to_the_backend`.
    let it = unsafe { idx.open_reader(mock.sctx(), as_tag(b""), 1.0) };
    assert!(it.is_ok());

    let calls = backend.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].token, Vec::<u8>::new());
}

/// When the backend cannot build an iterator the failure is returned rather than
/// turned into an empty reader, so the query aborts instead of silently missing
/// documents.
#[test]
fn open_reader_propagates_a_backend_failure() {
    let _backend = FakeEnterpriseBackend::install();

    let spec = FakeDiskSpec::new();
    let idx = spec.index();
    let mock = MockContext::new(0, 0);
    mock.set_dummy_disk_snapshot();

    // SAFETY: as in `open_reader_forwards_the_tag_field_and_snapshot_to_the_backend`.
    let Err(err) = (unsafe { idx.open_reader(mock.sctx(), as_tag(FAILING_TAG), 1.0) }) else {
        panic!("the fake backend refuses this tag");
    };
    assert_eq!(err.to_string(), BACKEND_ERROR);
}
