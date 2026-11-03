/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

extern crate redisearch_rs;

redis_mock::mock_or_stub_missing_redis_c_symbols!();

use std::{
    ffi::{CStr, CString, c_void},
    mem, ptr,
};

use dict::{KeysDictType, OwnedDict};
use index_spec::{IndexSpec, IndexSpecWeakRef};
use pretty_assertions::assert_eq;

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn field_specs() {
    let mut index_spec = unsafe { mem::zeroed::<ffi::IndexSpec>() };
    let fs0 = field_spec(c"aaa", c"bbb", 0);
    let fs1 = field_spec(c"ccc", c"ddd", 1);
    let mut fields = [fs0, fs1];
    index_spec.fields = ptr::from_mut(&mut fields).cast::<ffi::FieldSpec>();
    index_spec.numFields = fields.len().try_into().unwrap();
    let sut = unsafe { IndexSpec::from_raw(ptr::from_ref(&index_spec)) };

    let fss = sut.field_specs();

    assert_eq!(fss.len(), fields.len());
    assert_eq!(
        unsafe { fss[0].to_raw().as_ref() }.unwrap().index,
        fs0.index
    );
    assert_eq!(
        unsafe { fss[1].to_raw().as_ref() }.unwrap().index,
        fs1.index
    );

    unsafe {
        ffi::HiddenString_Free(fs0.fieldName, false);
        ffi::HiddenString_Free(fs0.fieldPath, false);
        ffi::HiddenString_Free(fs1.fieldName, false);
        ffi::HiddenString_Free(fs1.fieldPath, false);
    }
}

#[test]
fn rule() {
    let mut index_spec = unsafe { mem::zeroed::<ffi::IndexSpec>() };
    let mut schema_rule = unsafe { mem::zeroed::<ffi::SchemaRule>() };
    schema_rule.type_ = document::DocumentType::Json;
    index_spec.rule = ptr::from_mut(&mut schema_rule);
    let sut = unsafe { IndexSpec::from_raw(ptr::from_ref(&index_spec)) };

    let rule = sut.rule();

    assert_eq!(rule.type_(), document::DocumentType::Json);
}

fn field_spec(field_name: &CStr, field_path: &CStr, index: u16) -> ffi::FieldSpec {
    let mut res = unsafe { mem::zeroed::<ffi::FieldSpec>() };
    res.fieldName =
        unsafe { ffi::NewHiddenString(field_name.as_ptr(), field_name.count_bytes(), false) };
    res.fieldPath =
        unsafe { ffi::NewHiddenString(field_path.as_ptr(), field_path.count_bytes(), false) };
    res.index = index;
    res
}

unsafe extern "C" {
    fn StrongRef_New(
        obj: *mut c_void,
        free: Option<unsafe extern "C" fn(*mut c_void)>,
    ) -> ffi::StrongRef;
    fn StrongRef_Demote(strong: ffi::StrongRef) -> ffi::WeakRef;
    fn StrongRef_Release(strong: ffi::StrongRef);
    fn StrongRef_Invalidate(strong: ffi::StrongRef);
    fn WeakRef_Release(weak: ffi::WeakRef);
}

/// The spec is owned by the test, so the last reference must not free it.
const unsafe extern "C" fn keep_spec(_: *mut c_void) {}

/// The read lock taken from Rust excludes writers and pauses keys-dict
/// rehashing until it is dropped, and its guard reads the locked spec.
#[test]
#[cfg_attr(miri, ignore = "takes the C spec lock")]
fn the_read_lock_excludes_writers_until_dropped() {
    redis_mock::init_redis_module_mock();
    let keys_dict = OwnedDict::<KeysDictType>::create();
    // SAFETY: zero is valid for every field this test leaves unset.
    let mut spec = unsafe { mem::zeroed::<ffi::IndexSpec>() };
    // SAFETY: the static name is copied, because the spec takes ownership.
    spec.specName = unsafe { ffi::NewHiddenString(c"idx".as_ptr(), 3, true) };
    spec.obfuscatedName = CString::new("Index@test").unwrap().into_raw();
    spec.keysDict = keys_dict.as_mut_ptr();
    let rwlock = (&raw mut spec.rwlock).cast::<libc::pthread_rwlock_t>();
    // SAFETY: `rwlock` points at the spec's own, not yet used, lock storage.
    assert_eq!(unsafe { libc::pthread_rwlock_init(rwlock, ptr::null()) }, 0);

    // SAFETY: the spec outlives both references, which are released below, and
    // `keep_spec` leaves its storage to the test.
    let strong = unsafe { StrongRef_New((&raw mut spec).cast(), Some(keep_spec)) };
    // SAFETY: `strong` is the live reference created above.
    let weak = unsafe { StrongRef_Demote(strong) };
    // SAFETY: `weak` is a valid weak reference until it is released below.
    let spec_ref = unsafe { IndexSpecWeakRef::from_raw(weak) }
        .promote()
        .expect("the spec is alive");

    // SAFETY: the dict is live, and only this thread touches it.
    let paused = || unsafe { (*keys_dict.as_mut_ptr()).pauserehash };
    // SAFETY: `rwlock` was initialized above and stays live; a successful try is
    // undone right away.
    let writer_gets_in = || unsafe {
        let rc = libc::pthread_rwlock_trywrlock(rwlock);
        if rc == 0 {
            libc::pthread_rwlock_unlock(rwlock);
        }
        rc == 0
    };
    {
        let lock = spec_ref.read().expect("the spec is valid");
        assert_eq!(lock.guard().as_ptr(), &raw const spec);
        assert_eq!(paused(), 1);
        assert!(
            !writer_gets_in(),
            "a writer waits while the read lock is held"
        );
    }
    assert_eq!(paused(), 0);
    assert!(writer_gets_in(), "dropping the lock lets a writer in");

    drop(spec_ref);
    // SAFETY: both references were created above and are released once, and the
    // spec's names were allocated above and are freed once.
    unsafe {
        WeakRef_Release(weak);
        StrongRef_Release(strong);
        libc::pthread_rwlock_destroy(rwlock);
        ffi::HiddenString_Free(spec.specName, true);
        drop(CString::from_raw(spec.obfuscatedName));
    }
}

static FREES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Counts frees and leaves the storage to the test.
unsafe extern "C" fn count_free(_: *mut c_void) {
    FREES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

/// A read lock that is forgotten instead of dropped keeps its spec alive: the
/// reference it was taken from can go, and the spec is not freed under the lock.
#[test]
#[cfg_attr(miri, ignore = "takes the C spec lock")]
fn a_forgotten_read_lock_keeps_its_spec_alive() {
    redis_mock::init_redis_module_mock();
    let keys_dict = OwnedDict::<KeysDictType>::create();
    // SAFETY: zero is valid for every field this test leaves unset.
    let mut spec = unsafe { mem::zeroed::<ffi::IndexSpec>() };
    // SAFETY: the static name is copied, because the spec takes ownership.
    spec.specName = unsafe { ffi::NewHiddenString(c"idx".as_ptr(), 3, true) };
    spec.obfuscatedName = CString::new("Index@test").unwrap().into_raw();
    spec.keysDict = keys_dict.as_mut_ptr();
    let rwlock = (&raw mut spec.rwlock).cast::<libc::pthread_rwlock_t>();
    // SAFETY: `rwlock` points at the spec's own, not yet used, lock storage.
    assert_eq!(unsafe { libc::pthread_rwlock_init(rwlock, ptr::null()) }, 0);
    // SAFETY: the spec outlives the references, and `count_free` leaves its storage.
    let strong = unsafe { StrongRef_New((&raw mut spec).cast(), Some(count_free)) };
    // SAFETY: `strong` is the live reference created above.
    let weak = unsafe { StrongRef_Demote(strong) };
    // SAFETY: `weak` is a valid weak reference.
    let spec_ref = unsafe { IndexSpecWeakRef::from_raw(weak) }
        .promote()
        .expect("the spec is alive");

    mem::forget(spec_ref.read().expect("the spec is valid"));
    drop(spec_ref);
    // SAFETY: `strong` was created above and is released once.
    unsafe { StrongRef_Release(strong) };

    assert_eq!(
        FREES.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "the forgotten lock still holds the spec"
    );
    // SAFETY: `weak` was created above and is released once, and the spec's names
    // were allocated above and are freed once. The lock stays taken: the test
    // forgot it on purpose and its process ends here.
    unsafe {
        WeakRef_Release(weak);
        ffi::HiddenString_Free(spec.specName, true);
        drop(CString::from_raw(spec.obfuscatedName));
    }
}

/// A spec dropped after it was promoted cannot be locked, and the promoted
/// reference still releases cleanly.
#[test]
#[cfg_attr(miri, ignore = "calls the C reference manager")]
fn an_invalidated_spec_cannot_be_read_locked() {
    // SAFETY: zero is valid for every field; the spec is never locked.
    let mut spec = unsafe { mem::zeroed::<ffi::IndexSpec>() };
    // SAFETY: the spec outlives the references, and `keep_spec` leaves its storage.
    let strong = unsafe { StrongRef_New((&raw mut spec).cast(), Some(keep_spec)) };
    // SAFETY: `strong` is the live reference created above.
    let weak = unsafe { StrongRef_Demote(strong) };
    // SAFETY: `weak` is a valid weak reference.
    let spec_ref = unsafe { IndexSpecWeakRef::from_raw(weak) }
        .promote()
        .expect("the spec is alive");

    // SAFETY: `strong` is live; this is what dropping the index does.
    unsafe { StrongRef_Invalidate(strong) };
    assert!(spec_ref.read().is_none());

    drop(spec_ref);
    // SAFETY: both references were created above and are released once.
    unsafe {
        WeakRef_Release(weak);
        StrongRef_Release(strong);
    }
}
