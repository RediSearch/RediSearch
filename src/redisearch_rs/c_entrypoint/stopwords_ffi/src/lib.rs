/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Shared stopword lists and their C argument, persistence and rendering boundaries.

#![warn(
    clippy::unwrap_used,
    clippy::dbg_macro,
    clippy::todo,
    clippy::unimplemented,
    clippy::print_stdout,
    clippy::panic,
    clippy::unnecessary_safety_comment,
    clippy::ptr_as_ptr,
    clippy::borrow_as_ptr,
    clippy::ref_as_ptr,
    clippy::indexing_slicing
)]

use rdb_io::RdbIO;
use redis_module::logging::{RedisLogLevel, log_io_error};
use redis_module::{RedisModule_InfoAddFieldCString, RedisModuleIO, raw};
use std::{
    ffi::{c_char, c_int, c_void},
    io, ptr, slice,
    sync::{Arc, Mutex},
};
use stopwords::MAX_STOPWORDLIST_SIZE;
use value::SharedValue;

/// An immutable list, opaque to C.
pub struct StopWordList(pub stopwords::StopWordList, pub Ownership);

/// Whether references belong to callers or to a reclaimable global.
pub enum Ownership {
    /// Borrowed until [`StopWordList_FreeGlobals`].
    Shared,
    /// Released by [`StopWordList_Unref`].
    Counted,
}

static DEFAULT: Mutex<Option<Arc<StopWordList>>> = Mutex::new(None);
static EMPTY: Mutex<Option<Arc<StopWordList>>> = Mutex::new(None);

fn shared(
    slot: &Mutex<Option<Arc<StopWordList>>>,
    words: impl FnOnce() -> stopwords::StopWordList,
) -> *mut StopWordList {
    let mut slot = slot.lock().expect("stopword globals poisoned");
    let list = slot.get_or_insert_with(|| Arc::new(StopWordList(words(), Ownership::Shared)));
    Arc::as_ptr(list).cast_mut()
}

#[track_caller]
fn debug_assert_non_null<T>(pointer: *const T) -> *const T {
    debug_assert!(!pointer.is_null());
    pointer
}

fn owned(list: stopwords::StopWordList) -> *mut StopWordList {
    Arc::into_raw(Arc::new(StopWordList(list, Ownership::Counted))).cast_mut()
}

/// Borrow the shared English list until [`StopWordList_FreeGlobals`].
#[unsafe(no_mangle)]
pub extern "C" fn DefaultStopWordList() -> *mut StopWordList {
    shared(&DEFAULT, stopwords::StopWordList::default)
}

/// Release shared lists. Later constructors may initialize them again.
///
/// # Safety
/// 1. No shared list pointers may be in use, including concurrently with this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn StopWordList_FreeGlobals() {
    for slot in [&DEFAULT, &EMPTY] {
        slot.lock().expect("stopword globals poisoned").take();
    }
}

/// Consume at most [`MAX_STOPWORDLIST_SIZE`] remaining cursor arguments.
/// An empty cursor returns the shared empty list; otherwise the caller owns one reference.
///
/// # Safety
/// 1. `cursor` must be aligned, [valid] and exclusively accessible, initialized as a C
///    string, Redis string or SDS cursor with readable arguments for the call.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewStopWordListAC(cursor: *mut ffi::ArgsCursor) -> *mut StopWordList {
    // SAFETY: 1. The caller supplies an initialized, exclusive cursor.
    let ac = unsafe { debug_assert_non_null(cursor).cast_mut().as_mut() }.expect("null cursor");
    let count = ac.argc - ac.offset;
    if count == 0 {
        return shared(&EMPTY, || stopwords::StopWordList::new([]));
    }
    let words = (0..count).map(|_| {
        let mut len = 0;
        // SAFETY: 1. count is bounded by the remaining readable arguments.
        let word = unsafe { ffi::AC_GetStringNC(ac, &raw mut len) };
        let word = debug_assert_non_null(word);
        // SAFETY: 1. The argument buffer remains readable for this construction.
        unsafe { slice::from_raw_parts(word.cast(), len) }
    });
    owned(stopwords::StopWordList::new(words))
}

/// Case-insensitive membership; a null list or term never matches.
///
/// # Safety
/// 1. A non-null `list` must point to a [valid], immutably borrowed [`StopWordList`].
/// 2. A non-null `term` must be [valid] for reading `len` initialized bytes.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn StopWordList_Contains(
    list: *const StopWordList,
    term: *const c_char,
    len: usize,
) -> c_int {
    if list.is_null() || term.is_null() {
        return 0;
    }
    // SAFETY: 1. The null case was handled above.
    let list = unsafe { list.as_ref().expect("list is non-null") };
    // SAFETY: 2. The caller provides initialized bytes for the explicit length.
    let term = unsafe { slice::from_raw_parts(term.cast(), len) };
    list.0.contains(term).into()
}

/// Retain a custom list; shared globals remain borrowed.
///
/// # Safety
/// 1. `list` must be a [valid] pointer obtained from a list constructor, with a live reference.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn StopWordList_Ref(list: *mut StopWordList) {
    let list = debug_assert_non_null(list);
    // SAFETY: 1. The caller keeps the list alive throughout the ownership check.
    if let Ownership::Counted = unsafe { list.as_ref() }.expect("list is non-null").1 {
        // SAFETY: 1. Custom constructors return Arc::into_raw pointers with a live count.
        unsafe { Arc::increment_strong_count(list) };
    }
}

/// Release one custom reference; shared globals remain borrowed.
///
/// # Safety
/// 1. `list` must be a [valid] constructor pointer; this consumes one custom reference.
///    The pointer must not be used after its last custom reference is released.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn StopWordList_Unref(list: *mut StopWordList) {
    let list = debug_assert_non_null(list);
    // SAFETY: 1. The caller transfers a live reference.
    if let Ownership::Counted = unsafe { list.as_ref() }.expect("list is non-null").1 {
        // SAFETY: 1. The caller transfers one live Arc reference.
        unsafe { Arc::decrement_strong_count(list) };
    }
}

/// Save the count and exact stored byte keys through [`RdbIO`].
pub fn save(list: &stopwords::StopWordList, io: &mut impl RdbIO) {
    io.write_u64(list.len() as u64);
    for word in list.iter() {
        io.write_buffer(&word);
    }
}

/// A rejected count or a failure reading stored keys.
#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    /// The persistence stream could not be read.
    #[error(transparent)]
    Io(#[from] io::Error),
    /// The element count exceeds [`MAX_STOPWORDLIST_SIZE`].
    #[error("RDB Load: Stopword list size ({0}) exceeds maximum allowed ({MAX_STOPWORDLIST_SIZE})")]
    TooManyWords(u64),
}

/// Load stored keys without normalizing them.
///
/// # Errors
/// Rejects oversized counts and incomplete input, releasing all buffers read so far.
pub fn load(io: &mut impl RdbIO) -> Result<stopwords::StopWordList, LoadError> {
    let count = io.read_u64()?;
    if count > MAX_STOPWORDLIST_SIZE as u64 {
        return Err(LoadError::TooManyWords(count));
    }
    let words = (0..count)
        .map(|_| io.read_buffer())
        .collect::<io::Result<Vec<_>>>()?;
    Ok(stopwords::StopWordList::from_stored(words))
}

/// Restore a list owned by the caller, or return null on an IO or count error.
///
/// # Safety
/// 1. `rdb` must be a [valid], exclusively accessible Redis RDB reader.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn StopWordList_RdbLoad(
    rdb: *mut raw::RedisModuleIO,
    _encver: c_int,
) -> *mut StopWordList {
    let rdb = debug_assert_non_null(rdb).cast_mut();
    match load(&mut RedisModuleIO::new(rdb)) {
        Ok(list) => owned(list),
        Err(error @ LoadError::TooManyWords(_)) => {
            log_io_error(rdb, RedisLogLevel::Warning, &error.to_string());
            ptr::null_mut()
        }
        Err(LoadError::Io(_)) => ptr::null_mut(),
    }
}

/// Persist a list through Redis RDB IO.
///
/// # Safety
/// 1. `rdb` must be a [valid], exclusively accessible Redis RDB writer.
/// 2. `list` must point to a [valid], immutably borrowed [`StopWordList`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn StopWordList_RdbSave(
    rdb: *mut raw::RedisModuleIO,
    list: *mut StopWordList,
) {
    let rdb = debug_assert_non_null(rdb).cast_mut();
    // SAFETY: 2. The caller keeps the list alive and immutable.
    let list = unsafe { debug_assert_non_null(list).as_ref() }.expect("list is non-null");
    save(&list.0, &mut RedisModuleIO::new(rdb));
}

/// Render the INFO MODULES field with a terminating NUL, retaining stored byte order.
pub fn info_string(list: &StopWordList) -> Vec<u8> {
    let mut result = Vec::new();
    for word in list.0.iter() {
        if !result.is_empty() {
            result.push(b',');
        }
        result.push(b'"');
        result.extend(word);
        result.push(b'"');
    }
    result.push(0);
    result
}

/// Add the stopword field; a null list adds nothing.
///
/// # Safety
/// 1. `ctx` must be a [valid], exclusively accessible Redis INFO context.
/// 2. A non-null `list` must point to a [valid], immutably borrowed [`StopWordList`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn AddStopWordsListToInfo(
    ctx: *mut raw::RedisModuleInfoCtx,
    list: *mut StopWordList,
) {
    if list.is_null() {
        return;
    }
    let ctx = debug_assert_non_null(ctx).cast_mut();
    // SAFETY: 2. The null case was handled above.
    let words = info_string(unsafe { list.as_ref().expect("list is non-null") });
    // SAFETY: Redis initializes its API table before any module commands run.
    let add_field = unsafe { RedisModule_InfoAddFieldCString }.expect("INFO API unset");
    // SAFETY: 1. Both strings are NUL-terminated and live throughout the call.
    unsafe { add_field(ctx, c"stop_words".as_ptr(), words.as_ptr().cast()) };
}

/// Reply with the field name and an array of words.
///
/// # Safety
/// 1. `reply` must be a [valid], exclusively accessible C reply builder.
/// 2. `list` must point to a [valid], immutably borrowed [`StopWordList`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ReplyWithStopWordsList(
    reply: *mut ffi::RedisModule_Reply,
    list: *mut StopWordList,
) {
    let reply = debug_assert_non_null(reply).cast_mut();
    // SAFETY: 2. The caller keeps the list alive and immutable.
    let list = unsafe { debug_assert_non_null(list).as_ref() }.expect("list is non-null");
    // SAFETY: 1. The variadic formatter receives a matching, live C string.
    unsafe {
        ffi::RedisModule_Reply_SimpleStringf(reply, c"%s".as_ptr(), c"stopwords_list".as_ptr())
    };
    // SAFETY: 1. Opening through the C builder preserves parent accounting and assert tracking.
    unsafe { ffi::RedisModule_Reply_Array(reply) };
    for word in list.0.iter() {
        let value = SharedValue::new_string(word);
        // SAFETY: 1. SharedValue's pointer borrows its live RSValue allocation.
        unsafe { ffi::RedisModule_Reply_RSValue(reply, value.as_ptr().cast(), 0) };
    }
    // SAFETY: 1. Closes the collection opened above on the same builder.
    unsafe { ffi::RedisModule_Reply_ArrayEnd(reply) };
}

/// Hash the unique sorted stored keys with the shared SHA1 framing.
///
/// # Safety
/// 1. `list` must point to a [valid], immutably borrowed [`StopWordList`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn StopWordList_Fingerprint(list: *const StopWordList) -> u64 {
    /// # Safety
    /// 1. `hash` must be a live SHA1 context; `value` must borrow a live stopword list.
    unsafe extern "C" fn visit(hash: *mut ffi::Sha1Context, value: *const c_void) {
        let hash = debug_assert_non_null(hash).cast_mut();
        let value = debug_assert_non_null(value);
        // SAFETY: 1. ComputeValue passes back the list pointer unchanged.
        let list = unsafe { value.cast::<StopWordList>().as_ref() }.expect("list is non-null");
        // SAFETY: 1. ComputeValue supplies a live context for the callback.
        unsafe { ffi::Sha1_UpdateU64(hash, list.0.len() as u64) };
        for word in list.0.iter() {
            // SAFETY: 1. The owned key stays readable throughout this update.
            unsafe { ffi::Sha1_UpdateBuffer(hash, word.as_ptr().cast(), word.len()) };
        }
    }
    // SAFETY: 1. visit borrows list only during this synchronous callback.
    unsafe { ffi::Sha1_ComputeValue(Some(visit), debug_assert_non_null(list).cast()) }
}
