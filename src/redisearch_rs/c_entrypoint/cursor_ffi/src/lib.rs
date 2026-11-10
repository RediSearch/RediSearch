/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Cursor requests execute outside the list mutex. Only parked requests may be
//! inspected under that mutex; deletion of an executing request is deferred.

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

use cursor::{Collection, Kind, Registry};
use ffi::IndexSpec;
use query_error::{QueryError, QueryErrorCode};
use redis_module::raw;
use std::{
    ffi::{CStr, c_int, c_void},
    ptr,
    sync::{Mutex, MutexGuard, OnceLock},
};

/// Stable C facade. The execution owner initializes the request and timeout
/// snapshot before the first [`Cursor_Pause`]; idle cursors must not be accessed.
/// Callers must not modify [`Self::id`] or [`Self::spec_ref`] or release the owned request.
#[cheadergen::config(export)]
#[repr(C)]
pub struct Cursor {
    /// Owned weak reference, which never keeps a dropped index alive.
    pub spec_ref: ffi::WeakRef,
    /// Owned request, released after its blocked-client cycle has ended.
    pub query: *mut ffi::QueryRequest,
    /// Nonzero ID whose parity selects its registry.
    pub id: u64,
    /// Original command timeout, retained across subsequent reads.
    #[cheadergen(rename = "queryTimeoutMS")]
    pub query_timeout_ms: usize,
    /// Original timeout policy, unaffected by configuration changes.
    #[cheadergen(rename = "queryTimeoutPolicy")]
    pub query_timeout_policy: ffi::RSTimeoutPolicy,
}

struct OwnedCursor(*mut Cursor, Kind);

// SAFETY: the registry serializes pointer transfer and destruction. An execution
// owner alone accesses the facade until it publishes it through Cursor_Pause.
unsafe impl Send for OwnedCursor {}

impl Drop for OwnedCursor {
    fn drop(&mut self) {
        // SAFETY: the registry destroys only idle or owner-returned cursors, under
        // the owning list mutex. This is the allocation's sole owning handle.
        let cur = unsafe { Box::from_raw(self.0) };
        // SAFETY: the cursor owns its optional request, with no execution borrower.
        if let Some(query) = unsafe { cur.query.as_ref() } {
            assert!(!query.blockedClientCycleActive);
            // SAFETY: the request has no active cycle and this is its sole owner.
            unsafe { ffi::QueryRequest_Free(cur.query) };
        }
        if !cur.spec_ref.rm.is_null() {
            // SAFETY: the cursor owns this weak reference. Generic promotion
            // deliberately leaves the executing request's thread-local spec alone.
            let strong = unsafe { ffi::WeakRef_Promote(cur.spec_ref) };
            // SAFETY: promotion returned a strong reference or the supported null handle.
            let spec = unsafe { ffi::StrongRef_Get(strong) }.cast::<ffi::IndexSpec>();
            if !spec.is_null() {
                let count = assert_non_null_counter(spec, self.1);
                // SAFETY: the live counter field is protected by the held list lock.
                *unsafe { count.as_mut() }.expect("live index counter") -= 1;
                // SAFETY: successful promotion acquired this strong reference.
                unsafe { ffi::StrongRef_Release(strong) };
            }
            // SAFETY: the cursor's weak reference has not been released elsewhere.
            unsafe { ffi::WeakRef_Release(cur.spec_ref) };
        }
    }
}

struct List {
    registry: Registry<OwnedCursor>,
    timer: u64,
    kind: Kind,
}

/// Process-lifetime list handle. Emptying it never invalidates queued one-shots.
pub struct CursorList(OnceLock<Mutex<List>>);

/// User cursor list; initialized during module startup.
#[unsafe(no_mangle)]
pub static g_CursorsList: CursorList = CursorList(OnceLock::new());
/// Coordinator cursor list; initialized during module startup.
#[unsafe(no_mangle)]
pub static g_CursorsListCoord: CursorList = CursorList(OnceLock::new());

static RANDOM: Mutex<()> = Mutex::new(());

fn lock(cl: &CursorList) -> MutexGuard<'_, List> {
    cl.0.get()
        .expect("cursor list initialized")
        .lock()
        .expect("unpoisoned cursor mutex")
}

fn global(id: u64) -> &'static CursorList {
    match id % 2 {
        0 => &g_CursorsList,
        _ => &g_CursorsListCoord,
    }
}

// Field projection must not borrow the entire spec: the other list owns its other counter.
#[track_caller]
const fn assert_non_null_counter(spec: *const ffi::IndexSpec, kind: Kind) -> *mut usize {
    debug_assert!(!spec.is_null());
    let offset = match kind {
        Kind::Coordinator => std::mem::offset_of!(ffi::IndexSpec, activeCoordCursors),
        Kind::User => std::mem::offset_of!(ffi::IndexSpec, activeCursors),
    };
    spec.wrapping_byte_add(offset).cast_mut().cast()
}

fn now() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: ts is writable and the monotonic clock is supported on all targets.
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &raw mut ts) };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

/// Smallest whole-millisecond timer delay covering the remaining nanoseconds.
pub const fn timer_delay_ms(remaining_ns: u64) -> u64 {
    remaining_ns.div_ceil(1_000_000)
}

/// Rearms a list's timer on Redis's event-loop thread.
/// # Safety
/// 1. `cl` must be an initialized global and `list` its locked contents.
/// 2. Real timer APIs require module initialization and the main thread/GIL.
unsafe fn reschedule(cl: &'static CursorList, list: &mut List) {
    // SAFETY: 2. Redis API pointers are fixed before any cursor operation.
    let Some(create) = (unsafe { raw::RedisModule_CreateTimer }) else {
        return;
    };
    // SAFETY: 2. The TLS accessor is callable from any initialized module thread.
    assert!(!unsafe { ffi::MainThread_GetBlockedQueries() }.is_null());
    // SAFETY: 2. Reaching real timer APIs implies module initialization has completed.
    let ctx = unsafe { ffi::context::redisearch_module_context() };
    if list.timer != 0 {
        // SAFETY: 2. The API table is initialized and immutable during cursor operations.
        let stop = unsafe { raw::RedisModule_StopTimer }.expect("timer API initialized");
        // SAFETY: 1, 2. list owns this armed timer; we hold its lock on the main thread.
        unsafe { stop(ctx, list.timer, ptr::null_mut()) };
        list.timer = 0;
    }
    if let Some(deadline) = list.registry.next_deadline() {
        let period = timer_delay_ms(deadline.saturating_sub(now()));
        let data = ptr::from_ref(cl).cast_mut().cast();
        // SAFETY: 1, 2. cl is process-lifetime; callback and timer operations run on the main thread.
        list.timer = unsafe { create(ctx, period as i64, Some(sweep), data) };
    }
}

/// Collects an expired timer's list.
/// # Safety
/// 1. Redis must return the initialized global supplied by [`reschedule`] on its main thread.
unsafe extern "C" fn sweep(_: *mut raw::RedisModuleCtx, data: *mut c_void) {
    // SAFETY: 1. Redis returns the initialized global supplied at timer creation.
    let cl = unsafe { data.cast::<CursorList>().as_ref() }.expect("timer's global list");
    let mut list = lock(cl);
    list.timer = 0;
    list.registry.collect(now(), Collection::Forced);
    // SAFETY: 1. The global's list is locked. 2. Redis invokes timers on its main thread.
    unsafe { reschedule(cl, &mut list) };
}

/// Moves worker rescheduling onto Redis's event loop.
/// # Safety
/// 1. Redis must return the initialized global supplied by [`Cursor_Pause`] on its main thread.
unsafe extern "C" fn reschedule_once(data: *mut c_void) {
    // SAFETY: 1. One-shots retain the initialized global even across Empty.
    let cl = unsafe { data.cast::<CursorList>().as_ref() }.expect("one-shot's global list");
    // SAFETY: 1. The global's list is locked. 2. Redis invokes one-shots on its main thread.
    unsafe { reschedule(cl, &mut lock(cl)) };
}

/// Initializes a process-lifetime list before requests can use it.
///
/// # Safety
/// 1. `cl` must be an uninitialized global with no concurrent access; `coord` must match it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn CursorList_Init(cl: &CursorList, coord: bool) {
    let kind = if coord { Kind::Coordinator } else { Kind::User };
    let list = List {
        registry: Registry::new(kind),
        timer: 0,
        kind,
    };
    assert!(cl.0.set(Mutex::new(list)).is_ok());
    let _rng = RANDOM.lock().expect("random mutex is not poisoned");
    // SAFETY: the random-source mutex excludes concurrent cursor draws.
    unsafe { libc::srand48(std::process::id().into()) };
}

/// Reserves an executing cursor, borrowing a strong index reference if present.
///
/// # Safety
/// 1. `cl` must be an initialized global and module APIs/configuration initialized.
/// 2. `spec_ref` must be null or a live strong reference to an index throughout this call.
/// 3. On capacity failure, `status` must point to an exclusively writable initialized error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cursors_Reserve(
    cl: &CursorList,
    spec_ref: ffi::StrongRef,
    interval: u32,
    status: *mut QueryError,
) -> *mut Cursor {
    let mut list = lock(cl);
    list.registry.tick(now);
    // SAFETY: 2. The caller borrows a strong reference or the supported null handle.
    let spec = unsafe { ffi::StrongRef_Get(spec_ref) }.cast::<ffi::IndexSpec>();
    let limit = global_config::index_cursor_limit();
    let weak = if spec.is_null() {
        ffi::WeakRef {
            rm: ptr::null_mut(),
        }
    } else {
        let count = assert_non_null_counter(spec, list.kind);
        // SAFETY: 2. count belongs to this locked list and the live spec.
        if unsafe { count.read() } >= limit as usize {
            list.registry.collect(now(), Collection::Throttled);
            // SAFETY: 2. Reclamation may update count, but spec_ref still keeps it alive.
            if unsafe { count.read() } >= limit as usize {
                let message =
                    format!("INDEX_CURSOR_LIMIT of {limit} has been reached for an index");
                // SAFETY: 3. Caller supplies an exclusively writable error slot on failure.
                let status = unsafe { status.as_mut() }.expect("capacity error output");
                status.set_error(QueryErrorCode::Limit, &message);
                return ptr::null_mut();
            }
        }
        // SAFETY: 2. The spec remains live; this list lock exclusively protects count.
        *unsafe { count.as_mut() }.expect("live index counter") += 1;
        // SAFETY: 2. Caller retains the strong reference while we acquire a weak one.
        unsafe { ffi::StrongRef_Demote(spec_ref) }
    };
    let cur = Box::into_raw(Box::new(Cursor {
        spec_ref: weak,
        query: ptr::null_mut(),
        id: 0,
        query_timeout_ms: 0,
        query_timeout_policy: ffi::RSTimeoutPolicy_TimeoutPolicy_Return,
    }));
    let _rng = RANDOM.lock().expect("random mutex is not poisoned");
    let owned = OwnedCursor(cur, list.kind);
    let id = list.registry.reserve(owned, interval, || {
        // SAFETY: the random-source mutex serializes user and coordinator draws.
        unsafe { libc::lrand48() as u32 }
    });
    // SAFETY: the new allocation has not escaped to an execution owner yet.
    unsafe { cur.as_mut() }.expect("new cursor allocation").id = id;
    cur
}

/// Takes an idle cursor for execution, or returns null if unavailable.
///
/// # Safety
/// 1. `cl` must be an initialized global. Returned cursors obey [`Cursor`]'s ownership rules.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cursors_TakeForExecution(cl: &CursorList, id: u64) -> *mut Cursor {
    let mut list = lock(cl);
    list.registry.tick(now);
    list.registry.take(id).map_or(ptr::null_mut(), |cur| cur.0)
}

/// Publishes a request after execution, or destroys it if deletion was requested.
///
/// # Safety
/// 1. `cur` must be exclusively owned for execution under [`Cursor`]'s rules, with no active
///    blocked-client cycle. The caller relinquishes all access to it and its request.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cursor_Pause(cur: *mut Cursor) -> c_int {
    // SAFETY: 1. cur is owned for execution; its ID is immutable after reservation.
    let id = unsafe { cur.as_ref() }.expect("executing cursor").id;
    let cl = global(id);
    let mut list = lock(cl);
    list.registry.tick(now);
    // SAFETY: Redis API pointers are fixed before requests start.
    if list.registry.pause(id, now()) && unsafe { raw::RedisModule_CreateTimer }.is_some() {
        // SAFETY: the API table is initialized and immutable during cursor operations.
        let post =
            unsafe { raw::RedisModule_EventLoopAddOneShot }.expect("event loop API initialized");
        // SAFETY: posting is thread-safe and the global outlives pending callbacks.
        unsafe { post(Some(reschedule_once), ptr::from_ref(cl).cast_mut().cast()) };
    }
    0
}

/// Releases an executing cursor and its request.
///
/// # Safety
/// 1. `cur` must satisfy [`Cursor_Pause`]'s ownership precondition; this consumes it permanently.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cursor_Free(cur: *mut Cursor) -> c_int {
    // SAFETY: 1. The caller owns the live cursor, whose ID is immutable.
    let id = unsafe { cur.as_ref() }.expect("executing cursor").id;
    let mut list = lock(global(id));
    list.registry.tick(now);
    list.registry.remove(id);
    0
}

/// Deletes an idle cursor or defers deletion until its execution owner returns it.
/// Returns the Redis error status only when the ID is unknown.
///
/// # Safety
/// 1. `cl` must be an initialized global.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cursors_Purge(cl: &CursorList, id: u64) -> c_int {
    let mut list = lock(cl);
    list.registry.tick(now);
    c_int::from(!list.registry.purge(id))
}

/// Collects expired cursors and rearms the idle timer, returning -1 for a skipped sweep.
///
/// # Safety
/// 1. `cl` must be an initialized global; real timers require the Redis main thread/GIL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cursors_CollectIdle(cl: &'static CursorList) -> c_int {
    let mut list = lock(cl);
    let count = list.registry.collect(now(), Collection::Forced);
    // SAFETY: 1. The initialized global is locked. 2. The caller holds the main thread/GIL.
    unsafe { reschedule(cl, &mut list) };
    count.map_or(-1, |n| n as c_int)
}

/// Frees parked requests, marks executing requests for deletion and cancels the timer.
///
/// # Safety
/// 1. `cl` must be an initialized global. Armed timers require the Redis main thread/GIL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn CursorList_Empty(cl: &'static CursorList) {
    let mut list = lock(cl);
    list.registry.clear();
    if list.timer != 0 {
        // SAFETY: 1. The global is locked. 2. An armed timer requires the caller's main thread/GIL.
        unsafe { reschedule(cl, &mut list) };
    }
}

/// Marks only parked aggregate requests in both lists as potentially inaccurate.
///
/// # Safety
/// 1. Both globals must be initialized; parked requests must be aggregate requests.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn CursorList_MarkASMInaccuracy() {
    for cl in [global(1), global(0)] {
        for cur in lock(cl).registry.idle_values() {
            // SAFETY: the list lock excludes execution owners from idle requests.
            let request = unsafe { cur.0.as_ref() }.expect("parked cursor").query;
            // SAFETY: 1. Parked requests are aggregates, exclusively accessed under this lock.
            if let Some(req) = unsafe { request.cast::<ffi::AREQ>().as_mut() } {
                assert_eq!(req.base.kind, ffi::QueryRequestKind_QUERY_REQUEST_KIND_AREQ);
                req.stateflags |= ffi::QEStateFlags_QEXEC_S_ASM_TRIMMING_DELAY_TIMEOUT;
            }
        }
    }
}

/// Global user/coordinator counts, including executing cursors in totals.
#[repr(C)]
pub struct CursorsInfoStats {
    pub total_user: usize,
    pub total_idle_user: usize,
    pub total_internal: usize,
    pub total_idle_internal: usize,
}

/// Takes a consistent snapshot, locking the user list before the coordinator list.
///
/// # Safety
/// 1. Both globals must be initialized. The caller must not hold either list mutex.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cursors_GetInfoStats() -> CursorsInfoStats {
    let user = lock(global(0));
    let coord = lock(global(1));
    CursorsInfoStats {
        total_user: user.registry.len(),
        total_idle_user: user.registry.idle_len(),
        total_internal: coord.registry.len(),
        total_idle_internal: coord.registry.idle_len(),
    }
}

/// Reads both lists and their corresponding index counters.
/// # Safety
/// 1. Both list mutexes must be held in user/coordinator order; `spec` must point to a live index.
unsafe fn stats(user: &List, coord: &List, spec: *const IndexSpec) -> [(&'static CStr, i64); 5] {
    let idle = (user.registry.idle_len() + coord.registry.idle_len()) as i64;
    let total = (user.registry.len() + coord.registry.len()) as i64;
    // SAFETY: 1. Caller holds the user list mutex and keeps spec alive.
    let index_total = unsafe { assert_non_null_counter(spec, Kind::User).read() } as i64;
    // SAFETY: 1. Caller holds the coordinator list mutex and keeps spec alive.
    let index_internal = unsafe { assert_non_null_counter(spec, Kind::Coordinator).read() } as i64;
    [
        (c"global_idle", idle),
        (c"global_total", total),
        (c"index_capacity", global_config::index_cursor_limit()),
        (c"index_total", index_total),
        (c"index_total_internal", index_internal),
    ]
}

/// Emits cursor statistics through the C reply builder, preserving its frame accounting.
///
/// # Safety
/// 1. Lists must be initialized globals in user/coordinator order, neither already locked.
/// 2. `spec` must point to a live index and `reply` to an exclusively borrowed builder with an open map.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cursors_RenderStats(
    cl: &CursorList,
    coord: &CursorList,
    spec: *const ffi::IndexSpec,
    reply: *mut ffi::RedisModule_Reply,
) {
    let user = lock(cl);
    let coord = lock(coord);
    // SAFETY: 2. Caller owns the reply builder with an open enclosing map.
    unsafe { ffi::RedisModule_ReplyKV_Map(reply, c"cursor_stats".as_ptr()) };
    // SAFETY: 1, 2. Both list locks protect the live spec's counters, in user/coordinator order.
    for (key, value) in unsafe { stats(&user, &coord, spec) } {
        // SAFETY: 2. The nested map is open and key is a terminated static string.
        unsafe { ffi::RedisModule_ReplyKV_LongLong(reply, key.as_ptr(), value) };
    }
    // SAFETY: 2. This closes the map opened above on the exclusively borrowed builder.
    unsafe { ffi::RedisModule_Reply_MapEnd(reply) };
}

/// Emits crash-report statistics without waiting for either cursor list mutex.
///
/// # Safety
/// 1. Lists must be initialized globals in user/coordinator order.
/// 2. `spec` must point to a live index if both locks can be acquired.
/// 3. The INFO APIs must be initialized and `ctx` must satisfy their context requirements.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cursors_RenderStatsForInfo(
    cl: &CursorList,
    coord: &CursorList,
    spec: *const ffi::IndexSpec,
    ctx: *mut raw::RedisModuleInfoCtx,
) {
    let user = cl.0.get().expect("cursor list initialized").try_lock();
    let coord = coord.0.get().expect("cursor list initialized").try_lock();
    raw::add_info_begin_dict_field(ctx, "cursor_stats");
    match (user, coord) {
        (Ok(user), Ok(coord)) => {
            // SAFETY: 1, 2. Both list guards protect the live spec's counters, in user/coordinator order.
            for (key, value) in unsafe { stats(&user, &coord, spec) } {
                raw::add_info_field_long_long(ctx, key.to_str().expect("ASCII field name"), value);
            }
        }
        _ => {
            // SAFETY: 3. The INFO API table is initialized and not concurrently modified.
            let add =
                unsafe { raw::RedisModule_InfoAddFieldCString }.expect("INFO API initialized");
            // SAFETY: 3. ctx's dictionary is open and both strings are terminated.
            unsafe { add(ctx, c"status".as_ptr(), c"locked".as_ptr()) };
        }
    }
    raw::add_info_end_dict_field(ctx);
}
