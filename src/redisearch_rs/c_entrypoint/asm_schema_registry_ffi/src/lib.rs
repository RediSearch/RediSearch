/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Schema replay suppression for indexes dropped during an ASM import.

use asm_schema_registry::AsmSchemaRegistry;
use redis_module::{RedisModuleIO as RmIo, raw::RedisModuleIO};
use std::cell::RefCell;
use std::ffi::{CStr, c_char};
use std::sync::OnceLock;
use std::thread::ThreadId;

static OWNER_THREAD: OnceLock<ThreadId> = OnceLock::new();

thread_local! {
    static IMPORT_SCHEMAS: RefCell<AsmSchemaRegistry> = RefCell::new(AsmSchemaRegistry::default());
}

fn assert_owner_thread() {
    let current = std::thread::current().id();
    let owner = OWNER_THREAD.get_or_init(|| current);
    assert_eq!(
        *owner, current,
        "ASM schema registry called from wrong thread"
    );
}

/// Resets schema tracking for test isolation. Call on the main thread.
#[unsafe(no_mangle)]
pub extern "C" fn asm_schema_registry_reset() {
    assert_owner_thread();
    IMPORT_SCHEMAS.with_borrow_mut(|imports| *imports = AsmSchemaRegistry::default());
}

/// Starts an ASM import's schema-deletion tracking.
///
/// # Safety
/// 1. Call on the main thread.
/// 2. `task_id` must point to a [valid], NUL-terminated string for the call.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn asm_schema_registry_start_import(task_id: *const c_char) {
    assert_owner_thread();
    debug_assert_non_null(task_id);
    // SAFETY: (2) supplies the terminated string; no reference escapes this call.
    let task_id = unsafe { CStr::from_ptr(task_id) }.to_bytes();
    IMPORT_SCHEMAS.with_borrow_mut(|imports| imports.start(task_id));
}

/// Ends an ASM import's schema-deletion tracking on completion or failure.
///
/// # Safety
/// 1. Call on the main thread.
/// 2. `task_id` must point to a [valid], NUL-terminated string for the call.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn asm_schema_registry_end_import(task_id: *const c_char) {
    assert_owner_thread();
    debug_assert_non_null(task_id);
    // SAFETY: (2) supplies the terminated string; no reference escapes this call.
    let task_id = unsafe { CStr::from_ptr(task_id) }.to_bytes();
    IMPORT_SCHEMAS.with_borrow_mut(|imports| imports.end(task_id));
}

/// Prevents active imports from replaying a schema for the dropped name.
/// Returns whether a new record was inserted.
///
/// # Safety
/// 1. Call on the main thread.
/// 2. `name` must point to a [valid], NUL-terminated string for the call.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn asm_schema_registry_record_drop(name: *const c_char) -> bool {
    assert_owner_thread();
    debug_assert_non_null(name);
    // SAFETY: (2) supplies the terminated string; no reference escapes this call.
    let name = unsafe { CStr::from_ptr(name) }.to_bytes();
    IMPORT_SCHEMAS.with_borrow_mut(|imports| imports.record_drop(name))
}

/// Whether schema replay must ignore the name during an active ASM import.
///
/// # Safety
/// 1. Call on the main thread.
/// 2. `name` must point to a [valid], NUL-terminated string for the call.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn asm_schema_registry_was_dropped(name: *const c_char) -> bool {
    assert_owner_thread();
    debug_assert_non_null(name);
    // SAFETY: (2) supplies the terminated string; no reference escapes this call.
    let name = unsafe { CStr::from_ptr(name) }.to_bytes();
    IMPORT_SCHEMAS.with_borrow(|imports| imports.was_dropped(name))
}

#[track_caller]
fn debug_assert_non_null<T>(ptr: *const T) {
    debug_assert!(!ptr.is_null(), "import tracking pointer cannot be null");
}

/// Whether import deletion records require auxiliary RDB persistence.
#[unsafe(no_mangle)]
pub extern "C" fn asm_schema_registry_has_drops() -> bool {
    assert_owner_thread();
    IMPORT_SCHEMAS.with_borrow(|imports| imports.has_drops())
}

/// Clears pre-snapshot records without relying on another import-start notification.
#[unsafe(no_mangle)]
pub extern "C" fn asm_schema_registry_begin_loading() {
    assert_owner_thread();
    IMPORT_SCHEMAS.with_borrow_mut(|imports| imports.begin_loading());
}

/// Saves import deletion records, including when the index registry is empty.
///
/// # Safety
/// 1. Call on the main thread or its RDB child.
/// 2. `io` must point to a [valid] [`RedisModuleIO`] for the call.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn asm_schema_registry_save(io: *mut RedisModuleIO) {
    assert_owner_thread();
    debug_assert_non_null(io);
    let mut io = RmIo::new(io);
    IMPORT_SCHEMAS.with_borrow(|imports| imports.save(&mut io));
}

/// Loads records for the active import, returning false on malformed data or IO errors.
///
/// # Safety
/// 1. Call on the main thread.
/// 2. `io` must point to a [valid] [`RedisModuleIO`] for the call.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn asm_schema_registry_load(io: *mut RedisModuleIO) -> bool {
    assert_owner_thread();
    debug_assert_non_null(io);
    let mut io = RmIo::new(io);
    IMPORT_SCHEMAS.with_borrow_mut(|imports| imports.load(&mut io).is_ok())
}
