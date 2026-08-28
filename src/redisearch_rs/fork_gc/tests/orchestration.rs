/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// // These tests go through a real pipe that miri does not support.
// #![cfg(not(miri))]

use dict::{KeysDictType, MissingFieldDictType, OwnedDict};
use fork_gc::{
    ForkGC, HandleError,
    orchestration::{collect_scanners, handle_scanners},
};
use index_spec::IndexSpecReadGuard;
use std::{
    ffi::CString,
    io::{self, Read, Write},
    mem,
    os::fd::AsRawFd,
    sync::Once,
};

// Link both Rust-provided and C-provided symbols.
extern crate redisearch_rs;
// Provide Redis allocator shims so the C dict functions can allocate memory.
redis_mock::mock_or_stub_missing_redis_c_symbols!();

unsafe extern "C" {
    static mut RedisModule_SendChildHeartbeat: Option<unsafe extern "C" fn(f64)>;
}

static HEARTBEAT_INSTALLED: Once = Once::new();

unsafe extern "C" fn send_child_heartbeat(_: f64) {}

fn install_heartbeat_stub() {
    HEARTBEAT_INSTALLED.call_once(|| {
        // SAFETY: this test binary does not run the Redis module loader, so it
        // installs its no-op replacement before `collect_scanners` reads it.
        unsafe { RedisModule_SendChildHeartbeat = Some(send_child_heartbeat) };
    });
}

/// Construct an owned [`ForkGC`] backed by a real pipe pair.
fn make_fork_gc() -> (Box<ForkGC>, io::PipeReader, io::PipeWriter) {
    let (pipe_reader, pipe_writer) = io::pipe().unwrap();
    // SAFETY: all pointer fields in ffi::ForkGC are zeroed (null), which is
    // a valid bit pattern. Only pipe_read_fd / pipe_write_fd are used here.
    let mut raw = Box::new(unsafe { mem::zeroed::<ffi::ForkGC>() });
    raw.pipe_read_fd = pipe_reader.as_raw_fd();
    raw.pipe_write_fd = pipe_writer.as_raw_fd();
    // SAFETY: `ForkGC` is #[repr(transparent)] over ffi::ForkGC, so the
    // allocation's layout and drop behavior are unchanged by this conversion.
    let fgc = unsafe { Box::from_raw(Box::into_raw(raw).cast::<ForkGC>()) };

    (fgc, pipe_reader, pipe_writer)
}

/// Encode the empty stream produced by each scanner in protocol order.
fn empty_scanner_streams() -> Vec<u8> {
    let terminator = usize::MAX.to_ne_bytes();
    [
        terminator.as_slice(),
        terminator.as_slice(),
        &[0xc0], // `collect_tags` serializes Option::<TagEntry>::None as MessagePack nil.
        terminator.as_slice(),
        terminator.as_slice(),
    ]
    .concat()
}

/// Write the empty stream produced by each scanner.
fn write_empty_scanner_streams(fgc: &mut ForkGC) {
    fgc.writer().write_all(&empty_scanner_streams()).unwrap();
}

/// Minimal initialized index spec for an empty child-side scan.
struct EmptySpec {
    spec: ffi::IndexSpec,
    _keys_dict: OwnedDict<KeysDictType>,
    _missing_field_dict: OwnedDict<MissingFieldDictType>,
}

impl EmptySpec {
    fn new() -> Self {
        let keys_dict = OwnedDict::create();
        let missing_field_dict = OwnedDict::create();
        // SAFETY: the initialized fields below are the complete subset used by
        // an empty child-side scan; no scanner accesses the remaining fields.
        let mut spec = unsafe { mem::zeroed::<ffi::IndexSpec>() };
        // SAFETY: NewHiddenString copies the static name because ownership is requested.
        spec.specName =
            unsafe { ffi::NewHiddenString(c"idx".as_ptr(), c"idx".count_bytes(), true) };
        spec.obfuscatedName = CString::new("Index@test").unwrap().into_raw();
        // SAFETY: a fresh trie with no payload is valid for the empty terms scan.
        spec.terms = unsafe { ffi::NewTrie(None, ffi::TrieSortMode_Trie_Sort_Lex) };
        spec.keysDict = keys_dict.as_mut_ptr();
        spec.missingFieldDict = missing_field_dict.as_mut_ptr();

        Self {
            spec,
            _keys_dict: keys_dict,
            _missing_field_dict: missing_field_dict,
        }
    }

    fn read_guard(&self) -> mem::ManuallyDrop<IndexSpecReadGuard<'_>> {
        // SAFETY: this fixture owns the spec and does not mutate it while the
        // returned guard is live.
        unsafe { IndexSpecReadGuard::from_locked(&self.spec) }
    }
}

impl Drop for EmptySpec {
    fn drop(&mut self) {
        // SAFETY: these pointers are owned by the fixture and remain live until
        // this destructor runs.
        unsafe {
            ffi::TrieType_Free(self.spec.terms.cast());
            ffi::HiddenString_Free(self.spec.specName, true);
            drop(CString::from_raw(self.spec.obfuscatedName));
        }
    }
}

#[test]
fn child_writes_empty_scanner_streams_in_protocol_order() {
    install_heartbeat_stub();
    let (mut fgc, mut pipe_reader, _pipe_writer) = make_fork_gc();
    let spec = EmptySpec::new();
    let guard = spec.read_guard();

    collect_scanners(&mut fgc, &guard);

    let expected = empty_scanner_streams();
    let mut received = vec![0; expected.len() + 1];
    let written = pipe_reader.read(&mut received).unwrap();
    assert_eq!(written, expected.len());
    assert_eq!(&received[..written], expected);
}

#[test]
fn parent_consumes_scanner_terminators_then_eof() {
    let (mut fgc, _pipe_reader, pipe_writer) = make_fork_gc();

    write_empty_scanner_streams(&mut fgc);
    drop(pipe_writer);

    assert!(handle_scanners(&mut fgc).is_ok());
}

#[test]
fn parent_rejects_trailing_data_after_all_scanners() {
    let (mut fgc, _pipe_reader, pipe_writer) = make_fork_gc();

    write_empty_scanner_streams(&mut fgc);
    fgc.writer().write_all(b"unexpected trailing data").unwrap();
    drop(pipe_writer);

    assert!(matches!(
        handle_scanners(&mut fgc),
        Err((_, HandleError::Codec { .. }))
    ));
}
