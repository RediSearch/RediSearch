/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rdb_io::RdbIO;
use std::{io::Cursor, ptr, sync::Arc};
use stopwords::MAX_STOPWORDLIST_SIZE as LIMIT;
use stopwords_ffi::*;

extern crate redisearch_rs;
redis_mock::mock_or_stub_missing_redis_c_symbols!();

fn list<'a>(words: impl IntoIterator<Item = &'a [u8]>) -> StopWordList {
    StopWordList(stopwords::StopWordList::new(words), Ownership::Counted)
}

#[test]
#[cfg(not(miri))]
fn cursor_consumes_at_most_the_input_limit() {
    for (name, count, consumed, last) in [
        ("single", 1, 1, 1),
        ("limit", LIMIT, LIMIT, 1),
        ("over limit", LIMIT + 1, LIMIT, 0),
    ] {
        let mut words = vec![c"FOO".as_ptr().cast_mut().cast(); count + 1];
        words[0] = c"ignored".as_ptr().cast_mut().cast();
        words[count] = c"LAST".as_ptr().cast_mut().cast();
        let mut cursor = ffi::ArgsCursor {
            objs: words.as_mut_ptr(),
            type_: ffi::ACType_AC_TYPE_CHAR as i32,
            argc: words.len(),
            offset: 1,
        };
        // SAFETY: 1. The exclusive cursor borrows live C strings through words.
        let list = unsafe { NewStopWordListAC(&raw mut cursor) };
        assert_eq!(cursor.offset, 1 + consumed, "{name}");
        // SAFETY: 1, 2. list owns a live reference and the search buffer is readable.
        assert_eq!(
            unsafe { StopWordList_Contains(list, c"last".as_ptr(), 4) },
            last,
            "{name}"
        );
        // SAFETY: 1. Consumes the constructor's sole reference.
        unsafe { StopWordList_Unref(list) };
    }
}

#[test]
#[cfg(not(miri))]
fn cursor_keeps_explicit_lengths() {
    for (name, word) in [
        ("leading nul", b"\0FOO".as_slice()),
        ("unicode", "İ".as_bytes()),
        ("invalid UTF-8", b"\xc3(z"),
    ] {
        // SAFETY: The source bytes are readable for the supplied length.
        let string = unsafe { ffi::sdsnewlen(word.as_ptr().cast(), word.len()) };
        let mut object = string.cast();
        let mut cursor = ffi::ArgsCursor {
            objs: &raw mut object,
            type_: ffi::ACType_AC_TYPE_SDS as i32,
            argc: 1,
            offset: 0,
        };
        // SAFETY: 1. The exclusive cursor borrows the live SDS.
        let list = unsafe { NewStopWordListAC(&raw mut cursor) };
        // SAFETY: The list owns its normalized copy; release the input.
        unsafe { ffi::sdsfree(string) };
        // SAFETY: 1, 2. list is live and word is readable through its explicit length.
        assert_eq!(
            unsafe { StopWordList_Contains(list, word.as_ptr().cast(), word.len()) },
            1,
            "{name}"
        );
        // SAFETY: 1. Consumes the constructor's sole reference.
        unsafe { StopWordList_Unref(list) };
    }
}

#[test]
fn shared_lists_survive_references() {
    let _lock = GLOBALS.lock().unwrap();
    let mut cursor = ffi::ArgsCursor {
        objs: ptr::null_mut(),
        type_: 0,
        argc: 0,
        offset: 0,
    };
    // SAFETY: 1. An empty cursor needs no argument buffers.
    let empty = unsafe { NewStopWordListAC(&raw mut cursor) };
    for (name, list, expected) in [("empty", empty, 0), ("default", DefaultStopWordList(), 1)] {
        // SAFETY: 1. The global owns this live list.
        unsafe { StopWordList_Ref(list) };
        // SAFETY: 1. Shared references are borrowed; neither unref consumes the global.
        unsafe { StopWordList_Unref(list) };
        // SAFETY: 1. The same shared pointer remains borrowed from its global.
        unsafe { StopWordList_Unref(list) };
        // SAFETY: 1, 2. The global still owns list; the search buffer is readable.
        assert_eq!(
            unsafe { StopWordList_Contains(list, c"THE".as_ptr(), 3) },
            expected,
            "{name}"
        );
    }
    // SAFETY: 1. The test mutex excludes other users; no pointer survives cleanup.
    unsafe { StopWordList_FreeGlobals() };
}

static GLOBALS: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn free_globals_allows_reinit() {
    let _lock = GLOBALS.lock().unwrap();
    DefaultStopWordList();
    // SAFETY: 1. No shared pointer is borrowed by this test or another global test.
    unsafe { StopWordList_FreeGlobals() };
    let recreated = DefaultStopWordList();
    // SAFETY: 1, 2. The newly initialized global owns list; the term is readable.
    assert_eq!(
        unsafe { StopWordList_Contains(recreated, c"the".as_ptr(), 3) },
        1
    );
    // SAFETY: 1. The test mutex excludes other users; no pointer survives cleanup.
    unsafe { StopWordList_FreeGlobals() };
}

#[test]
fn custom_list_is_freed_on_last_unref() {
    let owner = Arc::new(list([b"CUSTOM".as_slice()]));
    let lifetime = Arc::downgrade(&owner);
    let custom = Arc::into_raw(owner).cast_mut();
    // SAFETY: 1. custom owns one live Arc reference, retained for another user.
    unsafe { StopWordList_Ref(custom) };
    // SAFETY: 1. Release one of the two live references.
    unsafe { StopWordList_Unref(custom) };
    assert!(lifetime.upgrade().is_some());
    // SAFETY: 1. Release the final owned reference; custom is not used again.
    unsafe { StopWordList_Unref(custom) };
    assert!(lifetime.upgrade().is_none());
}

#[test]
fn null_list_or_term_never_matches() {
    let mut list = list([b"word".as_slice()]);
    // SAFETY: 1, 2. A null term is explicitly accepted; list is live.
    assert_eq!(
        unsafe { StopWordList_Contains(&raw mut list, ptr::null(), 4) },
        0
    );
    // SAFETY: 1, 2. A null list is explicitly accepted; the search buffer is readable.
    assert_eq!(
        unsafe { StopWordList_Contains(ptr::null(), c"word".as_ptr(), 4) },
        0
    );
}

#[test]
fn info_lists_sorted_words() {
    for (name, words, expected) in [
        ("empty", vec![], b"\0".as_slice()),
        ("single", vec![b"FOO".as_slice()], b"\"foo\"\0"),
        (
            "unsorted duplicates",
            vec![b"zebra".as_slice(), b"APPLE", b"mango", b"apple"],
            b"\"apple\",\"mango\",\"zebra\"\0",
        ),
    ] {
        assert_eq!(info_string(&list(words)), expected, "{name}");
    }
}

#[test]
#[cfg(not(miri))]
fn fingerprints_depend_on_content() {
    for (name, first, second) in [
        (
            "word contents",
            vec![b"apple".as_slice(), b"zebra"],
            vec![b"apple".as_slice(), b"zebras"],
        ),
        (
            "word boundaries",
            vec![b"a".as_slice(), b"bc"],
            vec![b"ab".as_slice(), b"c"],
        ),
    ] {
        // SAFETY: 1. Each temporary list lives through its synchronous hash call.
        let first = unsafe { StopWordList_Fingerprint(&list(first)) };
        // SAFETY: 1. Each temporary list lives through its synchronous hash call.
        let second = unsafe { StopWordList_Fingerprint(&list(second)) };
        assert_ne!(first, second, "{name}");
    }
}

#[test]
fn rdb_preserves_stored_words() {
    for (name, words) in [
        ("empty", vec![]),
        (
            "stored bytes",
            vec![b"zebra".as_slice(), b"APPLE", b"a\0b", b"\xff"],
        ),
    ] {
        let mut bytes = stored_bytes(&words);
        let list = load(&mut &mut Cursor::new(&mut bytes)).expect("valid stored list");
        let mut sorted = words;
        sorted.sort();
        assert_eq!(list.iter().collect::<Vec<_>>(), sorted, "{name}");
        let mut saved = Vec::new();
        save(&list, &mut &mut Cursor::new(&mut saved));
        assert_eq!(saved, stored_bytes(&sorted), "{name}");
    }
}

fn stored_bytes(words: &[&[u8]]) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut cursor = Cursor::new(&mut bytes);
    (&mut cursor).write_u64(words.len() as u64);
    for word in words {
        (&mut cursor).write_buffer(word);
    }
    bytes
}

#[test]
fn rdb_rejects_truncated_and_oversized_lists() {
    let valid = stored_bytes(&[b"first", b"second"]);
    for end in 0..valid.len() {
        let mut truncated = valid[..end].to_vec();
        assert!(matches!(
            load(&mut &mut Cursor::new(&mut truncated)),
            Err(LoadError::Io(_))
        ));
    }
    for (name, count, accepted) in [("limit", LIMIT, true), ("over limit", LIMIT + 1, false)] {
        let mut bytes = stored_bytes(&vec![b"word".as_slice(); count]);
        assert_eq!(
            load(&mut &mut Cursor::new(&mut bytes)).is_ok(),
            accepted,
            "{name}"
        );
    }
}

#[test]
#[cfg(not(miri))]
fn rdb_errors_return_null_and_warn_only_for_oversized_counts() {
    use redis_module::raw;
    use std::ffi::{CStr, c_char, c_int};
    #[repr(C)]
    struct TestRdb {
        count: u64,
        error: c_int,
        level: [c_char; 16],
        warning: [c_char; 128],
    }
    unsafe extern "C" {
        fn StopwordsTest_LoadUnsigned(io: *mut raw::RedisModuleIO) -> u64;
        fn StopwordsTest_LoadStringBuffer(
            io: *mut raw::RedisModuleIO,
            len: *mut usize,
        ) -> *mut c_char;
        fn StopwordsTest_IsIOError(io: *mut raw::RedisModuleIO) -> c_int;
        fn StopwordsTest_LogIOError(
            io: *mut raw::RedisModuleIO,
            level: *const c_char,
            fmt: *const c_char,
            ...
        );
    }
    // SAFETY: Only this test uses Redis RDB IO, with the layout-matched TestRdb.
    unsafe { raw::RedisModule_LoadUnsigned = Some(StopwordsTest_LoadUnsigned) };
    // SAFETY: Only this test uses Redis RDB IO, with the layout-matched TestRdb.
    unsafe { raw::RedisModule_LoadStringBuffer = Some(StopwordsTest_LoadStringBuffer) };
    // SAFETY: Only this test uses Redis RDB IO, with the layout-matched TestRdb.
    unsafe { raw::RedisModule_IsIOError = Some(StopwordsTest_IsIOError) };
    // SAFETY: Only this test uses Redis RDB IO, with a genuine C variadic callback.
    unsafe { raw::RedisModule_LogIOError = Some(StopwordsTest_LogIOError) };
    for (name, count, level, warning) in [
        (
            "oversized",
            LIMIT as u64 + 1,
            c"warning",
            format!(
                "RDB Load: Stopword list size ({}) exceeds maximum allowed ({LIMIT})",
                LIMIT + 1
            ),
        ),
        ("truncated", 1, c"", String::new()),
    ] {
        let mut io = TestRdb {
            count,
            error: 0,
            level: [0; 16],
            warning: [0; 128],
        };
        // SAFETY: 1. The callbacks exclusively access this live, layout-matched state.
        assert!(
            unsafe { StopWordList_RdbLoad((&raw mut io).cast(), 0) }.is_null(),
            "{name}"
        );
        // SAFETY: The callbacks leave NUL-terminated strings in these fixed buffers.
        assert_eq!(
            unsafe { CStr::from_ptr(io.level.as_ptr()) },
            level,
            "{name}"
        );
        // SAFETY: The callbacks leave NUL-terminated strings in these fixed buffers.
        assert_eq!(
            unsafe { CStr::from_ptr(io.warning.as_ptr()) }.to_bytes(),
            warning.as_bytes(),
            "{name}"
        );
    }
}

#[cfg(not(miri))]
mod properties {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeSet;

    fn stored_words() -> impl Strategy<Value = Vec<Vec<u8>>> {
        prop::collection::vec(
            prop::collection::vec(any::<u8>(), 0..300),
            0..=stopwords::MAX_STOPWORDLIST_SIZE,
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn rdb_round_trips_stored_keys(words in stored_words()) {
            let expected: BTreeSet<_> = words.iter().cloned().collect();
            let list = stopwords::StopWordList::from_stored(words);
            let mut bytes = Vec::new();
            let mut cursor = Cursor::new(&mut bytes);
            save(&list, &mut &mut cursor);
            cursor.set_position(0);
            let restored = load(&mut &mut cursor).unwrap();
            prop_assert_eq!(restored.iter().collect::<Vec<_>>(), expected.into_iter().collect::<Vec<_>>());
        }

        #[test]
        fn fingerprint_is_order_independent(words in stored_words()) {
            let forward = list(words.iter().map(Vec::as_slice));
            let reverse = list(words.iter().rev().map(Vec::as_slice));
            // SAFETY: 1. Both lists remain alive and immutable during hashing.
            let forward = unsafe { StopWordList_Fingerprint(&forward) };
            // SAFETY: 1. Both lists remain alive and immutable during hashing.
            let reverse = unsafe { StopWordList_Fingerprint(&reverse) };
            prop_assert_eq!(forward, reverse);
        }
    }
}
