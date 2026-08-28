/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    ffi::{CString, c_char},
    io::Cursor,
    mem,
    ops::ControlFlow,
    ptr,
};

use c_trie::{SuffixMode, SuffixTrie, TrieTerm};
use dict::{KeysDictType, OwnedDict};
use ffi::IndexFlags_Index_DocIdsOnly;
use fork_gc::{
    HandleError,
    terms::{TermEntry, TermNotFound, apply_terms, collect_terms, receive_terms},
};
use index_result::RSIndexResult;
use index_spec::{IndexSpecReadGuard, IndexSpecWriteGuard};
use inverted_index::opaque::InvertedIndex as OpaqueInvertedIndex;
use inverted_index::{GcScanDelta, InvertedIndex, doc_ids_only::DocIdsOnly};
use serde::Serialize as _;

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
// Provide Redis allocator shims so the C dict/trie functions can allocate memory.
redis_mock::mock_or_stub_missing_redis_c_symbols!();

fn trie_term(term: &[u8]) -> TrieTerm {
    assert!(!term.is_empty());
    // SAFETY: all callers pass complete, non-empty ASCII terms, which satisfy the
    // trie's validity invariants.
    unsafe { TrieTerm::from_bytes_unchecked(Box::from(term)) }
}

struct TestSpec {
    spec: ffi::IndexSpec,
    _keys_dict: OwnedDict<KeysDictType>,
}

impl TestSpec {
    // Each term gets a matching `DocIdsOnly` inverted index with docs
    // `1..=doc_count`, registered under the term in the keys dict.
    fn create(terms: impl IntoIterator<Item = (&'static [u8], u64)>) -> Self {
        let keys_dict = OwnedDict::<KeysDictType>::create();

        // SAFETY: a zeroed IndexSpec is valid for the fields this fixture sets below;
        // every other field is left zero/null and unused by the functions under test.
        let mut spec: ffi::IndexSpec = unsafe { mem::zeroed() };
        spec.flags = IndexFlags_Index_DocIdsOnly;
        // SAFETY: `NewHiddenString` copies the static string because `takeOwnership` is true.
        spec.specName =
            unsafe { ffi::NewHiddenString(c"idx".as_ptr(), c"idx".count_bytes(), true) };
        spec.obfuscatedName = CString::new("Index@test").unwrap().into_raw();
        // SAFETY: `NewTrie` returns a fresh, valid, empty terms trie; a terms trie stores no
        // payload, so a null free callback is correct.
        spec.terms = unsafe { ffi::NewTrie(None, ffi::TrieSortMode_Trie_Sort_Lex) };
        spec.keysDict = keys_dict.as_mut_ptr();

        let mut test_spec = Self {
            spec,
            _keys_dict: keys_dict,
        };
        for (term, doc_count) in terms {
            test_spec.insert_trie_term(term, doc_count);

            let mut ii = InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
            for doc_id in 1..=doc_count {
                ii.add_record(&RSIndexResult::build_virt().doc_id(doc_id).build())
                    .unwrap();
            }
            test_spec
                ._keys_dict
                .try_insert(term, Box::new(OpaqueInvertedIndex::DocIdsOnly(ii)))
                .unwrap_or_else(|_| panic!("term already present in keysDict"));
        }

        test_spec
    }

    fn insert_trie_term(&mut self, term: &[u8], doc_count: u64) {
        // SAFETY: `self.spec.terms` is live; `term` points to `term.len()` valid bytes;
        // a null payload is accepted by the terms trie.
        unsafe {
            ffi::Trie_InsertStringBuffer(
                self.spec.terms,
                term.as_ptr().cast::<c_char>(),
                term.len(),
                1.0,
                0,
                ptr::null_mut(),
                doc_count as usize,
            );
        }
    }

    fn read_guard(&self) -> mem::ManuallyDrop<IndexSpecReadGuard<'_>> {
        // SAFETY: the fixture owns `spec` for the duration of the guard and does not
        // mutate it while the guard is held.
        unsafe { IndexSpecReadGuard::from_locked(&self.spec) }
    }

    fn collect(&self) -> Vec<u8> {
        let guard = self.read_guard();
        let mut buf = Vec::new();
        collect_terms(&mut buf, &guard).unwrap();
        buf
    }

    fn add_suffix_trie(&mut self, terms: impl IntoIterator<Item = &'static [u8]>) {
        // SAFETY: `NewTrie` returns a fresh, valid trie; `suffixTrie_freeCallback` is
        // the matching free callback for the payloads `addSuffixTrie` inserts.
        self.spec.suffix = unsafe {
            ffi::NewTrie(
                Some(ffi::suffixTrie_freeCallback),
                ffi::TrieSortMode_Trie_Sort_Lex,
            )
        };
        for term in terms {
            assert!(!term.is_empty(), "addSuffixTrie rejects empty strings");
            // SAFETY: `self.spec.suffix` is live, and `term` points to `term.len()`
            // valid, non-empty bytes.
            unsafe {
                ffi::addSuffixTrie(
                    self.spec.suffix,
                    term.as_ptr().cast::<c_char>(),
                    term.len() as u32,
                )
            };
        }
    }

    fn suffix_matches(&self, pattern: &str) -> Vec<String> {
        // SAFETY: `self.spec.suffix` is a live suffix trie owned by this fixture and
        // is not mutated for the duration of this borrow.
        let trie = unsafe { SuffixTrie::from_raw(self.spec.suffix) };

        let mut runes = vec![0 as ffi::rune; pattern.len() + 1];
        // SAFETY: `pattern` is valid UTF-8 of `pattern.len()` bytes, so the decode
        // stays within the slice, and `runes` has sufficient capacity.
        let rlen = unsafe {
            ffi::strToRunes(
                pattern.as_ptr().cast::<c_char>(),
                pattern.len(),
                runes.as_mut_ptr(),
                runes.len(),
            )
        };
        runes.truncate(rlen);

        let mut found = Vec::new();
        trie.iterate_contains(&runes, SuffixMode::Suffix, |term| {
            found.push(String::from_utf8_lossy(term).into_owned());
            ControlFlow::Continue(())
        });
        found.sort();
        found
    }

    fn write(&mut self) -> mem::ManuallyDrop<IndexSpecWriteGuard<'_>> {
        // SAFETY: each test owns the fixture exclusively and accesses it only through
        // the returned guard until that guard goes out of scope.
        unsafe { IndexSpecWriteGuard::from_locked_mut(&mut self.spec) }
    }
}

impl Drop for TestSpec {
    fn drop(&mut self) {
        // SAFETY: all pointers were allocated by `create` or `add_suffix_trie`, remain
        // owned by this fixture, and have not been freed yet.
        unsafe {
            ffi::TrieType_Free(self.spec.terms.cast());
            if !self.spec.suffix.is_null() {
                ffi::TrieType_Free(self.spec.suffix.cast());
            }
            ffi::HiddenString_Free(self.spec.specName, true);
            drop(CString::from_raw(self.spec.obfuscatedName));
        }
    }
}

fn assert_only_terminator(buf: &[u8]) {
    let mut cursor = Cursor::new(buf);
    assert!(
        rmp_serde::from_read::<_, Option<TermEntry>>(&mut cursor)
            .unwrap()
            .is_none()
    );
    assert_eq!(cursor.position(), buf.len() as u64);
}

#[test]
#[cfg_attr(miri, ignore = "accesses extern static `invIdxDictType`")]
fn empty_trie_writes_only_terminator() {
    let spec = TestSpec::create([]);
    assert_only_terminator(&spec.collect());
}

#[test]
#[cfg_attr(miri, ignore = "accesses extern static `invIdxDictType`")]
fn term_with_empty_inverted_index_is_skipped() {
    let spec = TestSpec::create([(&b"empty"[..], 0)]);
    assert_only_terminator(&spec.collect());
}

/// A term present in the trie but with no registered inverted index (never opened for
/// write) is skipped: `Redis_OpenInvertedIndex` returns null for it.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `invIdxDictType`")]
fn term_without_inverted_index_is_skipped() {
    let mut spec = TestSpec::create([]);
    spec.insert_trie_term(b"orphan", 1);
    assert_only_terminator(&spec.collect());
}

/// Multiple terms each produce their own serialised entry, followed by `None`.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `invIdxDictType`")]
fn multiple_terms_write_multiple_delta_frames() {
    let spec = TestSpec::create([(&b"apple"[..], 1), (&b"grape"[..], 1)]);
    let buf = spec.collect();

    let mut cursor = Cursor::new(&buf);

    let first = rmp_serde::from_read::<_, Option<TermEntry>>(&mut cursor)
        .unwrap()
        .unwrap();
    let second = rmp_serde::from_read::<_, Option<TermEntry>>(&mut cursor)
        .unwrap()
        .unwrap();

    let mut terms = [first.term.into_vec(), second.term.into_vec()];
    terms.sort(); // Entries can come in any order because trie iteration order isn't asserted here.

    assert_eq!(terms, [b"apple".to_vec(), b"grape".to_vec()]);
    assert!(
        rmp_serde::from_read::<_, Option<TermEntry>>(&mut cursor)
            .unwrap()
            .is_none()
    );
    assert_eq!(cursor.position(), buf.len() as u64);
}

#[test]
fn receive_none_returns_none() {
    let mut buf = Vec::new();
    Option::<TermEntry<&[u8]>>::None
        .serialize(&mut rmp_serde::Serializer::new(&mut buf))
        .unwrap();
    let mut cursor = Cursor::new(&buf);
    assert!(receive_terms(&mut cursor).unwrap().is_none());
    assert_eq!(cursor.position(), buf.len() as u64);
}

#[test]
fn receive_malformed_entry_returns_codec_error() {
    let mut cursor = Cursor::new(b"garbage");
    assert!(matches!(
        receive_terms(&mut cursor),
        Err(HandleError::Codec {
            msg: "decoding terms entry",
            ..
        })
    ));
}

#[test]
fn receive_entry_returns_term_and_delta() {
    let mut buf = Vec::new();
    Some(TermEntry {
        term: b"apple".as_slice(),
        delta: GcScanDelta::empty_for_testing(),
    })
    .serialize(&mut rmp_serde::Serializer::new(&mut buf))
    .unwrap();

    let mut cursor = Cursor::new(&buf);
    let entry = receive_terms(&mut cursor).unwrap().unwrap();
    assert_eq!(&*entry.term, b"apple");
    assert_eq!(entry.delta, GcScanDelta::empty_for_testing());
    assert_eq!(cursor.position(), buf.len() as u64);
}

#[test]
#[cfg_attr(miri, ignore = "accesses extern static `invIdxDictType`")]
fn apply_returns_err_when_term_not_found() {
    let mut spec = TestSpec::create([]);
    let delta = GcScanDelta::empty_for_testing();

    {
        let mut write_guard = spec.write();
        assert!(matches!(
            apply_terms(&trie_term(b"nonexistent"), delta, &mut write_guard),
            Err(HandleError::Custom(TermNotFound))
        ));
    }
}

/// A successful apply with a no-op delta leaves the term's entry in place.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `invIdxDictType`")]
fn apply_succeeds_and_keeps_entry_when_docs_remain() {
    let mut spec = TestSpec::create([(&b"apple"[..], 2)]);
    let delta = GcScanDelta::empty_for_testing();

    {
        let mut write_guard = spec.write();
        let stats = apply_terms(&trie_term(b"apple"), delta, &mut write_guard).unwrap();

        assert_eq!(stats.records_removed, 0);
        assert_eq!(stats.terms_removed, 0);
        assert_eq!(stats.terms_size_removed, 0);
        assert!(write_guard.keys_dict_mut().fetch_mut(b"apple").is_some());
    }
}

/// Full child-to-parent roundtrip: when all docs are deleted, the term's entry is
/// removed from the keys dict and trie, and its statistics are returned to the caller.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `invIdxDictType`")]
fn roundtrip_all_docs_deleted_removes_term() {
    let mut spec = TestSpec::create([(&b"apple"[..], 2)]);

    // Child side: collect.
    let buf = spec.collect();

    // Parent side: receive.
    let mut cursor = Cursor::new(&buf);
    let entry = receive_terms(&mut cursor).unwrap().unwrap();
    assert_eq!(&*entry.term, b"apple");

    // Parent side: apply.
    {
        let mut write_guard = spec.write();
        let stats = apply_terms(&entry.term, entry.delta, &mut write_guard).unwrap();

        assert!(write_guard.keys_dict_mut().fetch_mut(b"apple").is_none());
        assert!(stats.bytes_collected > 0);
        assert_eq!(
            write_guard.terms_mut().num_docs(b"apple"),
            0,
            "the term is gone from the terms trie too"
        );
        assert_eq!(stats.terms_removed, 1);
        assert_eq!(stats.terms_size_removed, entry.term.len());
    }
}

/// A collected term is removed from the suffix trie along with the terms trie, but
/// only the terms that were actually collected.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `invIdxDictType`")]
fn apply_removes_the_collected_term_from_the_suffix_trie() {
    let mut spec = TestSpec::create([(&b"apple"[..], 0), (&b"maple"[..], 1)]);
    spec.add_suffix_trie([&b"apple"[..], &b"maple"[..]]);

    assert_eq!(spec.suffix_matches("ple"), ["apple", "maple"]);

    {
        let mut write_guard = spec.write();
        apply_terms(
            &trie_term(b"apple"),
            GcScanDelta::empty_for_testing(),
            &mut write_guard,
        )
        .unwrap();
    }

    assert_eq!(
        spec.suffix_matches("ple"),
        ["maple"],
        "only the removed term is dropped from the suffix trie"
    );
}

/// A spec with no `WITHSUFFIXTRIE` field has a null suffix trie; removal must skip it
/// rather than dereference it.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `invIdxDictType`")]
fn apply_removes_an_empty_term_without_a_suffix_trie() {
    let mut spec = TestSpec::create([(&b"apple"[..], 0)]);
    assert!(
        spec.spec.suffix.is_null(),
        "no field opted into a suffix trie"
    );

    {
        let mut write_guard = spec.write();
        apply_terms(
            &trie_term(b"apple"),
            GcScanDelta::empty_for_testing(),
            &mut write_guard,
        )
        .unwrap();
        assert!(write_guard.keys_dict_mut().fetch_mut(b"apple").is_none());
    }
}

/// The trie and the keys dict can disagree if a term was already dropped from the
/// trie. Removal logs a warning and still completes the rest of the bookkeeping.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `invIdxDictType`")]
fn apply_survives_an_empty_term_missing_from_the_trie() {
    let mut spec = TestSpec::create([(&b"apple"[..], 0)]);

    {
        let mut write_guard = spec.write();
        let removed = write_guard.terms_mut().delete(&trie_term(b"apple"));
        assert!(
            removed,
            "drop the term from the trie only, leaving it in the keys dict"
        );

        let stats = apply_terms(
            &trie_term(b"apple"),
            GcScanDelta::empty_for_testing(),
            &mut write_guard,
        )
        .unwrap();
        assert_eq!(stats.terms_removed, 1);
        assert_eq!(stats.terms_size_removed, b"apple".len());
        assert!(write_guard.keys_dict_mut().fetch_mut(b"apple").is_none());
    }
}
